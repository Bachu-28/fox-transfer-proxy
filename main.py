import os
import hmac
import hashlib
import base64
import asyncio
import zipfile
import tempfile
import json
import time
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import httpx
import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("renderray-worker")

# ── CONFIG ─────────────────────────────────────────────────────
FOX_ACCESS_ID          = os.environ["FOX_ACCESS_ID"]
FOX_ACCESS_KEY         = os.environ["FOX_ACCESS_KEY"]
FOX_USER_ID            = os.environ["FOX_USER_ID"]
FOX_DOMAIN             = os.environ.get("FOX_DOMAIN", "jop.foxrenderfarm.com")
FOX_PLATFORM           = os.environ.get("FOX_PLATFORM", "62")
FOX_TRANSFER_IP        = os.environ.get("FOX_TRANSFER_SERVER_IP", "45.251.92.16")
FOX_TRANSFER_PORT      = os.environ.get("FOX_TRANSFER_SERVER_PORT", "12121")

SUPABASE_URL           = os.environ["SUPABASE_URL"]
SUPABASE_KEY           = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BUCKET_INPUT           = os.environ.get("SUPABASE_STORAGE_BUCKET_INPUT", "foxrender")
BUCKET_OUTPUT          = os.environ.get("SUPABASE_STORAGE_BUCKET_OUTPUT", "render-outputs")
WORKER_SECRET          = os.environ.get("WORKER_SECRET", "changeme")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="RenderRay Worker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── MODELS ─────────────────────────────────────────────────────
class TransferJobRequest(BaseModel):
    job_id: str
    fox_task_id: str
    storage_path: str
    scene_filename: str
    software: str
    frame_start: int
    frame_end: int

# ── SDK CONFIG BUILDERS ────────────────────────────────────────
def build_task_json(req: TransferJobRequest, scene_server_path: str) -> dict:
    """Follows FoxRenderfarm SDK Specification for task.json"""
    frames_str = f"{req.frame_start}-{req.frame_end}[1]"
    
    # CG ID Map from official docs
    cg_map = {
        "blender": {"id": "2007", "name": "Blender", "ver": "4.1"},
        "maya":    {"id": "2000", "name": "Maya",    "ver": "2024"},
        "houdini": {"id": "2004", "name": "Houdini", "ver": "19.5"},
        "3dsmax":  {"id": "2001", "name": "3ds Max", "ver": "2024"},
    }
    
    sw = cg_map.get(req.software.lower(), cg_map["blender"])

    config = {
        "software_config": {
            "cg_name": sw["name"],
            "cg_version": sw["ver"],
            "plugins": {}
        },
        "task_info": {
            "user_id": FOX_USER_ID,
            "task_id": req.fox_task_id,
            "cg_id": sw["id"],
            "platform": FOX_PLATFORM,
            "channel": "4",
            "os_name": "1",  # 1 for Windows, 0 for Linux
            "ram": "64",
            "input_cg_file": scene_server_path,
            "project_name": f"RR_{req.job_id[:8]}",
            "frames_per_task": "1",
            "pre_frames": "000", # Priority frames
            "stop_after_test": "1", # 1: stop after test, 2: continue
            "time_out": "43200"
        },
        "scene_info_render": {}
    }

    # Software-Specific Scene Info (Required by SDK)
    if req.software == "blender":
        config["scene_info_render"] = {"common": {"frames": frames_str, "Render_Format": "OPEN_EXR"}}
    elif req.software == "maya":
        config["scene_info_render"] = {"defaultRenderLayer": {"common": {"frames": frames_str, "renderer": "arnold"}}}
    elif req.software == "houdini":
        config["scene_info_render"] = {"rop_node": [{"node": "/out/mantra1", "frames": frames_str, "render": "1"}]}

    return config

# ── CORE PIPELINE ──────────────────────────────────────────────
async def run_transfer_job(req: TransferJobRequest):
    logger.info(f"▶ Starting pipeline for {req.job_id}")
    
    # Helper to update Supabase
    def update_status(s, err=None):
        supabase.table("cloud_render_jobs").update({"status": s, "error_message": err}).eq("id", req.job_id).execute()

    update_status("uploading")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        extract_dir = tmp / "files"
        extract_dir.mkdir()
        
        # 1. Download via Supabase SDK
        try:
            url_res = supabase.storage.from_(BUCKET_INPUT).get_public_url(req.storage_path)
            async with httpx.AsyncClient(timeout=600) as client:
                resp = await client.get(url_res)
                resp.raise_for_status()
                zip_p = tmp / "scene.zip"
                zip_p.write_bytes(resp.content)
            
            with zipfile.ZipFile(zip_p, 'r') as z:
                z.extractall(extract_dir)
        except Exception as e:
            return update_status("error", f"Download/Extract Failed: {str(e)}")

        # 2. Locate Scene File
        scene_file = next(extract_dir.rglob(f"*{req.scene_filename}"), None)
        if not scene_file:
            return update_status("error", "Scene file not found in ZIP")

        scene_rel = scene_file.relative_to(extract_dir)
        scene_server_path = f"/renderray/{req.fox_task_id}/{scene_rel}"

        # 3. Build SDK Configs
        task_json = build_task_json(req, scene_server_path)
        
        # Build upload.json list
        assets = []
        for f in extract_dir.rglob("*"):
            if f.is_file():
                assets.append({
                    "local": str(f),
                    "server": f"/renderray/{req.fox_task_id}/{f.relative_to(extract_dir)}"
                })
        upload_json = {"asset": assets, "task_id": req.fox_task_id}

        # Save configs to disk for the Rayvision SDK to pick up
        cfg_dir = tmp / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "task.json").write_text(json.dumps(task_json))
        (cfg_dir / "upload.json").write_text(json.dumps(upload_json))
        (cfg_dir / "tips.json").write_text(json.dumps({"taskId": req.fox_task_id}))
        (cfg_dir / "asset.json").write_text(json.dumps({"taskId": req.fox_task_id}))

        # 4. Upload to Fox via RayvisionSync
        try:
            from rayvision_api import RayvisionAPI
            from rayvision_sync.upload import RayvisionUpload
            
            api = RayvisionAPI(access_id=FOX_ACCESS_ID, access_key=FOX_ACCESS_KEY, domain=FOX_DOMAIN, platform=FOX_PLATFORM)
            uploader = RayvisionUpload(api)
            
            # Upload assets based on upload.json
            uploader.upload_asset(str(cfg_dir / "upload.json"))
            # Upload JSON configs
            uploader.upload_config(req.fox_task_id, [str(cfg_dir/f) for f in ["task.json", "tips.json", "asset.json"]])
        except Exception as e:
            return update_status("error", f"Fox Upload Failed: {str(e)}")

        # 5. Submit
        submit_url = f"https://{FOX_DOMAIN}/api/render/task/submitTask"
        # (Headers generation logic here - keeping your original function for brevity)
        # ... logic to call fox_api POST submitTask ...
        update_status("queued")

# [Standard FastAPI Endpoints / Startup Logic...]
