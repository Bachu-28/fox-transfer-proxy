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
BUCKET_INPUT           = os.environ.get("SUPABASE_STORAGE_BUCKET_INPUT", "render-inputs")
BUCKET_OUTPUT          = os.environ.get("SUPABASE_STORAGE_BUCKET_OUTPUT", "render-outputs")
WORKER_SECRET          = os.environ.get("WORKER_SECRET", "changeme")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="RenderRay Worker", version="1.0.0")
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
    signed_url: Optional[str] = None  # If provided, skip generating signed URL

class DownloadJobRequest(BaseModel):
    job_id: str
    fox_task_id: str
    user_id: str

# ── FOX AUTH ───────────────────────────────────────────────────
def generate_fox_headers() -> dict:
    timestamp = str(int(time.time()))
    nonce = base64.urlsafe_b64encode(os.urandom(6)).decode()[:8]
    message = FOX_ACCESS_KEY + timestamp + nonce
    sig = hmac.new(
        FOX_ACCESS_KEY.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).digest()
    signature = base64.b64encode(sig).decode()
    return {
        "accessId":     FOX_ACCESS_ID,
        "channel":      "4",
        "platform":     FOX_PLATFORM,
        "UTCTimestamp": timestamp,
        "nonce":        nonce,
        "signature":    signature,
        "version":      "1.0.0",
        "Content-Type": "application/json",
    }

async def fox_api(method: str, endpoint: str, payload: dict = None) -> dict:
    url = f"https://{FOX_DOMAIN}{endpoint}"
    headers = generate_fox_headers()
    async with httpx.AsyncClient(timeout=30) as client:
        if method == "GET":
            resp = await client.get(url, headers=headers, params=payload)
        else:
            resp = await client.post(url, headers=headers, json=payload or {})
    data = resp.json()
    logger.info(f"Fox API {method} {endpoint} → {data.get('code')} {data.get('message','')}")
    return data

# ── SUPABASE HELPERS ───────────────────────────────────────────
def update_job(job_id: str, updates: dict):
    supabase.table("cloud_render_jobs").update(updates).eq("id", job_id).execute()

def update_job_status(job_id: str, status: str, error: str = None):
    payload = {"status": status}
    if error:
        payload["error_message"] = error
    update_job(job_id, payload)
    logger.info(f"Job {job_id} → {status}" + (f" ({error})" if error else ""))

def get_signed_url(bucket: str, path: str, expires: int = 3600) -> str:
    result = supabase.storage.from_(bucket).create_signed_url(path, expires)
    return result["signedURL"]

# ── BUILD CONFIG JSON FILES ────────────────────────────────────
def build_upload_json(extracted_dir: str, fox_task_id: str) -> dict:
    asset_list = []
    base_dir = Path(extracted_dir)
    for file_path in base_dir.rglob("*"):
        if file_path.is_file():
            relative = file_path.relative_to(base_dir)
            server_path = f"/renderray/{fox_task_id}/{relative}"
            asset_list.append({
                "local": str(file_path),
                "server": server_path,
            })
    return {"asset": asset_list, "task_id": fox_task_id}

def build_task_json(req: TransferJobRequest, fox_task_id: str, scene_server_path: str) -> dict:
    frames_str = f"{req.frame_start}-{req.frame_end}[1]"
    cg_id_map = {
        "blender": "2007",
        "maya": "2000",
        "houdini": "2004",
        "3dsmax": "2001",
        "clarisse": "2013",
    }
    base = {
        "task_info": {
            "cg_id":                  cg_id_map.get(req.software, "2007"),
            "input_cg_file":          scene_server_path,
            "user_id":                FOX_USER_ID,
            "task_id":                fox_task_id,
            "platform":               FOX_PLATFORM,
            "channel":                "4",
            "frames_per_task":        "1",
            "pre_frames":             "100",
            "job_stop_time":          "86400",
            "task_stop_time":         "259200",
            "time_out":               "43200",
            "stop_after_test":        "1",
            "tiles":                  "1",
            "tiles_type":             "block",
            "os_name":                "1",
            "ram":                    "64",
            "is_picture":             "0",
            "is_layer_rendering":     "1",
            "enable_layered":         "1",
            "render_layer_type":      "0",
            "is_distribute_render":   "0",
            "distribute_render_node": "3",
            "input_project_path":     "",
            "project_name":           f"renderray_{fox_task_id}",
        }
    }

    if req.software == "blender":
        base["software_config"] = {"cg_name": "Blender", "cg_version": "4.1", "plugins": {}}
        base["scene_info_render"] = {
            "common": {
                "width": "1920", "height": "1080",
                "scene_name": ["Scene"],
                "camera_name": "Camera",
                "Render_Format": "OPEN_EXR",
                "frames": frames_str,
                "Output_path": "/tmp/",
            }
        }
    elif req.software == "maya":
        base["software_config"] = {"cg_name": "Maya", "cg_version": "2024", "plugins": {"mtoa": "5.3.1"}}
        base["scene_info_render"] = {
            "defaultRenderLayer": {
                "renderable": "1", "is_default_camera": "1", "option": "",
                "common": {
                    "image_format": "exr",
                    "start": str(req.frame_start), "end": str(req.frame_end),
                    "by_frame": "1", "frames": frames_str,
                    "width": "1920", "height": "1080",
                    "render_camera": ["persp"], "all_camera": ["persp"],
                    "renderer": "arnold", "animation": "True",
                }
            }
        }
    elif req.software == "houdini":
        base["software_config"] = {"cg_name": "Houdini", "cg_version": "19.5", "plugins": {}}
        base["scene_info_render"] = {
            "rop_node": [{"node": "/out/mantra1", "frames": frames_str,
                          "option": "-1", "render": "1", "height": "1080", "width": "1920"}],
            "geo_node": []
        }
    elif req.software == "3dsmax":
        base["software_config"] = {"cg_name": "3ds Max", "cg_version": "2024", "plugins": {}}
        base["scene_info_render"] = {
            "common": {
                "frames": frames_str, "width": "1920", "height": "1080",
                "all_camera": ["Camera001"], "renderable_camera": ["Camera001"],
            }
        }
    elif req.software == "clarisse":
        base["software_config"] = {"cg_name": "Clarisse", "cg_version": "5.0", "plugins": {}}
        base["scene_info_render"] = {
            "common": {
                "frames": frames_str, "width": "1920", "height": "1080", "camera": "Camera",
            }
        }

    return base

# ── CORE TRANSFER FUNCTION ─────────────────────────────────────
async def run_transfer_job(req: TransferJobRequest):
    logger.info(f"▶ Starting transfer for job {req.job_id}, Fox task {req.fox_task_id}")
    update_job_status(req.job_id, "uploading")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "scene.zip"
        extract_dir = tmp / "extracted"
        config_dir = tmp / "config"
        extract_dir.mkdir()
        config_dir.mkdir()

        # STEP 1: Download ZIP from Supabase Storage
        logger.info(f"Downloading ZIP from Supabase: {req.storage_path}")
        try:
            signed = req.signed_url if req.signed_url else get_signed_url(BUCKET_INPUT, req.storage_path, expires=3600)
            async with httpx.AsyncClient(timeout=600) as client:
                async with client.stream("GET", signed) as resp:
                    resp.raise_for_status()
                    with open(zip_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
            logger.info(f"ZIP downloaded: {zip_path.stat().st_size / 1024 / 1024:.1f} MB")
        except Exception as e:
            update_job_status(req.job_id, "error", f"Failed to download ZIP: {str(e)}")
            return

        # STEP 2: Extract ZIP
        logger.info("Extracting ZIP...")
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
            file_count = sum(1 for _ in extract_dir.rglob("*") if _.is_file())
            logger.info(f"Extracted {file_count} files")
        except Exception as e:
            update_job_status(req.job_id, "error", f"Failed to extract ZIP: {str(e)}")
            return

        # Find scene file
        extensions = {
            "blender": [".blend"],
            "maya": [".ma", ".mb"],
            "houdini": [".hip", ".hipnc"],
            "3dsmax": [".max"],
            "clarisse": [".project"],
        }
        scene_file = None
        for ext in extensions.get(req.software, []):
            found = list(extract_dir.rglob(f"*{ext}"))
            if found:
                scene_file = found[0]
                break

        if not scene_file:
            update_job_status(req.job_id, "error", f"No scene file found for software: {req.software}")
            return

        scene_server_path = f"/renderray/{req.fox_task_id}/{scene_file.relative_to(extract_dir)}"
        logger.info(f"Scene file: {scene_file.name} → {scene_server_path}")

        # STEP 3: Build config JSON files
        task_json   = build_task_json(req, req.fox_task_id, scene_server_path)
        upload_json = build_upload_json(str(extract_dir), req.fox_task_id)
        tips_json   = {"taskId": req.fox_task_id, "inputSceneFiles": [scene_server_path], "tips": {}}
        asset_json  = {"taskId": req.fox_task_id, "asset_path": scene_server_path}

        task_json_path   = config_dir / "task.json"
        upload_json_path = config_dir / "upload.json"
        tips_json_path   = config_dir / "tips.json"
        asset_json_path  = config_dir / "asset.json"

        task_json_path.write_text(json.dumps(task_json, indent=2))
        upload_json_path.write_text(json.dumps(upload_json, indent=2))
        tips_json_path.write_text(json.dumps(tips_json, indent=2))
        asset_json_path.write_text(json.dumps(asset_json, indent=2))

        # STEP 4 & 5: Upload via rayvision_sync
        logger.info("Uploading to Fox via rayvision_sync...")
        try:
            from rayvision_api import RayvisionAPI
            from rayvision_sync.upload import RayvisionUpload

            api = RayvisionAPI(
                access_id=FOX_ACCESS_ID,
                access_key=FOX_ACCESS_KEY,
                domain=FOX_DOMAIN,
                platform=FOX_PLATFORM,
            )
            upload = RayvisionUpload(api)

            config_file_list = [
                str(task_json_path),
                str(tips_json_path),
                str(asset_json_path),
                str(upload_json_path),
            ]
            logger.info("Uploading config files...")
            upload.upload_config(
                task_id=req.fox_task_id,
                config_file_list=config_file_list,
                server_ip=FOX_TRANSFER_IP,
                server_port=FOX_TRANSFER_PORT,
            )
            logger.info("Config files uploaded ✓")

            logger.info("Uploading scene assets (may take a while)...")
            upload.upload_asset(
                str(upload_json_path),
                engine_type="aspera",
                server_ip=FOX_TRANSFER_IP,
                server_port=FOX_TRANSFER_PORT,
            )
            logger.info("Scene assets uploaded ✓")

        except Exception as e:
            update_job_status(req.job_id, "error", f"File upload to Fox failed: {str(e)}")
            logger.error(f"Upload failed: {e}", exc_info=True)
            return

        # STEP 6: Submit task to Fox
        logger.info(f"Submitting task {req.fox_task_id} to Fox...")
        update_job_status(req.job_id, "pending")

        submit_result = await fox_api("POST", "/api/render/task/submitTask", {
            "task_id_list": [int(req.fox_task_id)]
        })

        if submit_result.get("code") not in [200, "200"]:
            update_job_status(req.job_id, "error",
                f"Fox submitTask failed: {submit_result.get('message')} (code {submit_result.get('code')})")
            return

        logger.info(f"Task {req.fox_task_id} submitted ✓")
        update_job_status(req.job_id, "queued")

        # STEP 7: Start polling
        asyncio.create_task(poll_job_status(req.job_id, req.fox_task_id))

    logger.info(f"Transfer pipeline complete for job {req.job_id}")


# ── POLLING ────────────────────────────────────────────────────
FOX_STATUS_MAP = {
    "0":  "rendering",
    "10": "done",
    "20": "stopped",
    "23": "error",
    "25": "error",
    "30": "stopped",
    "35": "queued",
    "45": "queued",
}

async def poll_job_status(job_id: str, fox_task_id: str):
    logger.info(f"Polling job {job_id} (Fox {fox_task_id})")
    terminal_states = {"done", "error", "stopped", "aborted"}

    while True:
        await asyncio.sleep(30)
        try:
            result = await fox_api("GET", "/api/render/task/taskInfo", {"task_id": fox_task_id})
            if result.get("code") not in [200, "200"]:
                logger.warning(f"Poll failed: {result.get('message')}")
                continue

            task_data  = result.get("data", {})
            status_code = str(task_data.get("taskStatus", "35"))
            new_status  = FOX_STATUS_MAP.get(status_code, "queued")

            frames_result = await fox_api("GET", "/api/render/task/queryTaskFrames", {"task_id": fox_task_id})
            frames_data   = frames_result.get("data", {})
            frames_done   = frames_data.get("completedFrames", 0)
            frames_failed = frames_data.get("failedFrames", 0)
            frames_total  = frames_data.get("totalFrames", 0)

            update_job(job_id, {
                "status":        new_status,
                "frames_done":   frames_done,
                "frames_failed": frames_failed,
                "frames_total":  frames_total,
            })
            logger.info(f"Job {job_id}: {new_status} | {frames_done}/{frames_total} frames")

            if new_status == "done":
                asyncio.create_task(download_outputs(job_id, fox_task_id))
                break

            if new_status in terminal_states:
                break

            if new_status == "rendering":
                asyncio.create_task(fetch_thumbnails(job_id, fox_task_id))

        except Exception as e:
            logger.error(f"Poll error: {e}", exc_info=True)


# ── THUMBNAIL FETCH ────────────────────────────────────────────
async def fetch_thumbnails(job_id: str, fox_task_id: str):
    try:
        result = await fox_api("GET", "/api/render/task/getFrameThumbnail", {"taskId": fox_task_id})
        thumbs = result.get("data", [])
        if not thumbs:
            return
        rows = []
        for thumb in thumbs:
            rows.append({
                "job_id":        job_id,
                "frame_number":  thumb.get("frameNumber"),
                "status":        "done",
                "thumbnail_url": thumb.get("thumbnailUrl"),
            })
        if rows:
            supabase.table("job_frames").upsert(rows, on_conflict="job_id,frame_number").execute()
            logger.info(f"Stored {len(rows)} thumbnails for job {job_id}")
    except Exception as e:
        logger.warning(f"Thumbnail fetch failed: {e}")


# ── OUTPUT DOWNLOAD ────────────────────────────────────────────
async def download_outputs(job_id: str, fox_task_id: str):
    logger.info(f"Downloading outputs for job {job_id}")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()

        try:
            from rayvision_api import RayvisionAPI
            from rayvision_sync.download import RayvisionDownload

            api = RayvisionAPI(
                access_id=FOX_ACCESS_ID,
                access_key=FOX_ACCESS_KEY,
                domain=FOX_DOMAIN,
                platform=FOX_PLATFORM,
            )
            download = RayvisionDownload(api)

            logger.info(f"Downloading frames to {output_dir}...")
            download.auto_download_after_task_completed(
                task_id_list=[int(fox_task_id)],
                local_path=str(output_dir),
                download_filename_format="false",
                engine_type="aspera",
                server_ip=FOX_TRANSFER_IP,
                server_port=FOX_TRANSFER_PORT,
                sleep_time=10,
            )
            logger.info("Frames downloaded from Fox ✓")

        except Exception as e:
            logger.error(f"Fox download failed: {e}", exc_info=True)
            update_job_status(job_id, "error", f"Output download failed: {str(e)}")
            return

        # Upload frames to Supabase Storage
        uploaded_urls = []
        for frame_file in sorted(output_dir.rglob("*")):
            if not frame_file.is_file():
                continue
            storage_key = f"{job_id}/{frame_file.name}"
            try:
                with open(frame_file, "rb") as f:
                    supabase.storage.from_(BUCKET_OUTPUT).upload(
                        storage_key, f,
                        {"content-type": "application/octet-stream", "x-upsert": "true"}
                    )
                url = supabase.storage.from_(BUCKET_OUTPUT).get_public_url(storage_key)
                uploaded_urls.append({"filename": frame_file.name, "url": url, "storage_key": storage_key})
                logger.info(f"Uploaded: {frame_file.name}")
            except Exception as e:
                logger.warning(f"Failed to upload {frame_file.name}: {e}")

        update_job(job_id, {
            "status":       "done",
            "output_files": uploaded_urls,
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        logger.info(f"Job {job_id} complete. {len(uploaded_urls)} frames uploaded ✓")


# ── API ENDPOINTS ──────────────────────────────────────────────
def verify_secret(x_worker_secret: Optional[str] = Header(None)):
    if x_worker_secret != WORKER_SECRET:
        raise HTTPException(status_code=401, detail="Invalid worker secret")

@app.get("/")
async def health():
    return {
        "status": "online",
        "service": "RenderRay Worker",
        "fox_domain": FOX_DOMAIN,
        "platform": FOX_PLATFORM,
    }

@app.post("/transfer")
async def start_transfer(
    req: TransferJobRequest,
    background_tasks: BackgroundTasks,
    x_worker_secret: Optional[str] = Header(None)
):
    verify_secret(x_worker_secret)
    background_tasks.add_task(run_transfer_job, req)
    return {"accepted": True, "job_id": req.job_id, "fox_task_id": req.fox_task_id}

@app.post("/download")
async def trigger_download(
    req: DownloadJobRequest,
    background_tasks: BackgroundTasks,
    x_worker_secret: Optional[str] = Header(None)
):
    verify_secret(x_worker_secret)
    background_tasks.add_task(download_outputs, req.job_id, req.fox_task_id)
    return {"accepted": True, "job_id": req.job_id}

@app.get("/status/{job_id}")
async def check_status(job_id: str, x_worker_secret: Optional[str] = Header(None)):
    verify_secret(x_worker_secret)
    result = supabase.table("cloud_render_jobs").select("*").eq("id", job_id).single().execute()
    return result.data


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), reload=False)
