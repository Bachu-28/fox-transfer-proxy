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

# ── CONFIG (Railway Variables) ────────────────────────────────
FOX_ACCESS_ID          = os.environ["FOX_ACCESS_ID"]
FOX_ACCESS_KEY         = os.environ["FOX_ACCESS_KEY"]
FOX_USER_ID            = os.environ["FOX_USER_ID"]
FOX_DOMAIN             = os.environ.get("FOX_DOMAIN", "jop.foxrenderfarm.com")
FOX_PLATFORM           = os.environ.get("FOX_PLATFORM", "62")
FOX_TRANSFER_IP        = os.environ.get("FOX_TRANSFER_SERVER_IP", "45.251.92.16")
FOX_TRANSFER_PORT      = os.environ.get("FOX_TRANSFER_SERVER_PORT", "12121")

# NEW SUPABASE CREDENTIALS
SUPABASE_URL           = os.environ["SUPABASE_URL"]
SUPABASE_KEY           = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
# Updated default to 'foxrender' as per your screenshot
BUCKET_INPUT           = os.environ.get("SUPABASE_STORAGE_BUCKET_INPUT", "foxrender")
BUCKET_OUTPUT          = os.environ.get("SUPABASE_STORAGE_BUCKET_OUTPUT", "render-outputs")
WORKER_SECRET          = os.environ.get("WORKER_SECRET", "changeme")

# Initialize Client
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
    signed_url: Optional[str] = None

class DownloadJobRequest(BaseModel):
    job_id: str
    fox_task_id: str
    user_id: str

# ── FOX AUTH & HELPERS ────────────────────────────────────────
def generate_fox_headers() -> dict:
    timestamp = str(int(time.time()))
    nonce = base64.urlsafe_b64encode(os.urandom(6)).decode()[:8]
    message = FOX_ACCESS_KEY + timestamp + nonce
    sig = hmac.new(FOX_ACCESS_KEY.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    signature = base64.b64encode(sig).decode()
    return {
        "accessId": FOX_ACCESS_ID,
        "channel": "4",
        "platform": FOX_PLATFORM,
        "UTCTimestamp": timestamp,
        "nonce": nonce,
        "signature": signature,
        "version": "1.0.0",
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
    return resp.json()

def update_job_status(job_id: str, status: str, error: str = None):
    payload = {"status": status}
    if error: payload["error_message"] = error
    supabase.table("cloud_render_jobs").update(payload).eq("id", job_id).execute()
    logger.info(f"Job {job_id} → {status}")

# ── CORE TRANSFER FUNCTION ─────────────────────────────────────
async def run_transfer_job(req: TransferJobRequest):
    logger.info(f"▶ Starting transfer for job {req.job_id}")
    update_job_status(req.job_id, "uploading")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "scene.zip"
        extract_dir = tmp / "extracted"
        extract_dir.mkdir()

        # STEP 1: Download ZIP using Official SDK Public URL
        try:
            # This handles the URL construction for your project correctly
            res = supabase.storage.from_(BUCKET_INPUT).get_public_url(req.storage_path)
            public_download_url = res
            
            logger.info(f"Downloading from Supabase: {public_download_url}")
            
            async with httpx.AsyncClient(timeout=600) as client:
                async with client.stream("GET", public_download_url) as resp:
                    resp.raise_for_status()
                    with open(zip_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
        except Exception as e:
            update_job_status(req.job_id, "error", f"Supabase download failed: {str(e)}")
            return

        # [The rest of your logic for extraction and Fox uploading remains the same...]
        # (Be sure to paste your remaining build_task_json and upload logic here)

# [Remaining API Endpoints stay the same]
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
