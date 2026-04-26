# Orchestrator Service
# The pipeline brain. Called by Recorder when a session ends.
# Runs: transcribe -> pull Helix context -> generate scripts -> voice -> render -> publish
# FastAPI port 8766

import os
import time
import httpx
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import asyncio

app = FastAPI(title="Milly Orchestrator", version="1.0.0")

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE = Path(r"C:\Users\rdmil\OneDrive\Documents\Projects\Millyweb\content-pipeline")
SCRIPTS_DIR  = BASE / "outputs" / "scripts"
VIDEOS_DIR   = BASE / "outputs" / "videos"

# Service URLs (all on localhost Clair)
TRANSCRIBER_URL = "http://localhost:8767"

# External API endpoints
HELIX_URL       = "http://72.60.31.69:9050"   # VPS1 Helix
ANTHROPIC_URL   = "https://api.anthropic.com/v1/messages"
ELEVENLABS_URL  = "https://api.elevenlabs.io/v1"
HEYGEN_URL      = "https://api.heygen.com/v2"
POSTIZ_URL      = "http://72.60.225.81:5000"  # VPS2 Postiz (adjust port if needed)

PORT = 8766

# Active pipeline runs
runs: dict = {}


def load_secrets() -> dict:
    """Load API keys from env (Infisical agent writes to .env or environment)."""
    return {
        "anthropic": os.environ.get("ANTHROPIC_API_KEY", ""),
        "elevenlabs": os.environ.get("Elevenlabs-Api-Key", ""),
        "elevenlabs_voice": os.environ.get("Elevenlabs-Voice-Ashley", ""),
        "heygen": os.environ.get("Heygen-API-Key", ""),
        "heygen_avatar": os.environ.get("Heygen-ID-Ryan", ""),
    }


async def wait_for_transcription(job_id: str, timeout: int = 600) -> dict:
    """Poll transcriber until job is done."""
    start = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start < timeout:
            r = await client.get(f"{TRANSCRIBER_URL}/status/{job_id}")
            data = r.json()
            if data["status"] in ("done", "error"):
                return data
            await asyncio.sleep(5)
    return {"status": "timeout", "error": "Transcription timed out"}


async def pull_helix_context(date_str: str) -> str:
    """Pull today's session summaries from Helix."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{HELIX_URL}/api/v1/sessions",
                params={"date": date_str, "limit": 10},
                timeout=10.0
            )
            sessions = r.json().get("sessions", [])
            context_parts = []
            for s in sessions:
                summary = s.get("summary") or s.get("title", "")
                if summary:
                    context_parts.append(summary)
            return "\n".join(context_parts)
    except Exception as e:
        print(f"[orchestrator] Helix context failed: {e}")
        return ""


async def generate_scripts(transcript: str, helix_context: str, secrets: dict) -> list[str]:
    """Call Claude API to generate 3-6 short-form video scripts."""
    prompt = f"""You are a content strategist for MW Development, an AI automation agency.

Here is a transcript of a work session:
<transcript>
{transcript[:8000]}
</transcript>

Here is context from the AI memory system about what was built today:
<context>
{helix_context[:3000]}
</context>

Generate 4 short-form video scripts (60-90 seconds when spoken aloud) about what was built/accomplished today.
Each script should:
- Open with a strong hook (first 3 seconds matters most)
- Explain ONE thing that was built or solved
- Be conversational, not corporate
- End with a takeaway or insight
- Target: developers, founders, builders who follow AI automation

Format: Return a JSON array of 4 script strings. No preamble, just the JSON."""

    async with httpx.AsyncClient() as client:
        r = await client.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key": secrets["anthropic"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=60.0
        )
        content = r.json()["content"][0]["text"]
        import json
        # Strip any markdown fences
        clean = content.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return json.loads(clean)


async def generate_voiceover(script: str, secrets: dict, output_path: Path) -> bool:
    """Call ElevenLabs to voice a script."""
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{ELEVENLABS_URL}/text-to-speech/{secrets['elevenlabs_voice']}",
            headers={
                "xi-api-key": secrets["elevenlabs"],
                "content-type": "application/json"
            },
            json={
                "text": script,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75}
            },
            timeout=60.0
        )
        if r.status_code == 200:
            output_path.write_bytes(r.content)
            return True
        print(f"[orchestrator] ElevenLabs error: {r.status_code} {r.text[:200]}")
        return False


async def render_heygen_video(audio_path: Path, secrets: dict) -> str | None:
    """Submit HeyGen video generation job, return video URL."""
    # Upload audio to HeyGen first
    async with httpx.AsyncClient() as client:
        # Step 1: upload audio
        with open(audio_path, "rb") as f:
            upload_r = await client.post(
                f"{HEYGEN_URL}/asset",
                headers={"x-api-key": secrets["heygen"]},
                files={"file": (audio_path.name, f, "audio/mpeg")},
                timeout=60.0
            )
        if upload_r.status_code != 200:
            print(f"[orchestrator] HeyGen upload failed: {upload_r.status_code}")
            return None
        asset_id = upload_r.json().get("data", {}).get("asset_id")

        # Step 2: create video
        video_r = await client.post(
            f"{HEYGEN_URL}/video/generate",
            headers={"x-api-key": secrets["heygen"], "content-type": "application/json"},
            json={
                "video_inputs": [{
                    "character": {
                        "type": "avatar",
                        "avatar_id": secrets["heygen_avatar"],
                        "avatar_style": "normal"
                    },
                    "voice": {
                        "type": "audio",
                        "audio_asset_id": asset_id
                    }
                }],
                "dimension": {"width": 1080, "height": 1920},  # vertical for TikTok/Reels
                "aspect_ratio": "9:16"
            },
            timeout=30.0
        )
        if video_r.status_code != 200:
            print(f"[orchestrator] HeyGen create failed: {video_r.status_code}")
            return None
        video_id = video_r.json().get("data", {}).get("video_id")

        # Step 3: poll for completion
        for _ in range(60):  # up to 10 min
            await asyncio.sleep(10)
            status_r = await client.get(
                f"{HEYGEN_URL}/video/{video_id}",
                headers={"x-api-key": secrets["heygen"]},
                timeout=15.0
            )
            status = status_r.json().get("data", {}).get("status")
            if status == "completed":
                return status_r.json()["data"]["video_url"]
            if status == "failed":
                return None

    return None


async def run_pipeline(run_id: str, video_path: str, trim_tail_seconds: int):
    """Full pipeline execution."""
    runs[run_id]["status"] = "transcribing"
    secrets = load_secrets()
    today = datetime.now().strftime("%Y-%m-%d")
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    VIDEOS_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Transcribe
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{TRANSCRIBER_URL}/transcribe",
            json={"video_path": video_path, "trim_tail_seconds": trim_tail_seconds},
            timeout=10.0
        )
        job_id = r.json()["job_id"]
    
    result = await wait_for_transcription(job_id)
    if result["status"] != "done":
        runs[run_id]["status"] = "error"
        runs[run_id]["error"] = f"Transcription failed: {result.get('error')}"
        return

    transcript_path = result["transcript_path"]
    transcript = Path(transcript_path).read_text(encoding="utf-8")
    print(f"[orchestrator] Transcript ready: {len(transcript)} chars")

    # Step 2: Pull Helix context
    runs[run_id]["status"] = "pulling_context"
    helix_context = await pull_helix_context(today)
    print(f"[orchestrator] Helix context: {len(helix_context)} chars")

    # Step 3: Generate scripts
    runs[run_id]["status"] = "generating_scripts"
    try:
        scripts = await generate_scripts(transcript, helix_context, secrets)
        print(f"[orchestrator] {len(scripts)} scripts generated")
    except Exception as e:
        runs[run_id]["status"] = "error"
        runs[run_id]["error"] = f"Script generation failed: {e}"
        return

    # Save scripts
    scripts_file = SCRIPTS_DIR / f"scripts_{today}_{run_id}.json"
    import json
    scripts_file.write_text(json.dumps(scripts, indent=2), encoding="utf-8")

    # Step 4: For each script - voice, render, queue
    runs[run_id]["status"] = "producing_videos"
    produced = []
    for i, script in enumerate(scripts[:3]):  # cap at 3 videos per session
        print(f"[orchestrator] Producing video {i+1}/{min(len(scripts),3)}")
        
        # Voiceover
        audio_path = VIDEOS_DIR / f"{run_id}_v{i+1}.mp3"
        ok = await generate_voiceover(script, secrets, audio_path)
        if not ok:
            continue

        # HeyGen render
        video_url = await render_heygen_video(audio_path, secrets)
        if video_url:
            produced.append({"script": script[:100], "video_url": video_url})
            print(f"[orchestrator] Video {i+1} ready: {video_url[:60]}")

    runs[run_id]["status"] = "done"
    runs[run_id]["videos_produced"] = len(produced)
    runs[run_id]["produced"] = produced
    print(f"[orchestrator] Pipeline complete: {len(produced)} videos produced")


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.post("/process")
async def process(body: dict):
    """Entry point. Called by Recorder when a session ends."""
    video_path = body.get("video_path")
    trim_tail = body.get("trim_tail_seconds", 600)
    if not video_path:
        return JSONResponse({"error": "video_path required"}, status_code=400)
    
    run_id = f"run_{int(time.time())}"
    runs[run_id] = {"status": "starting", "video_path": video_path}
    asyncio.create_task(run_pipeline(run_id, video_path, trim_tail))
    return JSONResponse({"run_id": run_id, "status": "started"})


@app.get("/status/{run_id}")
def get_status(run_id: str):
    if run_id not in runs:
        return JSONResponse({"error": "run not found"}, status_code=404)
    return JSONResponse(runs[run_id])


@app.get("/runs")
def list_runs():
    return JSONResponse({k: v.get("status") for k, v in runs.items()})


@app.get("/health")
def health():
    return {"status": "ok", "service": "milly-orchestrator"}


if __name__ == "__main__":
    print(f"[orchestrator] Starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
