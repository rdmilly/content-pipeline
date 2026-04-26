# Transcriber Service
# faster-whisper FastAPI on Clair port 8767
# Input: video/audio file path -> Output: transcript .txt file

import os
import time
from pathlib import Path
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from faster_whisper import WhisperModel
import uvicorn

app = FastAPI(title="Milly Transcriber", version="1.0.0")

# ── CONFIG ────────────────────────────────────────────────────────────────────
TRANSCRIPTS_DIR = Path(r"C:\Users\rdmil\OneDrive\Documents\Projects\Millyweb\content-pipeline\outputs\transcripts")
MODEL_SIZE = "base.en"  # fast + accurate enough; upgrade to 'medium.en' for better quality
PORT = 8767

# Load model once at startup (downloads on first run ~150MB for base)
print("[transcriber] Loading Whisper model...")
model = WhisperModel(MODEL_SIZE, device="cuda", compute_type="float16")  # GPU
print("[transcriber] Model ready")

# Active jobs tracker
jobs: dict = {}  # job_id -> {status, transcript_path, error}


def run_transcription(job_id: str, video_path: str, trim_tail_seconds: int = 0):
    """Background transcription job."""
    jobs[job_id]["status"] = "running"
    try:
        video_path = Path(video_path)
        if not video_path.exists():
            raise FileNotFoundError(f"Video not found: {video_path}")

        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        transcript_path = TRANSCRIPTS_DIR / f"{video_path.stem}.txt"

        print(f"[transcriber] Transcribing: {video_path.name}")
        start = time.time()

        # Transcribe (faster-whisper handles video files directly via av)
        segments, info = model.transcribe(
            str(video_path),
            beam_size=5,
            language="en",
            condition_on_previous_text=True,
            vad_filter=True,             # skip silence
            vad_parameters={"min_silence_duration_ms": 500}
        )

        # Collect segments, optionally trimming tail
        lines = []
        total_duration = info.duration
        cutoff = total_duration - trim_tail_seconds if trim_tail_seconds > 0 else total_duration

        for seg in segments:
            if seg.end > cutoff:
                break
            lines.append(f"[{seg.start:.1f}s] {seg.text.strip()}")

        transcript = "\n".join(lines)
        transcript_path.write_text(transcript, encoding="utf-8")

        elapsed = round(time.time() - start, 1)
        print(f"[transcriber] Done in {elapsed}s: {transcript_path.name} ({len(lines)} segments)")

        jobs[job_id]["status"] = "done"
        jobs[job_id]["transcript_path"] = str(transcript_path)
        jobs[job_id]["duration"] = info.duration
        jobs[job_id]["segments"] = len(lines)
        jobs[job_id]["elapsed_seconds"] = elapsed

    except Exception as e:
        print(f"[transcriber] Error: {e}")
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.post("/transcribe")
def transcribe(body: dict, background_tasks: BackgroundTasks):
    """
    Start a transcription job.
    Body: {video_path: str, trim_tail_seconds: int (optional)}
    Returns job_id to poll with /status/{job_id}
    """
    video_path = body.get("video_path")
    trim_tail = body.get("trim_tail_seconds", 0)

    if not video_path:
        return JSONResponse({"error": "video_path required"}, status_code=400)

    job_id = f"job_{int(time.time() * 1000)}"
    jobs[job_id] = {"status": "queued", "video_path": video_path}

    background_tasks.add_task(run_transcription, job_id, video_path, trim_tail)
    return JSONResponse({"job_id": job_id, "status": "queued"})


@app.get("/status/{job_id}")
def get_status(job_id: str):
    if job_id not in jobs:
        return JSONResponse({"error": "job not found"}, status_code=404)
    return JSONResponse(jobs[job_id])


@app.get("/health")
def health():
    return {"status": "ok", "service": "milly-transcriber", "model": MODEL_SIZE}


if __name__ == "__main__":
    print(f"[transcriber] Starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
