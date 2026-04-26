# Content Pipeline Recorder v2.1 - per-session tracking + title/project tagging + session catalog
# Each Claude conversation gets its own stitched video, tagged with conversation title and project.
# Session catalog persists to JSON for easy lookup of what each video contains.
import os, subprocess, threading, time, uuid, json, re
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="Milly Recorder", version="2.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

OUTPUT_DIR = Path(r"C:\Projects\Millyweb\content-pipeline\outputs\recordings")
CATALOG_PATH = Path(r"C:\Projects\Millyweb\content-pipeline\outputs\session-catalog.json")
INACTIVITY_TIMEOUT = 600
PORT = 8765
FFMPEG_PATH = r"C:\Users\Rdmil\AppData\Local\Microsoft\WinGet\Links\ffmpeg.exe"
FFMPEG_INPUT_ARGS = ["-f", "gdigrab", "-framerate", "15", "-draw_mouse", "1", "-i", "desktop"]
AUDIO_DEVICE = (os.environ.get("AUDIO_DEVICE", "") or "").strip() or None
ORCHESTRATOR_URL = "http://localhost:8766/process"

# ── PROJECT CLASSIFIER ────────────────────────────────────────────────────────
# Maps conversation titles to known projects via keyword matching.
# Order matters — first match wins. More specific patterns first.
PROJECT_PATTERNS = [
    (r"membrain|memory.?ext|chrome.?ext|hud|token.?compress", "membrain"),
    (r"content.?pipeline|recorder|transcrib|orchestrat|video.?pipe|screen.?record", "content-pipeline"),
    (r"lead.?pipe|scraper|enrichment|ccb|camoufox|cold.?email|outreach", "lead-pipeline"),
    (r"helix|cortex|forge|atom|molecule|kb.?index|knowledge.?graph", "helix-cortex"),
    (r"paving|agent.?mesh|discovery.?scan|node.?agent", "paving-agent"),
    (r"shanghai|oculus|anubis|tiktok|aos|investigation", "adventures-of-shanghai"),
    (r"mw.?dev|website|portfolio|carousel|landing.?page|milly\.?web", "mw-development"),
    (r"provisioner|mcp.?gate|mcp.?server|tool.?registry", "provisioner"),
    (r"confidence|christmas|lights|install", "confidence-lighting"),
    (r"nanny|just.?little|ashley|childcare", "just-little-steps"),
    (r"gnome|household|jerry|telegram", "project-gnome"),
    (r"postiz|social.?media|posting|schedule", "postiz"),
    (r"revenue|business|strategy|pricing|client", "business-strategy"),
    (r"infra|docker|traefik|vps|container|deploy|server", "infrastructure"),
]

def classify_project(title: str) -> str:
    """Classify a conversation title into a known project. Returns 'unknown' if no match."""
    if not title:
        return "unknown"
    lower = title.lower()
    for pattern, project in PROJECT_PATTERNS:
        if re.search(pattern, lower):
            return project
    return "unknown"

def clean_title(raw_title: str) -> str:
    """Strip platform suffixes from document.title. 'My Chat - Claude' -> 'My Chat'"""
    if not raw_title:
        return ""
    # Strip common suffixes
    for suffix in [" - Claude", " | Claude", " - Anthropic", " | Anthropic"]:
        if raw_title.endswith(suffix):
            raw_title = raw_title[:-len(suffix)]
    return raw_title.strip()

def safe_filename(text: str, max_len: int = 40) -> str:
    """Convert text to a safe filename fragment."""
    safe = re.sub(r'[^a-zA-Z0-9_-]', '_', text.lower())
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe[:max_len]

# ── SESSION CATALOG ─────────────────────────────────────────────────────────
def _load_catalog() -> dict:
    try:
        if CATALOG_PATH.exists():
            return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"sessions": {}}

def _save_catalog(catalog: dict):
    try:
        CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CATALOG_PATH.write_text(json.dumps(catalog, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        print(f"[recorder] catalog save failed: {e}")

def _catalog_update(session_id: str, title: str, project: str, video_path: str = None):
    """Update or create a session entry in the persistent catalog."""
    catalog = _load_catalog()
    now = datetime.now().isoformat()
    if session_id not in catalog["sessions"]:
        catalog["sessions"][session_id] = {
            "title": title,
            "project": project,
            "first_seen": now,
            "last_seen": now,
            "video_path": None,
            "work_sessions": [],
        }
    entry = catalog["sessions"][session_id]
    entry["last_seen"] = now
    if title and title != entry.get("title"):
        entry["title"] = title
        entry["project"] = classify_project(title)
    if video_path:
        entry["video_path"] = str(video_path)
        if work_session_id and work_session_id not in entry["work_sessions"]:
            entry["work_sessions"].append(work_session_id)
    _save_catalog(catalog)

# ── STATE ─────────────────────────────────────────────────────────────────────
ffmpeg_proc = None
current_segment_file = None
work_session_id = ""
current_session_id = ""
segment_counter = 0
session_segments = {}            # {session_id: [path, path, ...]}
session_metadata = {}            # {session_id: {title, project, first_seen}}
last_heartbeat_at = None
is_paused = False
state_lock = threading.Lock()
pending_pause_timer = None
PAUSE_DEBOUNCE_MS = 300


def _new_segment_path(session_id: str):
    global segment_counter
    segment_counter += 1
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    meta = session_metadata.get(session_id, {})
    project = meta.get("project", "unknown")
    sid_short = (session_id or "nosid")[:8]
    return OUTPUT_DIR / f"work_{work_session_id}_{project}_sid_{sid_short}_seg{segment_counter:02d}_{ts}.mkv"


def _launch_ffmpeg(output_path):
    cmd = [FFMPEG_PATH, "-y"] + list(FFMPEG_INPUT_ARGS)
    if AUDIO_DEVICE:
        cmd += ["-f", "dshow", "-i", f"audio={AUDIO_DEVICE}", "-c:a", "aac", "-b:a", "128k"]
    cmd += ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "32", str(output_path)]
    return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _stop_ffmpeg_gracefully():
    global ffmpeg_proc
    if ffmpeg_proc is None:
        return
    if ffmpeg_proc.poll() is None:
        try:
            ffmpeg_proc.stdin.write(b"q")
            ffmpeg_proc.stdin.flush()
            ffmpeg_proc.wait(timeout=10)
        except Exception:
            try:
                ffmpeg_proc.terminate()
                ffmpeg_proc.wait(timeout=5)
            except Exception:
                ffmpeg_proc.kill()
    ffmpeg_proc = None


def _start_new_segment(session_id: str):
    global ffmpeg_proc, current_segment_file
    output_path = _new_segment_path(session_id)
    current_segment_file = output_path
    ffmpeg_proc = _launch_ffmpeg(output_path)
    if session_id not in session_segments:
        session_segments[session_id] = []
    session_segments[session_id].append(output_path)
    meta = session_metadata.get(session_id, {})
    print(f"[recorder] segment {segment_counter} started for {meta.get('project','?')}/{session_id[:8]}: {output_path.name}")


def _stitch_session(session_id: str) -> Path | None:
    segments = session_segments.get(session_id, [])
    valid = [p for p in segments if p.exists() and p.stat().st_size > 0]
    if not valid:
        return None
    meta = session_metadata.get(session_id, {})
    project = safe_filename(meta.get("project", "unknown"))
    title_slug = safe_filename(meta.get("title", ""), 30)
    name_parts = ["session", project]
    if title_slug:
        name_parts.append(title_slug)
    name_parts.append(session_id[:12])
    name_parts.append(work_session_id)
    combined = OUTPUT_DIR / ("_".join(name_parts) + "_combined.mkv")
    if len(valid) == 1:
        valid[0].rename(combined)
        size_mb = combined.stat().st_size / (1024 * 1024)
        print(f"[recorder] single segment for {project}/{session_id[:8]} -> {combined.name} ({size_mb:.1f} MB)")
        return combined
    list_file = OUTPUT_DIR / f"concat_{session_id[:12]}_{work_session_id}.txt"
    list_file.write_text(chr(10).join(f"file '{p.as_posix()}'" for p in valid), encoding="utf-8")
    cmd = [FFMPEG_PATH, "-y", "-f", "concat", "-safe", "0", "-i", str(list_file), "-c", "copy", str(combined)]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=300)
        if r.returncode != 0:
            print(f"[recorder] stitch failed for {session_id[:8]}: {r.stderr[:300]}")
            return valid[0] if valid else None
    except Exception as e:
        print(f"[recorder] stitch exception for {session_id[:8]}: {e}")
        return valid[0] if valid else None
    size_mb = combined.stat().st_size / (1024 * 1024)
    print(f"[recorder] stitched {len(valid)} segments for {project}/{session_id[:8]} -> {combined.name} ({size_mb:.1f} MB)")
    for p in valid:
        try: p.unlink()
        except: pass
    try: list_file.unlink()
    except: pass
    return combined


def _trigger_orchestrator(video_path, session_id):
    import httpx
    meta = session_metadata.get(session_id, {})
    try:
        payload = {
            "video_path": str(video_path),
            "session_id": session_id,
            "session_ids": [session_id],
            "trim_tail_seconds": 0,
            "title": meta.get("title", ""),
            "project": meta.get("project", "unknown"),
        }
        httpx.post(ORCHESTRATOR_URL, json=payload, timeout=5.0)
        print(f"[recorder] orchestrator notified: {video_path.name} project={meta.get('project','?')} session={session_id[:8]}")
    except Exception as e:
        print(f"[recorder] orchestrator notification failed: {e}")


def _terminate_work_session(reason):
    global ffmpeg_proc, current_segment_file, work_session_id
    global segment_counter, current_session_id, last_heartbeat_at, is_paused
    global session_segments, session_metadata
    print(f"[recorder] terminating work session {work_session_id} (reason: {reason})")
    _stop_ffmpeg_gracefully()
    current_segment_file = None
    finished_ws = work_session_id
    sessions_processed = 0
    for sid in session_segments:
        combined = _stitch_session(sid)
        if combined:
            _catalog_update(sid, session_metadata.get(sid, {}).get("title", ""),
                           session_metadata.get(sid, {}).get("project", "unknown"), str(combined))
            _trigger_orchestrator(combined, sid)
            sessions_processed += 1
    # Log summary
    summary = {sid: session_metadata.get(sid, {}).get("project", "?") for sid in session_segments}
    print(f"[recorder] work session {finished_ws} ended: {sessions_processed} videos from {len(session_segments)} sessions: {summary}")
    # Reset
    work_session_id = ""
    session_segments = {}
    session_metadata = {}
    segment_counter = 0
    current_session_id = ""
    last_heartbeat_at = None
    is_paused = False


def _inactivity_watchdog():
    while True:
        time.sleep(30)
        with state_lock:
            if not work_session_id:
                break
            if last_heartbeat_at and (time.time() - last_heartbeat_at) >= INACTIVITY_TIMEOUT:
                _terminate_work_session(f"idle {INACTIVITY_TIMEOUT}s")
                break


@app.post("/record/heartbeat")
async def heartbeat(body: dict = None):
    global work_session_id, current_session_id, last_heartbeat_at, is_paused
    body = body or {}
    sid = (body.get("session_id") or "").strip()
    raw_title = (body.get("title") or "").strip()
    title = clean_title(raw_title)
    started_ws = False
    resumed = False
    session_switched = False
    with state_lock:
        last_heartbeat_at = time.time()
        # Update or create session metadata
        if sid:
            if sid not in session_metadata:
                project = classify_project(title)
                session_metadata[sid] = {
                    "title": title,
                    "project": project,
                    "first_seen": datetime.now().isoformat(),
                }
                print(f"[recorder] new session: {sid[:8]} project={project} title='{title[:50]}'")
                _catalog_update(sid, title, project)
            elif title and title != session_metadata[sid].get("title"):
                # Title changed (claude sometimes updates it after first message)
                session_metadata[sid]["title"] = title
                session_metadata[sid]["project"] = classify_project(title)
                print(f"[recorder] title updated: {sid[:8]} -> '{title[:50]}' project={session_metadata[sid]['project']}")
        # Start new work session if none active
        if not work_session_id:
            work_session_id = uuid.uuid4().hex[:8]
            started_ws = True
            print(f"[recorder] work session {work_session_id} STARTED")
            threading.Thread(target=_inactivity_watchdog, daemon=True).start()
        # Handle session_id change (tab switch)
        if sid and sid != current_session_id and current_session_id:
            session_switched = True
            old_proj = session_metadata.get(current_session_id, {}).get("project", "?")
            new_proj = session_metadata.get(sid, {}).get("project", "?")
            print(f"[recorder] session switch: {old_proj}/{current_session_id[:8]} -> {new_proj}/{sid[:8]}")
            _stop_ffmpeg_gracefully()
            current_session_id = sid
            _start_new_segment(sid)
        elif sid:
            current_session_id = sid
        # Start recording if not already
        if ffmpeg_proc is None or ffmpeg_proc.poll() is not None:
            if is_paused:
                resumed = True
                is_paused = False
            _start_new_segment(current_session_id or sid or "unknown")
    meta = session_metadata.get(sid, {})
    return JSONResponse({
        "status": "heartbeat_ok",
        "work_session_id": work_session_id,
        "started": started_ws,
        "resumed": resumed,
        "session_switched": session_switched,
        "current_session_id": current_session_id,
        "title": meta.get("title", ""),
        "project": meta.get("project", "unknown"),
        "sessions_tracked": {sid: session_metadata.get(sid, {}).get("project", "?") for sid in session_segments},
        "segment_count": segment_counter,
    })


def _execute_pause():
    global is_paused, pending_pause_timer
    with state_lock:
        pending_pause_timer = None
        if not work_session_id or is_paused or ffmpeg_proc is None:
            return
        _stop_ffmpeg_gracefully()
        is_paused = True
    print(f"[recorder] paused after segment {segment_counter} ({work_session_id})")


@app.post("/record/pause")
async def pause_recording(body: dict = None):
    global pending_pause_timer
    with state_lock:
        if not work_session_id:
            return JSONResponse({"status": "no_work_session"})
        if is_paused or ffmpeg_proc is None:
            return JSONResponse({"status": "already_paused", "work_session_id": work_session_id})
        if pending_pause_timer is not None:
            pending_pause_timer.cancel()
        pending_pause_timer = threading.Timer(PAUSE_DEBOUNCE_MS / 1000.0, _execute_pause)
        pending_pause_timer.daemon = True
        pending_pause_timer.start()
    return JSONResponse({"status": "pause_scheduled", "work_session_id": work_session_id, "debounce_ms": PAUSE_DEBOUNCE_MS})


@app.post("/record/resume")
async def resume_recording(body: dict = None):
    global is_paused, last_heartbeat_at, pending_pause_timer
    canceled_pending = False
    with state_lock:
        if not work_session_id:
            return JSONResponse({"status": "no_work_session"})
        if pending_pause_timer is not None:
            pending_pause_timer.cancel()
            pending_pause_timer = None
            canceled_pending = True
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
            return JSONResponse({"status": "already_recording", "work_session_id": work_session_id, "canceled_pending_pause": canceled_pending})
        last_heartbeat_at = time.time()
        is_paused = False
        _start_new_segment(current_session_id or "unknown")
    return JSONResponse({"status": "resumed", "work_session_id": work_session_id, "segment_count": segment_counter, "canceled_pending_pause": canceled_pending})


@app.post("/record/stop")
async def stop_recording(body: dict = None):
    with state_lock:
        if not work_session_id:
            return JSONResponse({"status": "no_work_session"})
        wsid = work_session_id
        seg = segment_counter
        sessions = {sid: session_metadata.get(sid, {}).get("project", "?") for sid in session_segments}
        _terminate_work_session("manual stop")
    return JSONResponse({"status": "stopped", "work_session_id": wsid, "segment_count": seg, "sessions_stitched": sessions})


@app.get("/record/status")
def get_status():
    is_recording = ffmpeg_proc is not None and ffmpeg_proc.poll() is None
    idle_seconds = round(time.time() - last_heartbeat_at) if last_heartbeat_at else None
    session_info = {}
    for sid, segs in session_segments.items():
        meta = session_metadata.get(sid, {})
        session_info[sid[:12]] = {
            "segments": len(segs),
            "title": meta.get("title", ""),
            "project": meta.get("project", "unknown"),
        }
    return JSONResponse({
        "recording": is_recording,
        "paused": is_paused,
        "work_session_id": work_session_id,
        "current_segment_file": str(current_segment_file) if current_segment_file else None,
        "segment_count": segment_counter,
        "current_session_id": current_session_id,
        "sessions_tracked": session_info,
        "audio_device": AUDIO_DEVICE,
        "idle_seconds": idle_seconds,
        "timeout_seconds": INACTIVITY_TIMEOUT,
    })


@app.get("/catalog")
def get_catalog():
    """Return the persistent session catalog."""
    catalog = _load_catalog()
    return JSONResponse(catalog)


@app.get("/health")
def health():
    return {"status": "ok", "service": "milly-recorder", "version": "2.1.0"}


if __name__ == "__main__":
    print(f"[recorder] Starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
