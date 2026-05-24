# CDP Tab Recorder v1.0 — Content Pipeline
# Records ONLY Claude tab content via Chrome DevTools Protocol
# Replaces gdigrab desktop recorder
# Port 8765 (drop-in replacement)

import asyncio
import json
import hashlib
import time
import uuid
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from contextlib import suppress

import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# ── CONFIG ─────────────────────────────────────────────────────────────────
CDP_HOST = "localhost"
CDP_PORT = 9222
CLAUDE_URL_PATTERN = "claude.ai/chat"
INACTIVITY_TIMEOUT = 600  # 10 min no frames → end work session
SCREENCAST_FPS = 15       # everyNthFrame = max(1, round(60/this))
SCREENCAST_QUALITY = 80
SCREENCAST_MAX_W = 1920
SCREENCAST_MAX_H = 1080
POLL_INTERVAL = 5         # seconds between tab discovery polls

BASE = Path(r"C:\Projects\Millyweb\content-pipeline")
RECORDINGS_DIR = BASE / "outputs" / "recordings"
CATALOG_PATH = BASE / "outputs" / "session-catalog.json"
FFMPEG = r"C:\Users\Rdmil\AppData\Local\Microsoft\WinGet\Links\ffmpeg.exe"
ORCHESTRATOR_URL = "http://localhost:8766"

PORT = 8765
app = FastAPI(title="CDP Tab Recorder", version="1.0.0")

# ── STATE ──────────────────────────────────────────────────────────────────
work_session_id = None
current_session_id = None
current_tab_ws_url = None
prev_frame_hash = None
ffmpeg_proc = None
frame_count = 0
last_frame_at = None
is_capturing = False
segment_counter = 0
session_segments = {}    # session_id -> [Path, ...]
session_metadata = {}    # session_id -> {title, project, first_seen}
state_lock = threading.Lock()


# ── CATALOG ────────────────────────────────────────────────────────────────
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
        print(f"[cdp-rec] catalog save failed: {e}")

def _catalog_update(session_id: str, title: str = "", project: str = "unknown", video_path: str = None):
    catalog = _load_catalog()
    now = datetime.now().isoformat()
    if session_id not in catalog["sessions"]:
        catalog["sessions"][session_id] = {
            "title": title, "project": project,
            "first_seen": now, "last_seen": now,
            "video_path": None, "work_sessions": [],
        }
    entry = catalog["sessions"][session_id]
    entry["last_seen"] = now
    if title and title != entry.get("title"):
        entry["title"] = title
    if project and project != "unknown":
        entry["project"] = project
    if video_path:
        entry["video_path"] = video_path
    if work_session_id and work_session_id not in entry.get("work_sessions", []):
        entry.setdefault("work_sessions", []).append(work_session_id)
    _save_catalog(catalog)


# ── HELPERS ─────────────────────────────────────────────────────────────────
def safe_filename(s: str, max_len: int = 50) -> str:
    import re
    s = re.sub(r'[^a-zA-Z0-9_\-]', '_', s.strip().lower().replace(' ', '_'))
    s = re.sub(r'_+', '_', s).strip('_')
    return s[:max_len]

def classify_project(title: str) -> str:
    """Simple keyword classifier."""
    t = title.lower()
    patterns = {
        "helix-cortex": ["helix", "cortex", "mcp server", "knowledge base"],
        "content-pipeline": ["content pipeline", "recorder", "orchestrat", "transcrib", "elevenlabs", "heygen"],
        "lead-pipeline": ["lead", "scraper", "ccb", "instantly", "cold outreach"],
        "membrain": ["membrain", "memory ext", "chrome ext", "compression", "hud"],
        "mw-development": ["mw dev", "revenuefirst", "portfolio", "ryanmilly"],
        "adventures-of-shanghai": ["shanghai", "oculus", "anubis", "tiktok"],
        "infrastructure": ["docker", "vps", "container", "deploy", "watchtower", "obsidian"],
    }
    for proj, keywords in patterns.items():
        if any(k in t for k in keywords):
            return proj
    return "unknown"


# ── CDP CONNECTION ─────────────────────────────────────────────────────────
async def get_claude_tabs() -> list:
    """Query Chrome's CDP endpoint for open Claude tabs."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://{CDP_HOST}:{CDP_PORT}/json", timeout=3)
            tabs = r.json()
            return [t for t in tabs if CLAUDE_URL_PATTERN in t.get("url", "") and t.get("type") == "page"]
    except Exception:
        return []


def extract_session_id_from_url(url: str) -> str:
    """Extract conversation UUID from claude.ai URL."""
    # URL format: https://claude.ai/chat/ce78b7da-ed39-4c66-a532-1c6dfa811362
    parts = url.rstrip("/").split("/")
    if len(parts) >= 2 and "chat" in parts:
        idx = parts.index("chat")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return ""


def extract_title_from_tab(tab: dict) -> str:
    """Get conversation title from tab title."""
    title = tab.get("title", "")
    # Claude tab titles are usually "Claude" or "Conversation Title - Claude"
    if " - Claude" in title:
        title = title.replace(" - Claude", "").strip()
    elif title == "Claude":
        title = ""
    return title


# ── FFMPEG PIPE ────────────────────────────────────────────────────────────
def _start_ffmpeg(output_path: Path):
    """Start ffmpeg process that accepts JPEG frames on stdin."""
    global ffmpeg_proc
    RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        FFMPEG, "-y",
        "-f", "image2pipe",
        "-framerate", "10",    # output framerate (unique frames are irregular)
        "-i", "pipe:0",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        str(output_path)
    ]
    ffmpeg_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"[cdp-rec] ffmpeg started → {output_path.name}")

def _stop_ffmpeg():
    """Gracefully close ffmpeg."""
    global ffmpeg_proc
    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        try:
            ffmpeg_proc.stdin.close()
            ffmpeg_proc.wait(timeout=10)
        except Exception:
            ffmpeg_proc.kill()
    ffmpeg_proc = None


def _write_frame(frame_bytes: bytes) -> bool:
    """Write a frame to ffmpeg, with dedup."""
    global prev_frame_hash, frame_count, last_frame_at
    h = hashlib.md5(frame_bytes).hexdigest()
    if h == prev_frame_hash:
        return False  # duplicate frame, skip
    prev_frame_hash = h
    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        try:
            ffmpeg_proc.stdin.write(frame_bytes)
            frame_count += 1
            last_frame_at = time.time()
            return True
        except (BrokenPipeError, OSError):
            pass
    return False


# ── SEGMENT MANAGEMENT ─────────────────────────────────────────────────────
def _new_segment_path(session_id: str) -> Path:
    global segment_counter
    segment_counter += 1
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    wsid = work_session_id or "nows"
    sid_short = session_id[:8] if session_id else "unknown"
    name = f"cdp_{wsid}_sid_{sid_short}_seg{segment_counter:02d}_{ts}.mkv"
    return RECORDINGS_DIR / name


def _stitch_master(session_id: str) -> Path | None:
    """Stitch segments into a master video for this conversation."""
    segments = session_segments.get(session_id, [])
    valid = [p for p in segments if p.exists() and p.stat().st_size > 0]
    if not valid:
        return None

    meta = session_metadata.get(session_id, {})
    project = safe_filename(meta.get("project", "unknown"))
    title_slug = safe_filename(meta.get("title", ""), 50)

    # Catalog-first: use existing video_path if available
    cat = _load_catalog()
    cat_entry = cat.get("sessions", {}).get(session_id, {})
    existing_vp = cat_entry.get("video_path")

    if existing_vp and Path(existing_vp).exists():
        master_path = Path(existing_vp)
    else:
        fs = cat_entry.get("first_seen", "")
        date_str = fs[:10] if fs else datetime.now().strftime("%Y-%m-%d")
        name_parts = ["master", date_str, project]
        if title_slug:
            name_parts.append(title_slug)
        name_parts.append(session_id[:12])
        master_path = RECORDINGS_DIR / ("_".join(name_parts) + ".mkv")

    # Build concat list
    all_parts = []
    if master_path.exists() and master_path.stat().st_size > 0:
        temp = RECORDINGS_DIR / (master_path.stem + "_prev.mkv")
        master_path.rename(temp)
        all_parts.append(temp)
    all_parts.extend(valid)

    if len(all_parts) == 1:
        all_parts[0].rename(master_path)
    else:
        concat_file = RECORDINGS_DIR / f"_concat_{session_id[:8]}.txt"
        concat_file.write_text("\n".join(f"file '{p}'" for p in all_parts), encoding="utf-8")
        cmd = [FFMPEG, "-y", "-f", "concat", "-safe", "0", "-i", str(concat_file), "-c", "copy", str(master_path)]
        subprocess.run(cmd, capture_output=True, timeout=120)
        concat_file.unlink(missing_ok=True)
        # Clean up prev
        for p in all_parts:
            if "_prev" in p.name:
                p.unlink(missing_ok=True)

    if master_path.exists():
        size_mb = master_path.stat().st_size / (1024 * 1024)
        print(f"[cdp-rec] master: {master_path.name} ({size_mb:.1f} MB)")
        _catalog_update(session_id, video_path=str(master_path))
        return master_path
    return None


# ── WORK SESSION LIFECYCLE ─────────────────────────────────────────────────
def _start_work_session():
    global work_session_id, segment_counter, frame_count, prev_frame_hash
    work_session_id = uuid.uuid4().hex[:8]
    segment_counter = 0
    frame_count = 0
    prev_frame_hash = None
    print(f"[cdp-rec] work session {work_session_id} STARTED")


def _end_work_session(reason: str = "idle"):
    global work_session_id, current_session_id, is_capturing, frame_count
    _stop_ffmpeg()
    wsid = work_session_id
    total_frames = frame_count
    masters = {}
    for sid in list(session_segments.keys()):
        m = _stitch_master(sid)
        if m:
            masters[sid] = str(m)
    # POST to orchestrator for each session
    for sid, vpath in masters.items():
        try:
            import requests
            requests.post(f"{ORCHESTRATOR_URL}/process",
                json={"video_path": vpath, "session_id": sid, "trim_tail_seconds": 0},
                timeout=5)
            print(f"[cdp-rec] POST to orchestrator: {sid[:8]} → {Path(vpath).name}")
        except Exception as e:
            print(f"[cdp-rec] orchestrator POST failed: {e}")

    session_segments.clear()
    work_session_id = None
    current_session_id = None
    is_capturing = False
    frame_count = 0
    print(f"[cdp-rec] work session {wsid} ENDED ({reason}): {total_frames} frames, {len(masters)} masters")


# ── SCREENCAST LOOP ────────────────────────────────────────────────────────
async def screencast_loop():
    """Main loop: discover Claude tabs, capture frames."""
    global current_session_id, current_tab_ws_url, is_capturing, last_frame_at

    while True:
        tabs = await get_claude_tabs()

        if not tabs:
            # No Claude tabs open
            if is_capturing:
                if last_frame_at and (time.time() - last_frame_at) > INACTIVITY_TIMEOUT:
                    with state_lock:
                        _end_work_session("no_claude_tabs")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        # Find the active (frontmost) Claude tab
        # CDP doesn't directly tell us which tab is focused, so we pick
        # the first one (Chrome returns tabs in MRU order usually)
        target_tab = tabs[0]
        tab_url = target_tab.get("url", "")
        tab_ws = target_tab.get("webSocketDebuggerUrl", "")
        tab_session_id = extract_session_id_from_url(tab_url)
        tab_title = extract_title_from_tab(target_tab)

        if not tab_ws or not tab_session_id:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        # Start work session if needed
        with state_lock:
            if not work_session_id:
                _start_work_session()

            # Track session metadata
            if tab_session_id not in session_metadata:
                project = classify_project(tab_title)
                session_metadata[tab_session_id] = {
                    "title": tab_title, "project": project,
                    "first_seen": datetime.now().isoformat(),
                }
                _catalog_update(tab_session_id, tab_title, project)
                print(f"[cdp-rec] new session: {tab_session_id[:8]} project={project} title='{tab_title[:50]}'")
            elif tab_title and tab_title != session_metadata[tab_session_id].get("title"):
                session_metadata[tab_session_id]["title"] = tab_title
                session_metadata[tab_session_id]["project"] = classify_project(tab_title)

            # Session switch?
            if tab_session_id != current_session_id and current_session_id:
                print(f"[cdp-rec] session switch: {current_session_id[:8]} → {tab_session_id[:8]}")
                _stop_ffmpeg()

            current_session_id = tab_session_id

        # Connect to tab and start screencast
        if tab_ws != current_tab_ws_url or not is_capturing:
            current_tab_ws_url = tab_ws
            try:
                await _run_screencast(tab_ws, tab_session_id)
            except Exception as e:
                print(f"[cdp-rec] screencast error: {type(e).__name__}: {e}")
                is_capturing = False
                await asyncio.sleep(POLL_INTERVAL)


async def _run_screencast(ws_url: str, session_id: str):
    """Connect to a tab and receive screencast frames."""
    global is_capturing, last_frame_at

    seg_path = _new_segment_path(session_id)
    session_segments.setdefault(session_id, []).append(seg_path)
    _start_ffmpeg(seg_path)
    is_capturing = True
    msg_id = 1

    async with websockets.connect(ws_url, max_size=50 * 1024 * 1024) as ws:
        # Start screencast
        await ws.send(json.dumps({
            "id": msg_id,
            "method": "Page.startScreencast",
            "params": {
                "format": "jpeg",
                "quality": SCREENCAST_QUALITY,
                "maxWidth": SCREENCAST_MAX_W,
                "maxHeight": SCREENCAST_MAX_H,
                "everyNthFrame": max(1, round(60 / SCREENCAST_FPS)),
            }
        }))
        msg_id += 1
        print(f"[cdp-rec] screencast started for {session_id[:8]}")

        try:
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=POLL_INTERVAL * 2)
                except asyncio.TimeoutError:
                    # No frames received — check if we should stop
                    if last_frame_at and (time.time() - last_frame_at) > INACTIVITY_TIMEOUT:
                        with state_lock:
                            _end_work_session(f"idle {INACTIVITY_TIMEOUT}s")
                        return
                    # Check if tab is still a Claude tab
                    tabs = await get_claude_tabs()
                    current_urls = [t.get("webSocketDebuggerUrl") for t in tabs]
                    if ws_url not in current_urls:
                        print(f"[cdp-rec] tab closed or navigated away")
                        _stop_ffmpeg()
                        is_capturing = False
                        return
                    continue

                msg = json.loads(raw)
                method = msg.get("method", "")

                if method == "Page.screencastFrame":
                    params = msg.get("params", {})
                    data_b64 = params.get("data", "")
                    session_id_from_frame = params.get("sessionId", 0)

                    # Ack the frame so Chrome sends the next one
                    await ws.send(json.dumps({
                        "id": msg_id,
                        "method": "Page.screencastFrameAck",
                        "params": {"sessionId": session_id_from_frame}
                    }))
                    msg_id += 1

                    # Decode and write
                    import base64
                    frame_bytes = base64.b64decode(data_b64)
                    with state_lock:
                        _write_frame(frame_bytes)

                    # Check for session switch (another Claude tab became active)
                    tabs = await get_claude_tabs()
                    if tabs:
                        new_url = tabs[0].get("url", "")
                        new_sid = extract_session_id_from_url(new_url)
                        if new_sid and new_sid != session_id:
                            print(f"[cdp-rec] tab switch detected: {session_id[:8]} → {new_sid[:8]}")
                            # Stop this screencast, outer loop will pick up new tab
                            await ws.send(json.dumps({
                                "id": msg_id,
                                "method": "Page.stopScreencast"
                            }))
                            _stop_ffmpeg()
                            is_capturing = False
                            return

        except websockets.exceptions.ConnectionClosed:
            print(f"[cdp-rec] websocket closed for {session_id[:8]}")
            _stop_ffmpeg()
            is_capturing = False


# ── API ENDPOINTS ──────────────────────────────────────────────────────────
@app.post("/record/heartbeat")
async def heartbeat(body: dict = None):
    """MemBrain compatibility — accepts heartbeats for metadata updates."""
    body = body or {}
    sid = (body.get("session_id") or "").strip()
    title = (body.get("title") or "").strip()
    if sid and title:
        with state_lock:
            if sid not in session_metadata:
                project = classify_project(title)
                session_metadata[sid] = {"title": title, "project": project, "first_seen": datetime.now().isoformat()}
                _catalog_update(sid, title, project)
            elif title != session_metadata[sid].get("title"):
                session_metadata[sid]["title"] = title
                session_metadata[sid]["project"] = classify_project(title)
    return JSONResponse({
        "status": "heartbeat_ok",
        "work_session_id": work_session_id,
        "current_session_id": current_session_id,
        "is_capturing": is_capturing,
        "frame_count": frame_count,
    })

@app.post("/record/pause")
async def pause():
    return JSONResponse({"status": "noop_cdp_handles_automatically"})

@app.post("/record/resume")
async def resume():
    return JSONResponse({"status": "noop_cdp_handles_automatically"})

@app.post("/record/stop")
async def stop():
    with state_lock:
        if work_session_id:
            _end_work_session("manual_stop")
    return JSONResponse({"status": "stopped"})

@app.get("/status")
@app.get("/record/status")
def status():
    idle_sec = round(time.time() - last_frame_at) if last_frame_at else None
    return JSONResponse({
        "service": "cdp-tab-recorder",
        "version": "1.0.0",
        "work_session_id": work_session_id,
        "current_session_id": current_session_id,
        "is_capturing": is_capturing,
        "frame_count": frame_count,
        "idle_seconds": idle_sec,
        "sessions_tracked": {sid: m.get("project", "?") for sid, m in session_metadata.items()},
        "segment_count": segment_counter,
    })

@app.get("/catalog")
def get_catalog():
    return JSONResponse(_load_catalog())

@app.get("/health")
def health():
    return {"status": "ok", "service": "cdp-tab-recorder", "cdp_port": CDP_PORT}


# ── STARTUP ────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    asyncio.create_task(screencast_loop())
    print(f"[cdp-rec] screencast loop started, watching CDP on port {CDP_PORT}")

if __name__ == "__main__":
    print(f"[cdp-rec] Starting CDP Tab Recorder on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
