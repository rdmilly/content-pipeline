# Content Pipeline

Fully automated content creation pipeline that records Claude AI development sessions and produces titled, metadata-embedded video logs per conversation.

## Architecture

```
Claude tab active → MemBrain heartbeat (5s interval)
↓
Recorder (port 8765) → FFmpeg screen capture, per-session segments
↓
Tab switch → new segment tagged by session_id + conversation title
↓
Work session ends (10 min idle) → per-conversation master video stitch
↓
Orchestrator (port 8766) receives video + session context
↓
Helix conversation pull + faster-whisper transcription
↓
Claude API → 1 long-form + 3 short-form scripts
↓
ElevenLabs voiceover + HeyGen avatar render
↓
Postiz → TikTok/LinkedIn/X
```

## Components

### Recorder (`recorder/main.py`) — v2.1.0
- Per-session segment tracking: each Claude conversation gets its own video
- Master video accumulation: segments stitch into one growing video per conversation
- Project classifier: 14 regex patterns auto-tag sessions
- Persistent session catalog: `session-catalog.json`
- Metadata embedding: title, project, session_id baked into MKV container
- Orchestrator handoff: POSTs each video to orchestrator with context

### Overlay (`recorder/overlay.py`)
- Always-on-top tkinter widget showing recording state
- 1-second polling, draggable, right-click to close
- Shows: REC/PAUSED/IDLE, project name, conversation title, segment count

### Orchestrator (`orchestrator/main.py`) — port 8766
- Receives video POSTs from recorder
- Pulls Helix conversation data for script context
- Generates scripts via Claude API (OpenRouter)
- ElevenLabs voiceover (v3 tuning)
- HeyGen avatar render
- Postiz publish queue

### Transcriber (`transcriber/main.py`) — port 8767
- faster-whisper model for audio transcription
- Called by orchestrator when video has audio stream

## Setup on Windows (Clair)

### Prerequisites
- Python 3.13+
- FFmpeg (via winget: `winget install ffmpeg`)
- faster-whisper (`pip install faster-whisper`)
- Dependencies: `pip install fastapi uvicorn httpx`

### Installation
```bash
# Clone to C:\Projects\Millyweb\content-pipeline\
git clone https://github.com/rdmilly/content-pipeline.git C:\Projects\Millyweb\content-pipeline

# Create output directories
mkdir C:\Projects\Millyweb\content-pipeline\outputs\recordings
mkdir C:\Projects\Millyweb\content-pipeline\outputs\transcripts
mkdir C:\Projects\Millyweb\content-pipeline\outputs\scripts
mkdir C:\Projects\Millyweb\content-pipeline\outputs\videos
```

### Scheduled Tasks
```
MillyRecorder     → wscript.exe start-recorder-silent.vbs (ONLOGON)
MillyRecorderOverlay → pythonw.exe recorder/overlay.py (ONLOGON)
MillyPipeline     → start-pipeline.bat (ONLOGON, all 3 services)
```

### MemBrain Extension Integration
The recorder receives heartbeats from the MemBrain Chrome extension:
- `bridge.js` extracts conversation title from `document.title`
- `service-worker.js` has a 5-second periodic heartbeat that queries the active tab
- Heartbeats POST to `localhost:8765/record/heartbeat` with `{session_id, title}`

## Configuration

### Environment Variables
- `AUDIO_DEVICE` — FFmpeg audio device name (optional, screen-only by default)

### Secrets (via Infisical)
- `Elevenlabs-Api-Key`, `Elevenlabs-Voice-Ashley`
- `Heygen-API-Key`, `Heygen-ID-Ryan`
- `OPENROUTER_API_KEY` (for Claude API script generation)

## API Endpoints

### Recorder (port 8765)
- `POST /record/heartbeat` — session keepalive with `{session_id, title}`
- `POST /record/pause` — debounced pause (300ms)
- `POST /record/resume` — resume or cancel pending pause
- `POST /record/stop` — manual stop + stitch + orchestrator handoff
- `GET /record/status` — current state, sessions tracked, segments
- `GET /catalog` — persistent session catalog
- `GET /health`

## Output Structure
```
outputs/
├── recordings/
│   ├── master_{project}_{title}_{session_id}.mkv  ← one per conversation
│   └── work_*_combined.mkv                       ← legacy pre-v2.1
├── session-catalog.json                         ← persistent conversation index
├── transcripts/
├── scripts/
└── videos/
```
