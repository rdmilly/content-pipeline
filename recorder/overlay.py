# MillyRecorder Overlay v1.0 — always-on-top recording indicator
# Polls localhost:8765/record/status every 2s, shows a small pill in the corner.
# Red = recording, Yellow = paused, Gray = idle.
import tkinter as tk
import json
import urllib.request
import threading
import time

RECORDER_URL = "http://localhost:8765/record/status"
POLL_INTERVAL = 2  # seconds
OPACITY = 0.85
POSITION = "top-right"  # top-left, top-right, bottom-left, bottom-right
MARGIN = 10

class RecorderOverlay:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("MillyRecorder")
        self.root.overrideredirect(True)  # no title bar
        self.root.attributes("-topmost", True)
        self.root.attributes("-alpha", OPACITY)
        self.root.configure(bg="#1a1a2e")

        # Make window draggable
        self._drag_x = 0
        self._drag_y = 0

        # Main frame
        self.frame = tk.Frame(self.root, bg="#1a1a2e", padx=8, pady=4)
        self.frame.pack()

        # Status dot + label row
        self.top_row = tk.Frame(self.frame, bg="#1a1a2e")
        self.top_row.pack(anchor="w")

        self.dot = tk.Canvas(self.top_row, width=12, height=12, bg="#1a1a2e", highlightthickness=0)
        self.dot.pack(side="left", padx=(0, 6), pady=2)
        self.dot_id = self.dot.create_oval(2, 2, 10, 10, fill="#555555", outline="")

        self.status_label = tk.Label(self.top_row, text="IDLE", font=("Segoe UI", 9, "bold"),
                                     fg="#888888", bg="#1a1a2e")
        self.status_label.pack(side="left")

        # Project label
        self.project_label = tk.Label(self.frame, text="", font=("Segoe UI", 8),
                                      fg="#B85C38", bg="#1a1a2e", anchor="w")
        self.project_label.pack(anchor="w")

        # Title label (truncated)
        self.title_label = tk.Label(self.frame, text="", font=("Segoe UI", 7),
                                    fg="#aaaaaa", bg="#1a1a2e", anchor="w",
                                    wraplength=220)
        self.title_label.pack(anchor="w")

        # Segment count
        self.seg_label = tk.Label(self.frame, text="", font=("Segoe UI", 7),
                                  fg="#666666", bg="#1a1a2e", anchor="w")
        self.seg_label.pack(anchor="w")

        # Bind drag events
        self.frame.bind("<ButtonPress-1>", self._start_drag)
        self.frame.bind("<B1-Motion>", self._on_drag)
        self.top_row.bind("<ButtonPress-1>", self._start_drag)
        self.top_row.bind("<B1-Motion>", self._on_drag)

        # Right-click to close
        self.frame.bind("<ButtonPress-3>", lambda e: self.root.destroy())
        self.top_row.bind("<ButtonPress-3>", lambda e: self.root.destroy())

        # Position window
        self.root.update_idletasks()
        self._position_window()

        # Start polling thread
        self._running = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

        # Handle close
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _position_window(self):
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        w = 260
        h = 75
        if POSITION == "top-right":
            x = sw - w - MARGIN
            y = MARGIN + 40  # below taskbar area
        elif POSITION == "top-left":
            x = MARGIN
            y = MARGIN + 40
        elif POSITION == "bottom-right":
            x = sw - w - MARGIN
            y = sh - h - MARGIN - 50
        else:  # bottom-left
            x = MARGIN
            y = sh - h - MARGIN - 50
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    def _start_drag(self, event):
        self._drag_x = event.x
        self._drag_y = event.y

    def _on_drag(self, event):
        x = self.root.winfo_x() + event.x - self._drag_x
        y = self.root.winfo_y() + event.y - self._drag_y
        self.root.geometry(f"+{x}+{y}")

    def _fetch_status(self):
        try:
            req = urllib.request.Request(RECORDER_URL, method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                return json.loads(resp.read())
        except Exception:
            return None

    def _update_ui(self, data):
        if data is None:
            self.dot.itemconfig(self.dot_id, fill="#333333")
            self.status_label.config(text="OFFLINE", fg="#555555")
            self.project_label.config(text="recorder not running")
            self.title_label.config(text="")
            self.seg_label.config(text="")
            return

        recording = data.get("recording", False)
        paused = data.get("paused", False)
        work_id = data.get("work_session_id", "")
        sessions = data.get("sessions_tracked", {})
        seg_count = data.get("segment_count", 0)
        current_sid = data.get("current_session_id", "")[:12]

        # Determine state
        if recording:
            self.dot.itemconfig(self.dot_id, fill="#ff3333")
            self.status_label.config(text="REC", fg="#ff3333")
        elif paused:
            self.dot.itemconfig(self.dot_id, fill="#ffaa00")
            self.status_label.config(text="PAUSED", fg="#ffaa00")
        elif work_id:
            self.dot.itemconfig(self.dot_id, fill="#ffaa00")
            self.status_label.config(text="SESSION", fg="#ffaa00")
        else:
            self.dot.itemconfig(self.dot_id, fill="#555555")
            self.status_label.config(text="IDLE", fg="#888888")

        # Current session info
        if sessions and current_sid:
            # Find the current session in tracked dict
            cur_info = None
            for sid_key, info in sessions.items():
                if current_sid.startswith(sid_key) or sid_key.startswith(current_sid[:8]):
                    cur_info = info
                    break
            if cur_info and isinstance(cur_info, dict):
                project = cur_info.get("project", "unknown")
                title = cur_info.get("title", "")
                self.project_label.config(text=project.upper())
                self.title_label.config(text=title[:60] if title else "(no title yet)")
            else:
                self.project_label.config(text="")
                self.title_label.config(text="")
        elif not work_id:
            self.project_label.config(text="")
            self.title_label.config(text="waiting for Claude activity")
        else:
            self.project_label.config(text="")
            self.title_label.config(text="")

        # Segment info
        if work_id:
            n_sessions = len(sessions)
            self.seg_label.config(text=f"{seg_count} segments \u00b7 {n_sessions} sessions \u00b7 ws:{work_id}")
        else:
            self.seg_label.config(text="")

    def _poll_loop(self):
        while self._running:
            data = self._fetch_status()
            try:
                self.root.after(0, self._update_ui, data)
            except Exception:
                break
            time.sleep(POLL_INTERVAL)

    def _on_close(self):
        self._running = False
        self.root.destroy()

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    overlay = RecorderOverlay()
    overlay.run()
