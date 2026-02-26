#!/usr/bin/env python3
"""
CHIP.STREAM - Chiptune Streaming Service Backend
Run: python3 chiptune_server.py
Then open: http://localhost:8080
"""

import os
import json
import uuid
import random
import mimetypes
import threading
from pathlib import Path
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote

# ── Config ────────────────────────────────────────────────────────────────────
PORT = 8080
UPLOAD_DIR = Path("uploads")
DB_FILE    = Path("songs.json")
CHUNK_SIZE = 65536   # 64 KB streaming chunks

ALLOWED_EXTS = {
    ".mp3", ".ogg", ".wav", ".flac", ".mod", ".xm", ".s3m",
    ".it", ".nsf", ".spc", ".gbs", ".vgm", ".vgz",".fc",
}

UPLOAD_DIR.mkdir(exist_ok=True)

# ── Simple JSON "database" ─────────────────────────────────────────────────────
db_lock = threading.Lock()

def load_db() -> list:
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []

def save_db(songs: list):
    DB_FILE.write_text(json.dumps(songs, ensure_ascii=False, indent=2), encoding="utf-8")

def get_songs():
    with db_lock:
        return load_db()

def add_song(meta: dict):
    with db_lock:
        songs = load_db()
        songs.append(meta)
        save_db(songs)

def delete_song_by_id(song_id: str) -> bool:
    with db_lock:
        songs = load_db()
        new = [s for s in songs if s["id"] != song_id]
        if len(new) == len(songs):
            return False
        save_db(new)
    return True

# ── HTTP Handler ──────────────────────────────────────────────────────────────
class ChipHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    # ── helpers ──────────────────────────────────────────────────────────────

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, msg, status=400):
        self.send_json({"error": msg}, status)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    # ── routing ──────────────────────────────────────────────────────────────

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        qs     = parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self._serve_file(Path("index.html"), "text/html")
        elif path == "/api/songs":
            self._api_songs(qs)
        elif path == "/api/random":
            self._api_random(qs)
        elif path.startswith("/stream/"):
            self._stream(path[8:])
        else:
            self.send_error_json("Not found", 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")

        if path == "/api/upload":
            self._api_upload()
        else:
            self.send_error_json("Not found", 404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")

        if path.startswith("/api/songs/"):
            song_id = path.split("/")[-1]
            self._api_delete(song_id)
        else:
            self.send_error_json("Not found", 404)

    # ── endpoints ────────────────────────────────────────────────────────────

    def _api_songs(self, qs):
        songs = get_songs()
        # Optional search
        q = (qs.get("q", [""])[0]).lower()
        if q:
            songs = [s for s in songs if q in s.get("title","").lower()
                     or q in s.get("artist","").lower()]
        self.send_json(songs)

    def _api_random(self, qs):
        exclude = qs.get("exclude", [""])[0]
        songs   = get_songs()
        pool    = [s for s in songs if s["id"] != exclude] or songs
        if not pool:
            self.send_error_json("No songs available", 404)
            return
        self.send_json(random.choice(pool))

    def _api_upload(self):
        ct = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ct:
            self.send_error_json("Expected multipart/form-data")
            return

        # Parse boundary
        boundary = None
        for part in ct.split(";"):
            part = part.strip()
            if part.startswith("boundary="):
                boundary = part[9:].strip('"').encode()
                break
        if not boundary:
            self.send_error_json("Missing boundary")
            return

        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)

        fields, file_data, filename = self._parse_multipart(body, boundary)

        title  = fields.get("title", "").strip() or "Unknown Title"
        artist = fields.get("artist", "").strip() or "Unknown Artist"
        comment= fields.get("comment", "").strip()

        if not file_data or not filename:
            self.send_error_json("No file uploaded")
            return

        ext = Path(filename).suffix.lower()
        if ext not in ALLOWED_EXTS:
            self.send_error_json(f"Unsupported format: {ext}")
            return

        song_id   = str(uuid.uuid4())
        save_name = f"{song_id}{ext}"
        (UPLOAD_DIR / save_name).write_bytes(file_data)

        meta = {
            "id":       song_id,
            "title":    title,
            "artist":   artist,
            "comment":  comment,
            "filename": save_name,
            "ext":      ext,
            "size":     len(file_data),
            "uploaded": datetime.utcnow().isoformat() + "Z",
        }
        add_song(meta)
        self.send_json({"ok": True, "song": meta}, 201)

    def _api_delete(self, song_id):
        songs = get_songs()
        target = next((s for s in songs if s["id"] == song_id), None)
        if not target:
            self.send_error_json("Not found", 404)
            return
        delete_song_by_id(song_id)
        try:
            (UPLOAD_DIR / target["filename"]).unlink(missing_ok=True)
        except Exception:
            pass
        self.send_json({"ok": True})
    def _stream_fc_as_wav(self, fpath):
        with open(fpath, "rb") as f:
            header = f.read(6)

            sample_rate = int.from_bytes(header[0:4], "little")
            channels    = int.from_bytes(header[4:6], "little")
            bit_depth   = 16

            pcm_size = os.path.getsize(fpath) - 6

            import struct

            byte_rate = sample_rate * channels * bit_depth // 8
            block_align = channels * bit_depth // 8

            wav_header = struct.pack(
            "<4sI4s4sIHHIIHH4sI",
            b"RIFF",
            36 + pcm_size,
            b"WAVE",
            b"fmt ",
            16,
            1,
            channels,
            sample_rate,
            byte_rate,
            block_align,
            bit_depth,
            b"data",
            pcm_size
            )

            self.send_response(200)
            self.send_header("Content-Type", "audio/wav")
            self.end_headers()

            self.wfile.write(wav_header)

            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                self.wfile.write(chunk)
    def _stream(self, raw_id):
        """Range-aware streaming"""
        song_id = unquote(raw_id)
        songs   = get_songs()
        target  = next((s for s in songs if s["id"] == song_id), None)
        if not target:
            self.send_error_json("Not found", 404)
            return

        fpath = UPLOAD_DIR / target["filename"]
        if not fpath.exists():
            self.send_error_json("File missing", 404)
            return
        if target["ext"] == ".fc":
            self._stream_fc_as_wav(fpath)
            return
        file_size = fpath.stat().st_size
        mime_type = mimetypes.guess_type(str(fpath))[0] or "application/octet-stream"

        # Range header support
        range_header = self.headers.get("Range")
        start, end = 0, file_size - 1

        if range_header and range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            try:
                start = int(parts[0]) if parts[0] else 0
                end   = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
            except ValueError:
                pass

        length = end - start + 1
        status = 206 if range_header else 200

        self.send_response(status)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "no-cache")
        self._cors()
        self.end_headers()

        with open(fpath, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except BrokenPipeError:
                    break
                remaining -= len(chunk)

    def _serve_file(self, fpath: Path, mime: str):
        if not fpath.exists():
            self.send_error_json("index.html not found - put it next to this script", 404)
            return
        data = fpath.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ── multipart parser ─────────────────────────────────────────────────────

    def _parse_multipart(self, body: bytes, boundary: bytes):
        fields = {}
        file_data = None
        filename  = None

        delimiter = b"--" + boundary
        parts = body.split(delimiter)

        for part in parts[1:]:
            if part in (b"", b"--", b"--\r\n"):
                continue
            if part.startswith(b"\r\n"):
                part = part[2:]
            if b"\r\n\r\n" not in part:
                continue

            headers_raw, _, content = part.partition(b"\r\n\r\n")
            # Strip trailing --\r\n or \r\n
            content = content.rstrip(b"\r\n").rstrip(b"--").rstrip(b"\r\n")

            headers_text = headers_raw.decode(errors="replace")
            disp = ""
            for line in headers_text.splitlines():
                if line.lower().startswith("content-disposition"):
                    disp = line
                    break

            # Parse name
            name = ""
            fn   = None
            for seg in disp.split(";"):
                seg = seg.strip()
                if seg.startswith("name="):
                    name = seg[5:].strip('"')
                elif seg.startswith("filename="):
                    fn   = seg[9:].strip('"')

            if fn:
                file_data = content
                filename  = fn
            elif name:
                fields[name] = content.decode(errors="replace")

        return fields, file_data, filename


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════╗
║   CHIP.STREAM  –  Chiptune Streaming     ║
║   http://localhost:{PORT}                   ║
╚══════════════════════════════════════════╝
Uploads : {UPLOAD_DIR.resolve()}
DB      : {DB_FILE.resolve()}
""")
    server = HTTPServer(("0.0.0.0", PORT), ChipHandler)
    server.serve_forever()
