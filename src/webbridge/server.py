"""Local mining bridge for the companion browser extension (groundwork).

Lets a browser extension (content script on YouTube/Netflix/any <video> page) submit
subtitle text + a video-frame screenshot + a short tab-audio clip so meikipop can look
the text up in its own dictionary and add an Anki card the same way Alt+A does -
without needing OCR, since the extension reads the caption text directly from the DOM.

Security:
- Binds to 127.0.0.1 ONLY. Never exposed on the network.
- Every request must include the per-install pairing token (config.bridge_token) via
  an `Authorization: Bearer <token>` header or a `token` field in the JSON body.
  Compared with hmac.compare_digest to avoid timing attacks.
- CORS is only ever granted to `chrome-extension://` / `moz-extension://` origins,
  so a malicious webpage's JS cannot call this server even though it's local.
- Request bodies are capped (MAX_BODY_SIZE) to avoid memory-exhaustion from huge
  base64 image/audio payloads.
"""
from __future__ import annotations  # defers annotation evaluation so `web.Request` etc. below don't
                                     # blow up at class-definition time when aiohttp isn't installed

import asyncio
import base64
import hmac
import logging
import threading
from typing import Optional

from src.config.config import config
from src.utils.mining import add_mined_note

logger = logging.getLogger(__name__)

try:
    from aiohttp import web
    AIOHTTP_AVAILABLE = True
except ImportError:
    web = None
    AIOHTTP_AVAILABLE = False

MAX_BODY_SIZE = 15 * 1024 * 1024  # 15MB cap for image+audio payloads
ALLOWED_ORIGIN_PREFIXES = ("chrome-extension://", "moz-extension://")


def _cors_headers(request: web.Request) -> dict:
    origin = request.headers.get("Origin", "")
    if origin.startswith(ALLOWED_ORIGIN_PREFIXES):
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        }
    return {}


def _check_token(request: web.Request, body: Optional[dict]) -> bool:
    if not config.bridge_token:
        return False
    provided = ""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        provided = auth_header[len("Bearer "):]
    elif isinstance(body, dict):
        provided = body.get("token", "") or ""
    return hmac.compare_digest(provided, config.bridge_token)


def _decode_data_url(data_url: str) -> bytes:
    """Decodes a `data:<mime>;base64,<data>` string, or a raw base64 string as a fallback."""
    _, _, data = data_url.partition(",")
    return base64.b64decode(data or data_url)


class BridgeServer(threading.Thread):
    def __init__(self, lookup):
        super().__init__(daemon=True, name="BridgeServer")
        self.lookup = lookup
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._runner: Optional[web.AppRunner] = None
        self._ready = threading.Event()

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._start())
            self._ready.set()
            self._loop.run_forever()
        except Exception:
            logger.exception("Mining bridge server crashed")
        finally:
            try:
                self._loop.run_until_complete(self._cleanup())
            except Exception:
                pass
            self._loop.close()

    async def _start(self):
        app = web.Application(client_max_size=MAX_BODY_SIZE)
        app.add_routes([
            web.get("/health", self.handle_health),
            web.post("/lookup", self.handle_lookup),
            web.options("/lookup", self.handle_options),
            web.post("/mine", self.handle_mine),
            web.options("/mine", self.handle_options),
        ])
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", config.bridge_port)
        await site.start()
        logger.info(f"Mining bridge listening on http://127.0.0.1:{config.bridge_port} (loopback only)")

    async def _cleanup(self):
        if self._runner:
            await self._runner.cleanup()

    def stop(self):
        loop = self._loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

    async def handle_options(self, request: web.Request):
        return web.Response(status=204, headers=_cors_headers(request))

    async def handle_health(self, request: web.Request):
        return web.json_response({"status": "ok", "app": "meikipop"}, headers=_cors_headers(request))

    async def handle_lookup(self, request: web.Request):
        headers = _cors_headers(request)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON body"}, status=400, headers=headers)

        if not _check_token(request, body):
            return web.json_response({"error": "unauthorized"}, status=401, headers=headers)

        text = (body.get("text") or "").strip()
        if not text:
            return web.json_response({"error": "missing 'text'"}, status=400, headers=headers)

        try:
            entries = self.lookup.lookup(text)
        except Exception as e:
            logger.exception("Bridge lookup failed")
            return web.json_response({"error": str(e)}, status=500, headers=headers)

        results = [{
            "written_form": e.written_form,
            "reading": e.reading,
            "senses": e.senses,
            "pitch_accents": e.pitch_accents,
        } for e in entries]
        return web.json_response({"results": results}, headers=headers)

    async def handle_mine(self, request: web.Request):
        headers = _cors_headers(request)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON body"}, status=400, headers=headers)

        if not _check_token(request, body):
            return web.json_response({"error": "unauthorized"}, status=401, headers=headers)

        text = (body.get("text") or "").strip()
        if not text:
            return web.json_response({"error": "missing 'text'"}, status=400, headers=headers)

        word, reading, meaning = "", "", ""
        try:
            entries = self.lookup.lookup(text)
            if entries:
                entry = entries[0]
                word = entry.written_form
                reading = entry.reading
                meaning = "<br>".join("; ".join(s.get('glosses', [])) for s in entry.senses)
        except Exception:
            logger.exception("Bridge mine: dictionary lookup failed; continuing without it")

        image_bytes, image_ext = None, "png"
        image_data_url = body.get("imageBase64")
        if image_data_url:
            try:
                image_bytes = _decode_data_url(image_data_url)
                if "jpeg" in image_data_url[:32] or "jpg" in image_data_url[:32]:
                    image_ext = "jpg"
            except Exception:
                logger.warning("Failed to decode imageBase64 from mining request")

        audio_bytes, audio_ext = None, "webm"
        audio_data_url = body.get("audioBase64")
        if audio_data_url:
            try:
                audio_bytes = _decode_data_url(audio_data_url)
                header = audio_data_url[:32]
                if "ogg" in header:
                    audio_ext = "ogg"
                elif "mp3" in header or "mpeg" in header:
                    audio_ext = "mp3"
            except Exception:
                logger.warning("Failed to decode audioBase64 from mining request")

        # Fallback for browsers that can't supply a tab-audio clip (Firefox has no
        # extension-facing API for tab/system audio capture at all - not just
        # chrome.tabCapture, but getDisplayMedia's audio option is also unimplemented
        # there). meikipop's own OS-level loopback recorder is browser-independent, so
        # if the user has enabled it (Settings -> Sentence Audio Capture), use it here
        # instead of leaving the sentence-audio field empty.
        if audio_bytes is None and config.enable_sentence_audio_capture:
            try:
                from src.utils.audio_recorder import audio_recorder
                clip = audio_recorder.get_clip_wav_bytes(config.sentence_audio_duration_seconds)
                if clip:
                    audio_bytes, audio_ext = clip, "wav"
                    logger.info("Bridge mine: browser supplied no audio clip; used meikipop's loopback recorder instead.")
            except Exception:
                logger.exception("Bridge mine: loopback audio fallback failed; continuing without audio")

        result = add_mined_note(
            word=word, reading=reading, meaning=meaning, sentence=text,
            image_bytes=image_bytes, image_ext=image_ext,
            audio_bytes=audio_bytes, audio_ext=audio_ext,
            source_tag="browser-mining",
        )
        status = 200 if result.get("ok") else 502
        return web.json_response(result, status=status, headers=headers)


_current: Optional[BridgeServer] = None
_lock = threading.Lock()


def start_bridge(lookup):
    """Starts the bridge server (idempotent; safe to call even if already running)."""
    if not AIOHTTP_AVAILABLE:
        logger.warning("The 'aiohttp' package is not installed; the mining bridge is disabled. Run `pip install aiohttp` to enable it.")
        return
    global _current
    with _lock:
        if _current and _current.is_alive():
            return
        _current = BridgeServer(lookup)
        _current.start()


def stop_bridge():
    """Stops the bridge server if running."""
    global _current
    with _lock:
        if _current and _current.is_alive():
            _current.stop()
        _current = None


def is_bridge_running() -> bool:
    return bool(_current and _current.is_alive())
