"""Rolling loopback (system) audio recorder used to mine real sentence audio for Anki cards.

Unlike the word-pronunciation audio (fetched from languagepod101 or a local audio
dictionary), this captures whatever the user was actually listening to (game, video,
stream) in a small ring buffer, so a clip of the last N seconds can be attached to a
card at mining time -- similar to tools like mpvacious/asbplayer.

Requires the optional `soundcard` package. If it isn't installed, or the platform
doesn't support loopback recording, capture is simply disabled and callers get `None`
back instead of a clip (never raises).
"""
import io
import logging
import threading
import time
import wave
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import soundcard as sc
    import numpy as np
    SOUNDCARD_AVAILABLE = True
except ImportError:
    SOUNDCARD_AVAILABLE = False


class AudioRecorder(threading.Thread):
    SAMPLE_RATE = 48000
    CHANNELS = 2
    MAX_BUFFER_SECONDS = 60
    CHUNK_FRAMES = SAMPLE_RATE // 10  # ~100ms per read

    def __init__(self):
        super().__init__(daemon=True, name="AudioRecorder")
        self.available = SOUNDCARD_AVAILABLE
        self._enabled = threading.Event()
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()
        self._buffer = deque()
        self._buffer_samples = 0

    def start_capturing(self):
        """Enables capture. Starts the background thread on first use."""
        if not self.available:
            logger.warning("The 'soundcard' package is not installed; sentence-audio capture is disabled.")
            return
        if not self.is_alive():
            self.start()
        self._enabled.set()

    def stop_capturing(self):
        """Disables capture (thread stays alive, idling, so it can be re-enabled instantly)."""
        self._enabled.clear()

    def request_stop(self):
        """Fully stops the background thread (used on app shutdown)."""
        self._stop_requested.set()
        self._enabled.set()  # wake the loop so it notices the stop request

    def run(self):
        if not self.available:
            return
        while not self._stop_requested.is_set():
            if not self._enabled.is_set():
                time.sleep(0.2)
                continue
            try:
                speaker = sc.default_speaker()
                mic = sc.get_microphone(id=str(speaker.name), include_loopback=True)
                with mic.recorder(samplerate=self.SAMPLE_RATE, channels=self.CHANNELS) as rec:
                    logger.info(f"Sentence-audio capture started (loopback device: {speaker.name})")
                    while self._enabled.is_set() and not self._stop_requested.is_set():
                        data = rec.record(numframes=self.CHUNK_FRAMES)
                        self._append_chunk(data)
                logger.info("Sentence-audio capture stopped.")
            except Exception as e:
                logger.error(f"Sentence-audio capture error (will retry in 2s): {e}")
                time.sleep(2)

    def _append_chunk(self, data):
        with self._lock:
            self._buffer.append(data)
            self._buffer_samples += len(data)
            max_samples = self.SAMPLE_RATE * self.MAX_BUFFER_SECONDS
            while self._buffer_samples > max_samples and self._buffer:
                removed = self._buffer.popleft()
                self._buffer_samples -= len(removed)

    def get_clip_wav_bytes(self, duration_seconds: float) -> Optional[bytes]:
        """Returns WAV-encoded bytes of the last `duration_seconds` of captured audio, or None."""
        if not self.available:
            return None

        with self._lock:
            if not self._buffer:
                return None
            chunks = list(self._buffer)

        audio = np.concatenate(chunks, axis=0)
        num_samples = max(1, int(duration_seconds * self.SAMPLE_RATE))
        clip = audio[-num_samples:] if num_samples < len(audio) else audio

        clip = np.clip(clip, -1.0, 1.0)
        clip_int16 = (clip * 32767).astype(np.int16)

        buf = io.BytesIO()
        with wave.open(buf, 'wb') as wf:
            wf.setnchannels(self.CHANNELS)
            wf.setsampwidth(2)
            wf.setframerate(self.SAMPLE_RATE)
            wf.writeframes(clip_int16.tobytes())
        return buf.getvalue()


# Single shared instance, mirroring the `config`/`magpie_manager` singleton pattern.
audio_recorder = AudioRecorder()
