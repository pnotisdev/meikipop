"""Standalone Anki mining helper used by the local mining bridge (browser extension).

Deliberately independent from src/gui/popup.py's Alt+A flow (which handles live
mss-screenshot cropping tied to the Qt/OCR pipeline) so browser-sourced mining
(subtitle text + video frame + tab-audio clip) can be added without touching that
code path or risking regressions in it.
"""
import base64
import datetime
import logging
from typing import Optional, List

from src.config.config import config
from src.utils.anki import AnkiConnect

logger = logging.getLogger(__name__)


def _resolve_field(model_fields: List[str], configured_name: str, heuristic_names: List[str]) -> Optional[str]:
    if configured_name:
        match = next((f for f in model_fields if f.lower() == configured_name.lower()), None)
        if match:
            return match
    return next((f for f in model_fields if f.lower() in heuristic_names), None)


def add_mined_note(word: str, reading: str, meaning: str, sentence: str,
                    image_bytes: Optional[bytes] = None, image_ext: str = "png",
                    audio_bytes: Optional[bytes] = None, audio_ext: str = "webm",
                    source_tag: str = "browser-mining") -> dict:
    """Adds an Anki note from mined browser data. Returns {"ok": bool, ...}."""
    anki = AnkiConnect(config.anki_url)
    if not anki.is_connected():
        return {"ok": False, "error": "AnkiConnect is not reachable. Is Anki running?"}

    deck_name = config.anki_deck_name
    model_name = config.anki_model_name

    model_fields = anki.get_model_field_names(model_name)
    if not model_fields:
        return {"ok": False, "error": f"Could not read fields for Anki model '{model_name}'."}

    target_word = _resolve_field(model_fields, config.anki_field_expression,
                                  ["front", "word", "expression", "vocab", "kanji", "selectiontext"])
    target_reading = _resolve_field(model_fields, config.anki_field_reading,
                                     ["reading", "kana", "furigana", "expressionreading"])
    target_meaning = _resolve_field(model_fields, config.anki_field_glossary,
                                     ["meaning", "glossary", "definition", "english", "maindefinition"])
    target_sentence = _resolve_field(model_fields, config.anki_field_sentence,
                                      ["sentence", "context", "example"])
    target_picture = _resolve_field(model_fields, config.anki_field_picture,
                                     ["picture", "image", "screenshot", "definitionpicture"])
    target_sentence_audio = _resolve_field(model_fields, config.anki_field_sentence_audio,
                                            ["sentenceaudio", "audio-media"])
    target_back = next((f for f in model_fields if f.lower() == "back"), None)

    fields = {}
    if target_word:
        fields[target_word] = word or sentence
    if target_reading:
        fields[target_reading] = reading
    if target_meaning:
        fields[target_meaning] = meaning
    if target_sentence:
        fields[target_sentence] = sentence

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    if image_bytes and target_picture:
        image_filename = f"meikipop-mined-{timestamp}.{image_ext}"
        anki.store_media_file(image_filename, base64.b64encode(image_bytes).decode())
        fields[target_picture] = f'<img src="{image_filename}">'

    if audio_bytes and target_sentence_audio:
        audio_filename = f"meikipop-mined-audio-{timestamp}.{audio_ext}"
        anki.store_media_file(audio_filename, base64.b64encode(audio_bytes).decode())
        fields[target_sentence_audio] = f"[sound:{audio_filename}]"

    if target_back and not (target_meaning or target_sentence or target_picture):
        content = [p for p in [reading, meaning, sentence] if p]
        fields[target_back] = "<br>".join(content)

    if not fields:
        fallback_field = model_fields[0]
        fields[fallback_field] = word or reading or meaning or sentence or "(empty)"

    result = anki.add_note(deck_name, model_name, fields, tags=["meikipop", source_tag])
    if result:
        return {"ok": True, "note_id": result}
    return {"ok": False, "error": "AnkiConnect rejected the note (it may already exist)."}
