from __future__ import annotations

import logging
from pathlib import Path
from threading import Lock
from typing import List, Set

logger = logging.getLogger(__name__)

_STORAGE_PATH = Path("logs") / "known_chat_ids.txt"
_LOCK = Lock()
_CACHED_CHAT_IDS: Set[int] | None = None


def _load_cache() -> Set[int]:
    global _CACHED_CHAT_IDS

    if _CACHED_CHAT_IDS is not None:
        return _CACHED_CHAT_IDS

    if not _STORAGE_PATH.exists():
        _CACHED_CHAT_IDS = set()
        return _CACHED_CHAT_IDS

    chat_ids: Set[int] = set()
    try:
        for line in _STORAGE_PATH.read_text(encoding="utf-8").splitlines():
            value = line.strip()
            if value.lstrip("-").isdigit():
                chat_ids.add(int(value))
    except Exception:
        logger.exception("Failed to load known chat ids from %s", _STORAGE_PATH)

    _CACHED_CHAT_IDS = chat_ids
    return _CACHED_CHAT_IDS


def remember_chat_id(chat_id: int) -> bool:
    """
    Запоминает chat_id в файле logs/known_chat_ids.txt.

    Returns True, если chat_id был добавлен впервые.
    """
    with _LOCK:
        cache = _load_cache()
        if chat_id in cache:
            return False

        _STORAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _STORAGE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(f"{chat_id}\n")

        cache.add(chat_id)
        logger.info("Saved new chat_id=%s to %s", chat_id, _STORAGE_PATH)
        return True


def get_known_chat_ids() -> List[int]:
    with _LOCK:
        return sorted(_load_cache())
