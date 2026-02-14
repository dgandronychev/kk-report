# app/utils/max_api.py
from __future__ import annotations

import requests
import logging
import time
from typing import Optional
from app.config import API_BASE, HEADERS

logger = logging.getLogger(__name__)

def get_updates(marker: Optional[int] = None) -> dict:
    params = {}
    if marker is not None:
        params["marker"] = marker

    r = requests.get(
        f"{API_BASE}/updates",
        headers=HEADERS,
        params=params,
        timeout=60,
    )
    if not r.ok:
        logger.error(
            "[MAX API] get_updates failed | status=%s | response=%s",
            r.status_code,
            r.text[:1000],
        )

    r.raise_for_status()
    return r.json()


def send_text(chat_id: int, text: str) -> None:
    r = requests.post(
        f"{API_BASE}/messages",
        headers={**HEADERS, "Content-Type": "application/json"},
        params={"chat_id": chat_id},
        json={"text": text},
        timeout=30,
    )
    if not r.ok:
        logger.error(
            "[MAX API] send_text failed | status=%s | response=%s",
            r.status_code,
            r.text[:1000],
        )

    r.raise_for_status()
    return r.json()


def upload_image_to_max(file_bytes: bytes, filename: str = "image.jpg") -> dict:
    r = requests.post(
        f"{API_BASE}/uploads",
        headers=HEADERS,
        params={"type": "image"},
        timeout=30,
    )
    r.raise_for_status()
    upload_url = r.json()["url"]

    files = {"data": (filename, file_bytes, "application/octet-stream")}
    r2 = requests.post(upload_url, files=files, timeout=120)
    r2.raise_for_status()
    return r2.json()


def send_image(chat_id: int, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> None:
    image_payload = upload_image_to_max(file_bytes, filename)

    payload: dict = {
        "attachments": [{"type": "image", "payload": image_payload}],
    }
    if caption:
        payload["text"] = caption

    last_exc: Optional[Exception] = None
    for attempt in range(5):
        try:
            r = requests.post(
                f"{API_BASE}/messages",
                headers={**HEADERS, "Content-Type": "application/json"},
                params={"chat_id": chat_id},
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            return
        except Exception as e:
            last_exc = e
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Failed to send image after retries: {last_exc}")
