# app/utils/max_api.py
from __future__ import annotations

import requests
import logging
import time
from typing import Any, Optional
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
    send_message(chat_id=chat_id, text=text)


def send_text_with_reply_buttons(chat_id: int, text: str, button_texts: list[str]) -> None:
    link_variants = [
        {
            "type": "reply",
            "buttons": [
                [{"type": "text", "text": button_text}] for button_text in button_texts
            ],
        },
        {
            "type": "reply",
            "buttons": [
                [{"type": "callback", "text": button_text, "payload": button_text}]
                for button_text in button_texts
            ],
        },
    ]

    for link in link_variants:
        try:
            send_message(chat_id=chat_id, text=text, link=link)
            return
        except Exception:
            logger.exception("[MAX API] failed to send keyboard with link=%s", link)

    send_text(chat_id=chat_id, text=text)


def send_message(
    chat_id: int,
    text: Optional[str] = None,
    attachments: Optional[list[dict]] = None,
    link: Optional[dict[str, Any]] = None,
) -> dict:
    payload: dict[str, Any] = {}
    if text:
        payload["text"] = text
    if attachments:
        payload["attachments"] = attachments
    if link:
        payload["link"] = link

    if not payload:
        raise ValueError("send_message requires text or attachments")

    r = requests.post(
        f"{API_BASE}/messages",
        headers={**HEADERS, "Content-Type": "application/json"},
        params={"chat_id": chat_id},
        json=payload,
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
