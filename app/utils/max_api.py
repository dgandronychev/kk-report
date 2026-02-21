from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import httpx

from app.config import API_BASE, HEADERS

logger = logging.getLogger(__name__)


def _run_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("Cannot run sync MAX API call inside running event loop")


async def get_updates(marker: Optional[int] = None) -> dict:
    params = {}
    if marker is not None:
        params["marker"] = marker

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{API_BASE}/updates",
            headers=HEADERS,
            params=params,
        )

    if not r.is_success:
        logger.error(
            "[MAX API] get_updates failed | status=%s | response=%s",
            r.status_code,
            r.text[:1000],
        )

    r.raise_for_status()
    return r.json()


async def send_text(chat_id: int, text: str) -> None:
    await send_message(chat_id=chat_id, text=text)


async def send_text_with_reply_buttons(
    chat_id: int,
    text: str,
    button_texts: list[str],
    button_payloads: Optional[list[str]] = None,
) -> Optional[dict]:
    if button_payloads is not None and len(button_payloads) != len(button_texts):
        raise ValueError("button_payloads length should match button_texts length")

    callback_rows = []
    for idx, button_text in enumerate(button_texts):
        text_value = str(button_text).strip()
        payload_raw = button_payloads[idx] if button_payloads else button_text
        payload_value = str(payload_raw).strip()
        if not text_value or not payload_value:
            logger.warning(
                "[MAX API] skip empty inline button | text=%r | payload=%r",
                button_text,
                payload_raw,
            )
            continue
        callback_rows.append(
            [
                {
                    "type": "callback",
                    "text": button_text,
                    "payload": {"command": payload_value},
                }
            ]
        )
    if not callback_rows:
        logger.warning("[MAX API] inline keyboard has no valid buttons, sending plain text")
        await send_text(chat_id=chat_id, text=text)
        return None
    try:
        return await send_message(
            chat_id=chat_id,
            text=text,
            extra_payload={
                "attachments": [
                    {
                        "type": "inline_keyboard",
                        "payload": {"buttons": callback_rows},
                    }
                ]
            },
        )
    except Exception:
        logger.exception("[MAX API] failed to send inline keyboard")
        await send_text(chat_id=chat_id, text=text)
        return None

async def send_message(
    chat_id: int,
    text: Optional[str] = None,
    attachments: Optional[list[dict]] = None,
    link: Optional[dict[str, Any]] = None,
    extra_payload: Optional[dict[str, Any]] = None,
) -> dict:
    payload: dict[str, Any] = {}
    if text:
        payload["text"] = text
    if attachments:
        payload["attachments"] = attachments
    if link:
        payload["link"] = link
    if extra_payload:
        payload.update(extra_payload)

    if not payload:
        raise ValueError("send_message requires payload")

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{API_BASE}/messages",
            headers={**HEADERS, "Content-Type": "application/json"},
            params={"chat_id": chat_id},
            json=payload,
        )

    if not r.is_success:
        logger.error(
            "[MAX API] send_message failed | status=%s | response=%s | payload=%s",
            r.status_code,
            r.text[:1000],
            payload,
        )

    r.raise_for_status()
    return r.json()

def _find_message_id(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        for key in ("message_id", "mid", "id"):
            v = value.get(key)
            if v is not None:
                return str(v)
        for nested in value.values():
            found = _find_message_id(nested)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_message_id(item)
            if found:
                return found
    return None

def extract_message_id(payload: Optional[dict]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    return _find_message_id(payload)


async def delete_message(chat_id: int, message_id: str | int) -> bool:
    mid = str(message_id)
    candidates = [
        (f"{API_BASE}/messages/{mid}", {"chat_id": chat_id}),
        (f"{API_BASE}/messages", {"chat_id": chat_id, "message_id": mid}),
        (f"{API_BASE}/messages", {"chat_id": chat_id, "mid": mid}),
    ]

    async with httpx.AsyncClient(timeout=20) as client:
        for url, params in candidates:
            try:
                r = await client.delete(url, headers=HEADERS, params=params)
                if r.is_success:
                    return True
            except Exception:
                logger.exception("[MAX API] delete message failed | url=%s", url)

    logger.warning("[MAX API] delete message failed | chat_id=%s | message_id=%s", chat_id, mid)
    return False

async def upload_image_to_max(file_bytes: bytes, filename: str = "image.jpg") -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{API_BASE}/uploads",
            headers=HEADERS,
            params={"type": "image"},
        )
    r.raise_for_status()
    upload_url = r.json()["url"]

    files = {"data": (filename, file_bytes, "application/octet-stream")}
    async with httpx.AsyncClient(timeout=120) as client:
        r2 = await client.post(upload_url, files=files)
    r2.raise_for_status()
    return r2.json()


async def send_image(chat_id: int, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> None:
    image_payload = await upload_image_to_max(file_bytes, filename)

    payload: dict = {
        "attachments": [{"type": "image", "payload": image_payload}],
    }
    if caption:
        payload["text"] = caption

    last_exc: Optional[Exception] = None
    for attempt in range(5):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    f"{API_BASE}/messages",
                    headers={**HEADERS, "Content-Type": "application/json"},
                    params={"chat_id": chat_id},
                    json=payload,
                )
            r.raise_for_status()
            return
        except Exception as e:
            last_exc = e
            await asyncio.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Failed to send image after retries: {last_exc}")


def send_text_sync(chat_id: int, text: str) -> None:
    _run_sync(send_text(chat_id=chat_id, text=text))


def send_message_sync(
    chat_id: int,
    text: Optional[str] = None,
    attachments: Optional[list[dict]] = None,
    link: Optional[dict[str, Any]] = None,
    extra_payload: Optional[dict[str, Any]] = None,
) -> dict:
    return _run_sync(
        send_message(
            chat_id=chat_id,
            text=text,
            attachments=attachments,
            link=link,
            extra_payload=extra_payload,
        )
    )


def get_updates_sync(marker: Optional[int] = None) -> dict:
    return _run_sync(get_updates(marker=marker))
