from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

def _api_base(bot_token: str | None = None) -> str:
    token = bot_token.strip()
    if not token:
        return ""
    return f"https://api.telegram.org/bot{token}"

def is_telegram_enabled(bot_token: str | None = None) -> bool:
    return bool(_api_base(bot_token))


def _iter_urls(payload: Any) -> Iterable[str]:
    if isinstance(payload, dict):
        for key in ("url", "download_url", "src", "href"):
            value = payload.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                yield value
        for value in payload.values():
            yield from _iter_urls(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _iter_urls(value)


def _first_url(payload: Any) -> Optional[str]:
    for url in _iter_urls(payload):
        return url
    return None


def _chat_link_id(chat_id: int) -> str:
    cid = str(chat_id).strip()
    if cid.startswith("-100"):
        return cid[4:]
    if cid.startswith("-"):
        return cid[1:]
    return cid


def build_message_link(chat_id: int, message_id: int | str, thread_id: int | None = None) -> str:
    base = f"https://t.me/c/{_chat_link_id(chat_id)}"
    if thread_id:
        return f"{base}/{thread_id}/{message_id}"
    return f"{base}/{message_id}"


async def _telegram_call(method: str, payload: dict, bot_token: str | None = None) -> Optional[dict]:
    api_base = _api_base(bot_token)
    if not api_base:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(f"{api_base}/{method}", json=payload)
    except Exception:
        logger.exception("telegram call failed with exception | method=%s", method)
        return None

    if not response.is_success:
        logger.warning("telegram call failed | method=%s | status=%s | body=%s", method, response.status_code, response.text[:1000])
        return None

    data = response.json()
    if not data.get("ok"):
        logger.warning("telegram call not ok | method=%s | body=%s", method, data)
        return None
    return data


async def send_text(chat_id: int, text: str, thread_id: int | None = None, bot_token: str | None = None) -> Optional[str]:
    if not is_telegram_enabled(bot_token):
        return None

    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if thread_id:
        payload["message_thread_id"] = thread_id

    data = await _telegram_call("sendMessage", payload, bot_token=bot_token)
    if not isinstance(data, dict):
        return None
    result = data.get("result")
    if not isinstance(result, dict):
        return None

    message_id = result.get("message_id")
    if message_id is None:
        return None
    return build_message_link(chat_id, message_id, thread_id=thread_id)


async def send_report(
    chat_id: int,
    text: str,
    attachments: Optional[list[dict]] = None,
    thread_id: int | None = None,
    bot_token: str | None = None,
) -> Optional[str]:
    if not is_telegram_enabled(bot_token):
        return None

    files = attachments or []
    media_items: list[dict] = []
    for item in files:
        if not isinstance(item, dict):
            continue
        f_type = str(item.get("type") or "")
        url = _first_url(item.get("payload"))
        if not url:
            continue
        if f_type == "image":
            media_items.append({"type": "photo", "media": url})
        elif f_type == "video":
            media_items.append({"type": "video", "media": url})
        else:
            media_items.append({"type": "document", "media": url})

    if media_items:
        # Telegram limits caption length for media to 1024 chars.
        # Long reports should still be delivered as plain text even when
        # media upload fails because of caption/media restrictions.
        caption_text = text if len(text) <= 1024 else text[:1021] + "..."
        media_items[0]["caption"] = caption_text
        payload: dict[str, Any] = {"chat_id": chat_id, "media": media_items}
        if thread_id:
            payload["message_thread_id"] = thread_id

        data = await _telegram_call("sendMediaGroup", payload, bot_token=bot_token)
        if not isinstance(data, dict):
            logger.warning("sendMediaGroup failed, falling back to sendMessage")
            return await send_text(chat_id=chat_id, text=text, thread_id=thread_id, bot_token=bot_token)
        result = data.get("result")
        if isinstance(result, list) and result and isinstance(result[0], dict):
            message_id = result[0].get("message_id")
            if message_id is not None:
                return build_message_link(chat_id, message_id, thread_id=thread_id)
        return await send_text(chat_id=chat_id, text=text, thread_id=thread_id, bot_token=bot_token)

    return await send_text(chat_id=chat_id, text=text, thread_id=thread_id, bot_token=bot_token)
