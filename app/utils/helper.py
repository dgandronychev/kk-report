# app/utils/helper.py
from __future__ import annotations

import re
from typing import Optional

import httpx
import requests

from app.config import URL_GET_FIO, URL_REGISTRASHION


def normalize_phone(s: str) -> Optional[str]:
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 11 and digits.startswith("7"):
        return digits
    return None


def _build_registration_payload(max_chat_id: int) -> dict[str, str]:
    return {
        "max_username_id": str("@test"),
        "max_chat_id": str(max_chat_id),
    }


def _extract_registration_error(status_code: int, body: str, json_data: object) -> str:
    if isinstance(json_data, dict):
        result = json_data.get("result")
        if result:
            return str(result)
        return str(json_data)

    return (body or "")[:300] or f"HTTP {status_code}"


def post_registration(
    phone: str,
    max_user_id: int,
    max_chat_id: int,
) -> Optional[str]:
    """
    Returns: None on success, else error message string.
    """
    if not URL_REGISTRASHION:
        return "URL_REGISTRASHION is not set"

    json_data = {
        "phone": phone,
        **_build_registration_payload(max_chat_id=max_chat_id),
    }

    r = requests.post(URL_REGISTRASHION, json=json_data, timeout=20)

    if r.status_code < 400:
        return None

    try:
        parsed = r.json()
    except Exception:
        parsed = None

    return _extract_registration_error(r.status_code, r.text, parsed)


async def post_registration_async(
    phone: str,
    max_user_id: int,
    max_chat_id: int,
) -> Optional[str]:
    if not URL_REGISTRASHION:
        return "URL_REGISTRASHION is not set"

    json_data = {
        "phone": phone,
        **_build_registration_payload(max_chat_id=max_chat_id),
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(URL_REGISTRASHION, json=json_data)

    if r.status_code < 400:
        return None

    try:
        parsed = r.json()
    except Exception:
        parsed = None

    return _extract_registration_error(r.status_code, r.text, parsed)

def _extract_fio_from_payload(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    user_obj = payload.get("user")
    if isinstance(user_obj, dict):
        fullname = user_obj.get("fullname")
        if isinstance(fullname, str) and fullname.strip():
            return fullname.strip()

    for key in ("fullname", "fio"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def _fallback_fio(msg: dict, user_id: int) -> str:
    sender = msg.get("sender") if isinstance(msg, dict) else None
    if isinstance(sender, dict):
        first_name = str(sender.get("first_name") or "").strip()
        last_name = str(sender.get("last_name") or "").strip()
        fio = f"{last_name} {first_name}".strip()
        if fio:
            return fio

        username = str(sender.get("username") or "").strip()
        if username:
            return f"@{username}" if not username.startswith("@") else username

    return str(user_id)


async def get_fio_async(max_chat_id: int, user_id: int, msg: Optional[dict] = None) -> str:
    if not URL_GET_FIO:
        return _fallback_fio(msg or {}, user_id)

    fallback = _fallback_fio(msg or {}, user_id)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(URL_GET_FIO, params={"chat_id": str(max_chat_id)})

        response.raise_for_status()
        parsed = response.json()

        fio = _extract_fio_from_payload(parsed)
        if fio:
            return fio
    except Exception:
        return fallback

    return fallback