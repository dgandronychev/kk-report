# app/utils/helper.py
from __future__ import annotations

import re
from typing import Optional

import httpx
import requests

from app.config import URL_REGISTRASHION


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
