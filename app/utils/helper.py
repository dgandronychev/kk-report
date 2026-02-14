# app/utils/helper.py
from __future__ import annotations

import re
from typing import Optional

import requests

from app.config import URL_REGISTRASHION


def normalize_phone(s: str) -> Optional[str]:
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 11 and digits.startswith("7"):
        return digits
    return None


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
        "max_username_id": str("@test"),
        "max_chat_id": str(max_chat_id),
    }

    r = requests.post(URL_REGISTRASHION, json=json_data, timeout=20)

    if r.status_code < 400:
        return None

    try:
        j = r.json()
        return j.get("result") or str(j)
    except Exception:
        return (r.text or "")[:300] or f"HTTP {r.status_code}"
