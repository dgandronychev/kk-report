from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import gspread
import httpx
from gspread import Client, Spreadsheet

from app.config import (
    GSPREAD_URL_ANSWER,
    HUB_REPORT_DB_PATH,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_SBORKA_BELKA,
    TELEGRAM_CHAT_ID_SBORKA_CITY,
    TELEGRAM_CHAT_ID_SBORKA_YANDEX,
    TELEGRAM_THREAD_ID_HAB_BELKA,
    TELEGRAM_THREAD_ID_HAB_CITY,
    TELEGRAM_THREAD_ID_HAB_YANDEX,
)

logger = logging.getLogger(__name__)

_TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""

_last_message_ids: dict[str, int | list[int]] = {
    "Яндекс": 0,
    "СитиДрайв": 0,
    "Белка": 0,
}

_HUB_COMPANY_TARGETS = {
    "Яндекс": (TELEGRAM_CHAT_ID_SBORKA_YANDEX, TELEGRAM_THREAD_ID_HAB_YANDEX),
    "СитиДрайв": (TELEGRAM_CHAT_ID_SBORKA_CITY, TELEGRAM_THREAD_ID_HAB_CITY),
    "Белка": (TELEGRAM_CHAT_ID_SBORKA_BELKA, TELEGRAM_THREAD_ID_HAB_BELKA),
}


def _db_path() -> Path:
    path = Path(HUB_REPORT_DB_PATH)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def init_hub_report_db() -> None:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS company_messages (
                company TEXT PRIMARY KEY CHECK (company IN ('Яндекс', 'СитиДрайв', 'Белка')),
                message_id TEXT
            )
            """
        )
        for company in _last_message_ids:
            cur.execute(
                "INSERT OR IGNORE INTO company_messages (company, message_id) VALUES (?, NULL)",
                (company,),
            )
        conn.commit()


def load_message_ids() -> dict[str, int | list[int]]:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT company, message_id FROM company_messages")
        rows = cur.fetchall()

    result: dict[str, int | list[int]] = {}
    for company, payload in rows:
        if not payload:
            result[company] = 0
            continue
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, list) and len(parsed) == 1:
                result[company] = int(parsed[0])
            elif isinstance(parsed, list):
                result[company] = [int(v) for v in parsed]
            else:
                result[company] = int(parsed)
        except Exception:
            result[company] = 0
    return result


def save_message_ids(company: str, message_ids: int | list[int]) -> None:
    if isinstance(message_ids, list):
        payload = json.dumps(message_ids, ensure_ascii=False)
    else:
        payload = json.dumps([int(message_ids)], ensure_ascii=False)
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO company_messages (company, message_id) VALUES (?, ?)",
            (company, payload),
        )
        conn.commit()


def _telegram_call(method: str, payload: dict) -> dict | None:
    if not _TELEGRAM_API_BASE:
        return None
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(f"{_TELEGRAM_API_BASE}/{method}", json=payload)
    except Exception:
        logger.exception("telegram sync call failed | method=%s", method)
        return None

    if not resp.is_success:
        logger.warning("telegram call failed | method=%s status=%s body=%s", method, resp.status_code, resp.text[:500])
        return None
    data = resp.json()
    if not data.get("ok"):
        logger.warning("telegram call not ok | method=%s body=%s", method, data)
        return None
    return data


def _send_message(chat_id: int, text: str, thread_id: int | None = None) -> int | None:
    payload = {"chat_id": chat_id, "text": text}
    if thread_id:
        payload["message_thread_id"] = thread_id
    data = _telegram_call("sendMessage", payload)
    if not isinstance(data, dict):
        return None
    result = data.get("result") or {}
    message_id = result.get("message_id")
    return int(message_id) if message_id is not None else None


def _edit_message(chat_id: int, message_id: int, text: str, thread_id: int | None = None) -> bool:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if thread_id:
        payload["message_thread_id"] = thread_id
    data = _telegram_call("editMessageText", payload)
    return isinstance(data, dict)


def _delete_message(chat_id: int, message_id: int) -> None:
    _telegram_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})


def _split_message(text: str, max_length: int = 4096) -> list[str]:
    if len(text) <= max_length:
        return [text]
    lines = text.split("\n")
    parts: list[str] = []
    chunk = ""
    for line in lines:
        candidate = f"{chunk}\n{line}" if chunk else line
        if len(candidate) > max_length and chunk:
            parts.append(chunk)
            chunk = line
        else:
            chunk = candidate
    if chunk:
        parts.append(chunk)
    return parts


def _send_or_update_long_message(company: str, chat_id: int, text: str, thread_id: int | None = None) -> None:
    parts = _split_message(text)
    old_ids = _last_message_ids.get(company) or 0
    old_list = [old_ids] if isinstance(old_ids, int) else list(old_ids)
    old_list = [int(v) for v in old_list if int(v) > 0]

    new_ids: list[int] = []
    for idx, part in enumerate(parts):
        if idx < len(old_list):
            if _edit_message(chat_id, old_list[idx], part, thread_id=thread_id):
                new_ids.append(old_list[idx])
                continue
        msg_id = _send_message(chat_id, part, thread_id=thread_id)
        if msg_id:
            new_ids.append(msg_id)

    if len(old_list) > len(parts):
        for mid in old_list[len(parts):]:
            _delete_message(chat_id, mid)

    if not new_ids:
        return

    _last_message_ids[company] = new_ids[0] if len(new_ids) == 1 else new_ids
    save_message_ids(company, _last_message_ids[company])


def _gspread_open_by_url(url: str) -> Spreadsheet:
    gc: Client = gspread.service_account("app/creds.json")
    return gc.open_by_url(url)


def print_google_data(company: str) -> None:
    sh = _gspread_open_by_url(GSPREAD_URL_ANSWER)
    ws = sh.worksheet("Онлайн остатки Хаба")
    rows = ws.get_all_values()
    if len(rows) < 2:
        return

    filtered = [r for r in rows[1:] if len(r) >= 10 and str(r[1]).strip() == company]
    groups: Dict[str, Dict[tuple[str, str, str, str, str, str], Dict[str, int]]] = {}
    for row in filtered:
        brand = str(row[2]).strip()
        key = (str(row[3]).strip(), str(row[4]).strip(), str(row[5]).strip(), str(row[6]).strip(), str(row[7]).strip(), str(row[8]).strip())
        wheel_pos = str(row[9]).strip()

        groups.setdefault(brand, {}).setdefault(key, {"Левое": 0, "Правое": 0})
        if wheel_pos in groups[brand][key]:
            groups[brand][key][wheel_pos] += 1

    lines: List[str] = []
    for brand in sorted(groups):
        lines.append(f"🚗  {brand}")
        for subgroup in sorted(groups[brand]):
            radius, size, tire_brand, tire_model, season, disk_type = subgroup
            counts = groups[brand][subgroup]
            left_count = counts.get("Левое", 0)
            right_count = counts.get("Правое", 0)

            details: list[str] = []
            kit_count = min(left_count // 2, right_count // 2)
            if kit_count:
                details.append(f"Комплект {kit_count}шт")
                left_count -= 2 * kit_count
                right_count -= 2 * kit_count

            axle_count = min(left_count, right_count)
            if axle_count:
                details.append(f"Ось {axle_count}шт")
                left_count -= axle_count
                right_count -= axle_count

            if left_count:
                details.append(f"Левое {left_count}шт")
            if right_count:
                details.append(f"Правое {right_count}шт")

            lines.append(
                f"🛞 {radius}/{size} | {tire_brand} {tire_model} | {season} | {disk_type} | "
                + " | ".join(details)
                + " |"
            )

    current_time = (datetime.now() + timedelta(hours=3)).strftime("%H:%M %d.%m.%Y")
    message_text = current_time + "\n\n" + "\n\n".join(lines)

    target = _HUB_COMPANY_TARGETS.get(company)
    if not target:
        return
    chat_id, thread_id = target
    _send_or_update_long_message(company, int(chat_id), message_text, thread_id=thread_id or None)


def refresh_hub_reports() -> None:
    for company in ("Яндекс", "СитиДрайв", "Белка"):
        print_google_data(company)


def schedule_hub_report_updates(interval_seconds: int = 300) -> None:
    def worker() -> None:
        while True:
            try:
                refresh_hub_reports()
            except Exception:
                logger.exception("hub report scheduler loop failed")
            time.sleep(interval_seconds)

    threading.Thread(target=worker, daemon=True).start()


def bootstrap_hub_report_state() -> None:
    init_hub_report_db()
    loaded = load_message_ids()
    for company in _last_message_ids:
        value = loaded.get(company)
        if value is not None:
            _last_message_ids[company] = value
