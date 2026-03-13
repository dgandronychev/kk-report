# app/utils/max_scheduler.py
from __future__ import annotations

import logging
import os
import threading
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import List, Optional
from collections import defaultdict
from zoneinfo import ZoneInfo

from app.utils.max_api import send_text_sync
from app.utils.gsheets import find_logistics_rows_shift, get_record_sklad
from app.config import LOGISTICS_CHAT_IDS, REPORT_CHAT_ID, TABLO_CHAT_ID

logger = logging.getLogger(__name__)

TZ_MSK = ZoneInfo("Europe/Moscow")

def _parse_int_list_from_config(value: str) -> List[int]:
    if not value:
        return []

    result: List[int] = []
    for part in value.split(","):
        p = part.strip()
        if p.lstrip("-").isdigit():
            result.append(int(p))

    return result


def _parse_int_from_config(value: str) -> Optional[int]:
    if not value:
        return None

    v = value.strip()
    if v.lstrip("-").isdigit():
        return int(v)

    return None

def _parse_int_list_env(name: str) -> List[int]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []

    result: List[int] = []
    for part in raw.split(","):
        p = part.strip()
        if p.lstrip("-").isdigit():
            result.append(int(p))

    return result


def _parse_int_env(name: str) -> Optional[int]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return None

    if raw.lstrip("-").isdigit():
        return int(raw)

    return None



@dataclass(frozen=True)
class Schedules:
    logistics_times: List[time]
    report_times: List[time]




def _build_wheels_summary_chunks() -> list[str]:
    records = get_record_sklad()
    if not records:
        return []

    grouped_records: dict[tuple[str, ...], int] = defaultdict(int)
    for record in records:
        grouped_records[tuple(str(x) for x in record)] += 1

    lines: list[str] = []
    for record, count in grouped_records.items():
        if len(record) < 11:
            continue
        sb_number = record[10]
        line = f"{sb_number} | {count} шт | {record[0]} | {record[1]} | {record[3]}/{record[2]} | "
        if len(record) > 5 and record[4]:
            line += f"{record[4]} {record[5]} | "
        if len(record) > 6 and record[6]:
            line += f"{record[6]} | "
        if len(record) > 7 and record[7]:
            line += f"{record[7]} | "
        if len(record) > 8 and record[8]:
            line += f"{record[8]} | "
        if len(record) > 9 and record[9]:
            line += f"{record[9]}ч"
        if len(record) > 16 and record[16]:
            line += f"| {record[16]}"
        line += "\n---------------------------\n"

        merged = False
        for i, existing in enumerate(lines):
            if existing.startswith(f"{sb_number} | 2 шт"):
                upd = existing.replace("2 шт", "комплект").replace("Левое | ", "").replace("Правое | ", "")
                lines[i] = upd
                merged = True
                break
        if merged:
            continue

        for i, existing in enumerate(lines):
            if existing.startswith(f"{sb_number} | 1 шт"):
                if ("Левое | " in existing and "Правое | " in line) or ("Правое | " in existing and "Левое | " in line):
                    upd = existing.replace("1 шт", "ось").replace("Левое | ", "").replace("Правое | ", "")
                    lines[i] = upd
                    merged = True
                    break
        if merged:
            continue

        lines.append(line)

    chunks: list[str] = []
    current: list[str] = []
    for line in lines:
        current.append(line)
        if len(current) > 30:
            chunks.append("".join(current))
            current = []
    if current:
        chunks.append("".join(current))
    return chunks


def send_wheels_summary_once() -> None:
    chat_id = _parse_int_from_config(TABLO_CHAT_ID)
    if not chat_id:
        return
    chunks = _build_wheels_summary_chunks()
    for chunk in chunks:
        send_text_sync(chat_id, chunk)


def _wheels_loop() -> None:
    chat_id = _parse_int_from_config(TABLO_CHAT_ID)
    if not chat_id:
        logger.info("TABLO_CHAT_ID is empty; wheels scheduler disabled")
        return

    # Первый прогон сразу, затем каждый час.
    while True:
        try:
            send_wheels_summary_once()
        except Exception:
            logger.exception("Wheels scheduler loop error")

        time_mod.sleep(3600)

DEFAULT_SCHEDULES = Schedules(
    logistics_times=[time(8, 5), time(21, 5)],
    report_times=[time(21, 55), time(23, 55), time(2, 55), time(5, 55)],
)


def _next_run(now: datetime, times: List[time]) -> datetime:
    candidates: List[datetime] = []
    for t in times:
        target = datetime.combine(now.date(), t, tzinfo=now.tzinfo)
        if target <= now:
            target += timedelta(days=1)
        candidates.append(target)
    return min(candidates)


def _build_logistics_text(now: datetime) -> str:
    # Ночная/дневная смена по времени
    t = now.time()
    begin, end = ("20:00", "08:00") if (t >= time(20, 0) or t < time(8, 0)) else ("08:00", "20:00")

    active = find_logistics_rows_shift()
    if not active:
        return "Запись о текущем логисте не найдена"

    # active: list[tuple(direction, fio, tag)] — как в TG логике
    logistics = "\n".join(f"{direction}: {fio}" for direction, fio, _tag in active)
    return (
        "Доброго времени суток!\n"
        f"Сегодня ответственные логисты CleanCar с {begin} до {end}:\n{logistics}"
    )


def _logistics_loop(schedules: Schedules) -> None:
    chat_ids = _parse_int_list_from_config(LOGISTICS_CHAT_IDS)
    if not chat_ids:
        logger.warning("LOGISTICS_CHAT_IDS is empty; logistics scheduler disabled")
        return
    while True:
        try:
            now = datetime.now(TZ_MSK)
            target = _next_run(now, schedules.logistics_times)
            delay = max(0.0, (target - now).total_seconds())

            logger.info("Next MAX logistics notify at %s (in %.0fs)", target.isoformat(), delay)
            time_mod.sleep(delay)

            text = _build_logistics_text(datetime.now(TZ_MSK))
            for cid in chat_ids:
                try:
                    send_text_sync(cid, text)
                except Exception:
                    logger.exception("Failed to send logistics to chat_id=%s", cid)
        except Exception:
            logger.exception("Logistics scheduler loop error; retry in 60s")
            time_mod.sleep(60)


def _report_loop(schedules: Schedules) -> None:
    text = "🔔Необходимо сформировать отчёт по выгрузке задач на карту моек BelkaCar & Yandex-Drive🔔"
    chat_id = _parse_int_from_config(REPORT_CHAT_ID)
    if not chat_id:
        logger.warning("REPORT_CHAT_ID is empty; report scheduler disabled")
        return
    while True:
        try:
            now = datetime.now(TZ_MSK)
            target = _next_run(now, schedules.report_times)
            delay = max(0.0, (target - now).total_seconds())

            logger.info("Next MAX report reminder at %s (in %.0fs)", target.isoformat(), delay)
            time_mod.sleep(delay)

            send_text_sync(chat_id, text)
        except Exception:
            logger.exception("Report scheduler loop error; retry in 60s")
            time_mod.sleep(60)


def start_schedulers(schedules: Schedules = DEFAULT_SCHEDULES) -> None:
    threading.Thread(target=_logistics_loop, args=(schedules,), daemon=True).start()
    # threading.Thread(target=_report_loop, args=(schedules,), daemon=True).start()
    # threading.Thread(target=_wheels_loop, daemon=True).start()
