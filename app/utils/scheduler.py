# app/utils/max_scheduler.py
from __future__ import annotations

import logging
import os
import threading
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import List, Optional
from zoneinfo import ZoneInfo

from app.utils.max_api import send_text_sync
from app.utils.gsheets import find_logistics_rows_shift
from app.config import LOGISTICS_CHAT_IDS, REPORT_CHAT_ID

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
    threading.Thread(target=_report_loop, args=(schedules,), daemon=True).start()
