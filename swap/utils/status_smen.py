import asyncio
import logging
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import requests
import gspread
from aiogram import types
from aiogram.utils.markdown import escape_md
from app.utils.gsheets import get_performers_with_tags_for_date, cancel_in_answers, get_performers_for_date
from app.bot import bot
from app.config import (
    THREAD_ID_SHIFT_LOG,
    CHAT_ID_GRAPH,
    THREAD_ID_GRAPH,
    URL_GET_TASK_MOIKA,
    URL_IS_ACTIV_KK,
    URL_GET_USER_ID,
    URL_GET_COUNT_TASK,
    CHAT_ID_PROBLEM_SHM_ST,
    CHAT_ID_PROBLEM_LOG,

)
logger = logging.getLogger(__name__)
MOSCOW = ZoneInfo("Europe/Moscow")

def check_user_info(username: str) -> str:
    try:
        r = requests.get(
            URL_GET_TASK_MOIKA,
            params={"tg_username": username},
            timeout=5
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("is_shift_found"):
            return ""
        return data.get("first_task_date") or ""
    except requests.RequestException:
        return ""

COMPANY_SHORT = {
    "Delimobil": "Д",
    "Белка": "Б",
    "YandexDrive": "Я",
}

def get_user_target_count(tag: str) -> str:
    tag = tag.lstrip('@')
    try:
        r = requests.get(
            URL_GET_COUNT_TASK,
            json={"tg_username": tag},
            timeout=5
        )
        r.raise_for_status()
        data = r.json()

        # Основной суммарный счётчик
        count = data.get("completed_tasks_count")
        if not count:
            return "0"

        # Собираем детализацию по компаниям
        details_parts = []
        for item in data.get("completed_tasks_by_company") or []:
            name = item.get("carsharing__name")
            company_count = item.get("count")
            if not company_count:
                continue
            if not name:
                continue
            details_parts.append(f"{name} - {company_count}")

        if details_parts:
            details_str = " ".join(details_parts)
            return f"{count} ({details_str})"
        else:
            # Если детализации нет, вернём только общее количество
            return str(count)

    except requests.RequestException:
        return "0"

def is_tag_active(tag: str) -> bool:
    try:
        r = requests.get(URL_IS_ACTIV_KK, params={"tg_username": tag}, timeout=5)
        r.raise_for_status()
        return bool(r.json().get("is_active"))
    except requests.RequestException:
        return True


def build_text(
    target: date,
    statuses: dict[tuple[str,str], str],
    include_time: bool = False,
) -> str:
    header = escape_md(f"#График Смена {target.strftime('%d.%m.%Y')}")
    lines = [header, ""]
    for (fio, tag), mark in statuses.items():
        fio_esc = escape_md(fio)
        mark_esc = escape_md(mark)  # ← Экранируем дату
        if tag:
            tag_esc = escape_md(tag)
            # обратите внимание, что здесь в строке literal `\\-` даёт в итоговом тексте "\-"
            lines.append(f"{fio_esc} @{tag_esc} \\- {mark_esc}")
        else:
            lines.append(f"{fio_esc} \\- {mark_esc}")
    if include_time:
        now = datetime.now(MOSCOW).strftime("%H:%M:%S")
        lines.append(f"\n{escape_md('Обновлено:')} {escape_md(now)}")
    return "\n".join(lines)


def build_text_count(
    target: date,
    statuses: dict[tuple[str,str], str],
) -> str:
    header = escape_md(f"#Количество_задач\n Смена {target.strftime('%d.%m.%Y')}")
    lines = [header, ""]
    for (fio, tag), mark in statuses.items():
        fio_esc = escape_md(fio)
        mark_esc = escape_md(mark)  # ← Экранируем дату
        if tag:
            tag_esc = escape_md(tag)
            # обратите внимание, что здесь в строке literal `\\-` даёт в итоговом тексте "\-"
            lines.append(f"{fio_esc} @{tag_esc} \\- {mark_esc}")
        else:
            lines.append(f"{fio_esc} \\- {mark_esc}")
    return "\n".join(lines)


async def send_initial_shift_status() -> int:
    today = date.today()
    performers = get_performers_with_tags_for_date(today)
    statuses: dict[tuple[str, str], str] = {}
    for fio, tag in performers:
        first_date = check_user_info(tag)
        if first_date:
            try:
                dt = datetime.fromisoformat(first_date)
                formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                formatted = first_date  # на случай некорректного формата
            statuses[(fio, tag)] = f"✅ {formatted}"
        else:
            statuses[(fio, tag)] = "❌"

    text = build_text(today, statuses)
    msg = await bot.send_message(
        CHAT_ID_GRAPH,
        text,
        parse_mode=types.ParseMode.MARKDOWN_V2,
        message_thread_id=THREAD_ID_GRAPH,
    )
    return msg.message_id

async def send_count_target_message(for_date: date | None = None) -> int:
    """
    Формирует и отправляет сообщение количества выполненных задач по исполнителям
    за день for_date (по умолчанию — сегодня). Всегда отправляет НОВОЕ сообщение.
    """
    if for_date is None:
        for_date = date.today()
    performers = get_performers_with_tags_for_date(for_date)
    statuses: dict[tuple[str, str], str] = {}
    for fio, tag in performers:
        statuses[(fio, tag)] = get_user_target_count(tag)

    text = build_text_count(for_date, statuses)
    msg = await bot.send_message(
        CHAT_ID_PROBLEM_LOG,
        text,
        parse_mode=types.ParseMode.MARKDOWN_V2,
        message_thread_id=THREAD_ID_SHIFT_LOG,
    )
    return msg.message_id

async def handle_shift_updates(message_id: int, target_date: date):
    """Редактирует сообщение:
      – каждые 10 минут с 21:10 до 23:00,
      – каждые 30 минут с 23:30 до 08:30,
      – в 09:00 финалит, ставит ❌ у тех, кто так и не появился.
    """
    # 1. рамки «окошек»
    start1 = datetime.combine(target_date, time(21, 10), tzinfo=MOSCOW)
    end1   = datetime.combine(target_date, time(23,  1), tzinfo=MOSCOW)
    start2 = datetime.combine(target_date, time(23, 30), tzinfo=MOSCOW)
    end2   = datetime.combine(target_date + timedelta(days=1), time( 9,  1), tzinfo=MOSCOW)
    final_time = end2

    # 2. инициализация статусов
    performers = get_performers_with_tags_for_date(target_date)
    statuses: dict[tuple[str,str], str] = {}
    for fio, tag in performers:
        first_date = check_user_info(tag)
        if first_date:
            try:
                dt = datetime.fromisoformat(first_date)
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                date_str = first_date
            statuses[(fio, tag)] = f"✅ {date_str}"
        else:
            statuses[(fio, tag)] = "❌"
    logger.info("Initialized shift statuses: %s", statuses)

    async def _run_cycle(interval: float, end_marker: datetime):
        """Обновляет статусы до end_marker с паузой interval"""
        while datetime.now(MOSCOW) <= end_marker:
            # 3. обновление статусов
            for (fio, tag), mark in list(statuses.items()):
                if mark == "❌":
                    fd = check_user_info(tag)
                    if fd:
                        try:
                            dt = datetime.fromisoformat(fd)
                            ds = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            ds = fd
                        statuses[(fio, tag)] = f"✅ {ds}"
            logger.info("Updating shift statuses: %s", statuses)

            # 4. безопасный edit_message_text
            try:
                await bot.edit_message_text(
                    build_text(target_date, statuses, include_time=True),
                    chat_id=CHAT_ID_GRAPH,
                    message_id=message_id,
                    parse_mode=types.ParseMode.MARKDOWN_V2,
                )
            except Exception:
                logger.exception("Failed to edit shift status message")

            await asyncio.sleep(interval)

    # дождаться 21:10, если ещё раньше
    now = datetime.now(MOSCOW)
    if now < start1:
        await asyncio.sleep((start1 - now).total_seconds())
    # цикл 10 минут до 23:00
    await _run_cycle(interval=10*60, end_marker=end1)

    # дождаться 23:30
    now = datetime.now(MOSCOW)
    if now < start2:
        await asyncio.sleep((start2 - now).total_seconds())
    # цикл 30 минут до 08:30
    await _run_cycle(interval=30*60, end_marker=end2)

    # финальное сообщение в 09:00
    now = datetime.now(MOSCOW)
    if now < final_time:
        await asyncio.sleep((final_time - now).total_seconds())

    for key, mark in statuses.items():
        if not mark:
            statuses[key] = "❌"

    try:
        await bot.edit_message_text(
            build_text(target_date, statuses, include_time=True),
            chat_id=CHAT_ID_GRAPH,
            message_id=message_id,
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )
    except Exception:
        logger.exception("Failed to send final shift-status update")

async def send_daily_shift_reminder():
    """
    В 13:00 Мск шлёт в чат список тегов активных исполнителей,
    а у неактивных – проставляет в "Ответы" статус "Отменен".
    """
    tomorrow = date.today()
    performers = get_performers_for_date(tomorrow)

    header = escape_md(f"По графику {tomorrow.strftime('%d.%m')} выходят:")
    note   = escape_md("Коллеги, снятие со смены возможно только за сутки до дня смены.")
    lines = [header, ""]

    for fio, tag in performers:
        if is_tag_active(str(tag[1:])):
            lines.append(f"{escape_md(tag)}")
        else:
            cancel_in_answers(fio)

    lines.extend(["", note])
    text = "\n".join(lines)

    await bot.send_message(
        CHAT_ID_GRAPH,
        text,
        parse_mode=types.ParseMode.MARKDOWN_V2,
        message_thread_id=THREAD_ID_GRAPH,
    )
