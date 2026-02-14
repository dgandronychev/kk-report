# app/bot.py
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime

import logging
import threading
import time
from typing import Optional

from app.utils.scheduler import start_schedulers
from app.config import WELCOME_TEXT, LOGS_DIR, MAX_TOKEN
from app.utils.http import run_http
from app.utils.max_api import get_updates, send_text
from app.utils.chat_memory import remember_chat_id
from app.handlers.registration import (
    RegistrationState,
    cmd_registration,
    try_handle_phone_step,
)
from app.handlers import work_shift

logger = logging.getLogger(__name__)

# ===== State (позже вынесешь в отдельный storage) =====
_reg = RegistrationState(wait_phone_users=set())
_shift = work_shift.WorkShiftState()

# ===== MAX update parsing helpers =====
def _extract_message(update: dict) -> Optional[dict]:
    """
    Достаёт message из апдейта максимально терпимо к схеме MAX.
    """
    # Вариант 1: {"message": {...}}
    msg = update.get("message")
    if isinstance(msg, dict):
        return msg

    # Вариант 2: {"payload": {"message": {...}}}
    payload = update.get("payload")
    if isinstance(payload, dict):
        msg2 = payload.get("message")
        if isinstance(msg2, dict):
            return msg2

    callback = None
    if isinstance(update.get("callback"), dict):
        callback = update["callback"]
    elif isinstance(payload, dict) and isinstance(payload.get("callback"), dict):
        callback = payload.get("callback")

    if not isinstance(callback, dict):
        return None

    # В callback-апдейтах MAX может передавать исходное сообщение в callback.message,
    # а данные нажатой кнопки — в callback.payload/data.
    callback_msg = callback.get("message")
    if not isinstance(callback_msg, dict):
        callback_msg = {}

    merged_msg = dict(callback_msg)
    merged_msg["callback"] = callback

    # Для callback-событий приоритет у callback.sender (это тот, кто нажал кнопку).
    # callback.message.sender часто содержит автора исходного сообщения (бота).
    if isinstance(callback.get("sender"), dict):
        merged_msg["sender"] = callback.get("sender")

    if "chat_id" not in merged_msg:
        chat_id = callback.get("chat_id")
        if chat_id is not None:
            merged_msg["chat_id"] = chat_id

    if "recipient" not in merged_msg and isinstance(callback.get("recipient"), dict):
        merged_msg["recipient"] = callback.get("recipient")

    return merged_msg


def _msg_text(msg: dict) -> str:
    def _clean(value: object) -> str:
        if isinstance(value, str):
            return value.strip()
        return ""

    text = _clean(msg.get("text"))
    if text:
        return text

    body = msg.get("body")
    if isinstance(body, dict):
        for key in ("text", "caption", "command", "callback_data", "data"):
            text2 = _clean(body.get(key))
            if text2:
                return text2

        callback = body.get("callback")
        if isinstance(callback, dict):
            for key in ("text", "data", "payload", "command"):
                text3 = _clean(callback.get(key))
                if text3:
                    return text3

    for container in ("payload", "callback", "action"):
        node = msg.get(container)
        if isinstance(node, dict):
            for key in ("text", "data", "payload", "command"):
                text4 = _clean(node.get(key))
                if text4:
                    return text4

    return ""

def _has_attachments(msg: dict) -> bool:
    for attachments in (
        msg.get("attachments"),
        (msg.get("body") or {}).get("attachments") if isinstance(msg.get("body"), dict) else None,
        (msg.get("payload") or {}).get("attachments") if isinstance(msg.get("payload"), dict) else None,
    ):
        if isinstance(attachments, list) and len(attachments) > 0:
            return True
    return False

def _sender_id(msg: dict) -> Optional[int]:
    sender = msg.get("sender") or {}

    def _to_int(value: object) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            value = value.strip()
            if value.isdigit():
                return int(value)
            m = re.search(r"\d+", value)
            if m:
                return int(m.group(0))
        return None

    for key in ("user_id", "id"):
        uid = _to_int(sender.get(key))
        if uid is not None:
            return uid

    user = sender.get("user")
    if isinstance(user, dict):
        for key in ("user_id", "id"):
            uid = _to_int(user.get(key))
            if uid is not None:
                return uid

    return None


def _chat_id(msg: dict) -> Optional[int]:
    # Иногда chat_id лежит прямо в msg
    cid = msg.get("chat_id")
    if isinstance(cid, int):
        return cid
    if isinstance(cid, str) and cid.lstrip("-").isdigit():
        return int(cid)

    # Иногда в recipient
    recipient = msg.get("recipient") or {}
    cid2 = recipient.get("chat_id")
    if isinstance(cid2, int):
        return cid2
    if isinstance(cid2, str) and cid2.lstrip("-").isdigit():
        return int(cid2)

    return None

# ===== Routing =====
def _route_text(user_id: int, chat_id: int, text: str, msg: dict) -> None:
    t = text.strip()

    # 1) Сначала — шаги (stateful). Если ждём телефон — обработаем тут.
    if try_handle_phone_step(_reg, user_id, chat_id, t, msg):
        return
    if work_shift.try_handle_work_shift_step(_shift, user_id, chat_id, t, msg):
        return

    # 2) Команды
    if not t:
        return

    if t == "/start":
        send_text(chat_id, WELCOME_TEXT)
        return

    if t == "/registration":
        cmd_registration(_reg, user_id, chat_id)
        return

    if t == "/start_job_shift":
        work_shift.cmd_start_job_shift(_shift, user_id, chat_id)
        return

    if t == "/end_work_shift":
        work_shift.cmd_end_work_shift(_shift, user_id, chat_id)
        return

    # 3) Default
    send_text(chat_id, "Команды: /start, /registration, /start_job_shift, /end_work_shift")

def _polling_loop() -> None:
    marker: Optional[int] = None
    logging.info("MAX polling started")

    while True:
        try:
            data = get_updates(marker)

            # marker
            m = data.get("marker")
            if isinstance(m, int):
                marker = m
            elif isinstance(m, str) and m.isdigit():
                marker = int(m)

            # updates
            updates = data.get("updates")
            if not isinstance(updates, list):
                updates = []

            for upd in updates:
                if not isinstance(upd, dict):
                    continue

                msg = _extract_message(upd)
                if not msg:
                    continue

                chat_id = _chat_id(msg)
                if chat_id is None:
                    continue

                remember_chat_id(chat_id)

                user_id = _sender_id(msg)
                text = _msg_text(msg)

                if user_id is None:
                    continue

                if not text and not _has_attachments(msg):
                    continue

                try:
                    _route_text(user_id, chat_id, text, msg)
                except Exception:
                    logging.exception("handler failed user_id=%s chat_id=%s", user_id, chat_id)

        except Exception:
            logging.exception("polling error; retry in 2s")
            time.sleep(2)


def run() -> None:
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    log_name = f'{LOGS_DIR}/{datetime.now().strftime("%Y-%m-%d")}.log'
    file_handler = logging.FileHandler(log_name, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    logger.info(
        "MAX_TOKEN loaded | length=%s | prefix=%s***",
        len(MAX_TOKEN),
        MAX_TOKEN[:4]
    )

    # HTTP endpoints (/notify, /notify_image, /health)
    threading.Thread(target=run_http, daemon=True).start()

    # Periodic notifications (logistics + report reminders)
    start_schedulers()

    # Polling в основном потоке
    _polling_loop()