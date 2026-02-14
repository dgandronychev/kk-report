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
from app.handlers.registration import (
    RegistrationState,
    cmd_registration,
    try_handle_phone_step,
)

# from app.utils.scheduler import start_scheduler

logger = logging.getLogger(__name__)

# ===== State (позже вынесешь в отдельный storage) =====
_reg = RegistrationState(wait_phone_users=set())


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

    return None


def _msg_text(msg: dict) -> str:
    text = msg.get("text")
    if isinstance(text, str) and text:
        return text

    body = msg.get("body")
    if isinstance(body, dict):
        text2 = body.get("text")
        if isinstance(text2, str):
            return text2

    return ""


def _sender_id(msg: dict) -> Optional[int]:
    sender = msg.get("sender") or {}
    uid = sender.get("user_id")
    if isinstance(uid, int):
        return uid
    if isinstance(uid, str) and uid.isdigit():
        return int(uid)
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

    # 2) Команды
    if t == "/start":
        send_text(chat_id, WELCOME_TEXT)
        return

    if t == "/registration":
        cmd_registration(_reg, user_id, chat_id)
        return

    # 3) Default
    send_text(chat_id, "Команды: /start, /registration")


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
                user_id = _sender_id(msg)
                text = _msg_text(msg)

                if chat_id is None or user_id is None or not text:
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

