# app/bot.py
from __future__ import annotations
import asyncio
import sys
from pathlib import Path
from datetime import datetime
import json
import re

import logging
import threading
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
from app.handlers.damage import DamageState, cmd_damage, try_handle_damage_step
from app.handlers.sborka import SborkaState, cmd_sborka, try_handle_sborka_step
from app.handlers.soberi import SoberiState, cmd_soberi, cmd_soberi_belka, try_handle_soberi_step

logger = logging.getLogger(__name__)

# ===== State (позже вынесешь в отдельный storage) =====
_reg = RegistrationState(wait_phone_users=set())
_shift = work_shift.WorkShiftState()
_damage = DamageState()
_sborka = SborkaState()
_soberi = SoberiState()

# ===== MAX update parsing helpers =====
def _extract_message(update: dict) -> Optional[dict]:
    """
    Достаёт message из апдейта максимально терпимо к схеме MAX.
    """
    payload = update.get("payload")

    callback = None
    if isinstance(update.get("callback"), dict):
        callback = update["callback"]
    elif isinstance(payload, dict) and isinstance(payload.get("callback"), dict):
        callback = payload.get("callback")

    # Вариант 1: {"message": {...}}
    msg = update.get("message")
    if isinstance(msg, dict):
        merged_msg = dict(msg)
        # Иногда sender/recipient/chat_id находятся рядом с message на уровне update.
        for key in ("sender", "recipient", "chat_id", "body", "payload"):
            if key not in merged_msg and update.get(key) is not None:
                merged_msg[key] = update.get(key)
        # В некоторых callback-апдейтах одновременно приходит message + callback.
        # Если callback есть, сохраняем его и приоритезируем sender/chat_id от callback.
        if isinstance(callback, dict):
            merged_msg["callback"] = callback
            if isinstance(callback.get("sender"), dict):
                merged_msg["sender"] = callback.get("sender")
            chat_id = callback.get("chat_id")
            if chat_id is not None:
                merged_msg["chat_id"] = chat_id
            if "recipient" not in merged_msg and isinstance(callback.get("recipient"), dict):
                merged_msg["recipient"] = callback.get("recipient")
        return merged_msg

    # Вариант 2: {"payload": {"message": {...}}}
    if isinstance(payload, dict):
        msg2 = payload.get("message")
        if isinstance(msg2, dict):
            merged_msg = dict(msg2)
            # Во многих апдейтах MAX sender/chat_id лежат на уровне payload.
            for key in ("sender", "recipient", "chat_id", "body"):
                if key not in merged_msg and payload.get(key) is not None:
                    merged_msg[key] = payload.get(key)
            if "payload" not in merged_msg:
                merged_msg["payload"] = payload

        # Аналогично варианту выше, callback может идти рядом с message.
        if isinstance(callback, dict):
            merged_msg["callback"] = callback
            if isinstance(callback.get("sender"), dict):
                merged_msg["sender"] = callback.get("sender")
            chat_id = callback.get("chat_id")
            if chat_id is not None:
                merged_msg["chat_id"] = chat_id
            if "recipient" not in merged_msg and isinstance(callback.get("recipient"), dict):
                merged_msg["recipient"] = callback.get("recipient")

        return merged_msg

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

    def _from_node(node: object) -> str:
        text = _clean(node)
        if text:
            if text.startswith("{") and text.endswith("}"):
                try:
                    parsed = json.loads(text)
                except Exception:
                    return text
                nested = _from_node(parsed)
                return nested or text
            return text
        if isinstance(node, dict):
            for key in ("text", "caption", "command", "callback_data", "data", "payload"):
                nested = _from_node(node.get(key))
                if nested:
                    return nested

            return ""

        return ""

    direct = _from_node(msg.get("text"))
    if direct:
        return direct

    # Для callback-событий сначала ищем явную команду в callback/payload/action,
    # иначе текст исходного сообщения (body.text) может перехватить маршрутизацию.
    for key in ("callback", "payload", "action", "body"):
        nested = _from_node(msg.get(key))
        if nested:
            return nested

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
    if not isinstance(sender, dict):
        sender = {}

    callback = msg.get("callback")
    if not isinstance(callback, dict):
        callback = {}

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

    for key in ("user_id", "sender_user_id", "from_user_id", "id"):
        uid = _to_int(callback.get(key))
        if uid is not None:
            return uid


    for node in (
        callback.get("sender"),
        callback.get("user"),
        sender,
        sender.get("user"),
        (msg.get("body") or {}).get("sender") if isinstance(msg.get("body"), dict) else None,
        (msg.get("payload") or {}).get("sender") if isinstance(msg.get("payload"), dict) else None,
    ):
        if isinstance(node, dict):
            for key in ("user_id", "id", "sender_user_id", "from_user_id"):
                uid = _to_int(node.get(key))
                if uid is not None:
                    return uid


    return None


def _chat_id(msg: dict) -> Optional[int]:
    def _to_chat_id(value: object) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.lstrip("-").isdigit():
            return int(value)
        return None

    for value in (
        msg.get("chat_id"),
        (msg.get("recipient") or {}).get("chat_id") if isinstance(msg.get("recipient"), dict) else None,
        (msg.get("body") or {}).get("chat_id") if isinstance(msg.get("body"), dict) else None,
        (msg.get("body") or {}).get("recipient", {}).get("chat_id") if isinstance((msg.get("body") or {}).get("recipient"), dict) else None,
        (msg.get("payload") or {}).get("chat_id") if isinstance(msg.get("payload"), dict) else None,
        (msg.get("payload") or {}).get("recipient", {}).get("chat_id") if isinstance((msg.get("payload") or {}).get("recipient"), dict) else None,
        (msg.get("callback") or {}).get("chat_id") if isinstance(msg.get("callback"), dict) else None,
        (msg.get("callback") or {}).get("recipient", {}).get("chat_id") if isinstance((msg.get("callback") or {}).get("recipient"), dict) else None,
    ):
        cid = _to_chat_id(value)
        if cid is not None:
            return cid

    return None

# ===== Routing =====
async def _route_text(user_id: int, chat_id: int, text: str, msg: dict) -> None:
    t = text.strip()

    # Кнопки в MAX могут присылать либо slash-команды, либо человекочитаемый текст.
    aliases = {
        "start": "/start",
        "registration": "/registration",
        "register": "/registration",
        "start_job_shift": "/start_job_shift",
        "start-work-shift": "/start_job_shift",
        "end_work_shift": "/end_work_shift",
        "end-job-shift": "/end_work_shift",
        "damage": "/damage",
        "sborka": "/sborka",
        "soberi": "/soberi",
        "soberi_belka": "/soberi_belka",
        "сборка": "/sborka",
        "повреждение": "/damage",
        "регистрация": "/registration",
        "начало смены": "/start_job_shift",
        "окончание смены": "/end_work_shift",
    }

    if t:
        normalized = t.strip().lower()
        if normalized.startswith("/"):
            normalized = normalized[1:]
        normalized = normalized.split("@", 1)[0].strip()
        normalized = re.sub(r"\s+", "_", normalized)
        t = aliases.get(normalized, t)

    # 1) Сначала — шаги (stateful). Если ждём телефон — обработаем тут.
    if await try_handle_phone_step(_reg, user_id, chat_id, t, msg):
        return
    if await work_shift.try_handle_work_shift_step(_shift, user_id, chat_id, t, msg):
        return
    if await try_handle_damage_step(_damage, user_id, chat_id, t, msg):
        return
    if await try_handle_sborka_step(_sborka, user_id, chat_id, t, msg):
        return
    if await try_handle_soberi_step(_soberi, user_id, chat_id, t, msg):
        return

    # 2) Команды
    if not t:
        return

    if t == "/start":
        await send_text(chat_id, WELCOME_TEXT)
        return

    if t == "/registration":
        await cmd_registration(_reg, user_id, chat_id)
        return

    if t == "/start_job_shift":
        await work_shift.cmd_start_job_shift(_shift, user_id, chat_id)
        return

    if t == "/end_work_shift":
        await work_shift.cmd_end_work_shift(_shift, user_id, chat_id)
        return

    if t == "/damage":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_damage(_damage, user_id, chat_id, username)
        return
    if t in {"/sborka", "/sborka_ko"}:
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_sborka(_sborka, user_id, chat_id, username, cmd=t.lstrip("/"))
        return
    if t == "/soberi":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_soberi(_soberi, user_id, chat_id, username)
        return
    if t == "/soberi_belka":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_soberi_belka(_soberi, user_id, chat_id, username)
        return

    # 3) Default
    await send_text(chat_id, "Команды: /start, /registration, /start_job_shift, /end_work_shift, /damage, /sborka, /soberi, /soberi_belka")
async def _polling_loop() -> None:
    marker: Optional[int] = None
    logging.info("MAX polling started")

    while True:
        try:
            data = await get_updates(marker)

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

                # Callback events (inline keyboard buttons) can come without text
                # and without attachments, so they still must reach the handlers.
                if not text and not _has_attachments(msg) and not isinstance(msg.get("callback"), dict):
                    continue

                try:
                    await _route_text(user_id, chat_id, text, msg)
                except Exception:
                    logging.exception("handler failed user_id=%s chat_id=%s", user_id, chat_id)

        except Exception:
            logging.exception("polling error; retry in 2s")
            await asyncio.sleep(2)


def run() -> None:
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    log_name = f'{LOGS_DIR}/{datetime.now().strftime("%Y-%m-%d")}.log'
    file_handler = logging.FileHandler(log_name, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(fmt)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)
    root_logger.addHandler(stream_handler)

    logger.info(
        "MAX_TOKEN loaded | length=%s | prefix=%s***",
        len(MAX_TOKEN),
        MAX_TOKEN[:4],
    )

    # HTTP endpoints (/notify, /notify_image, /health)
    threading.Thread(target=run_http, daemon=True).start()

    # Periodic notifications (logistics + report reminders)
    start_schedulers()

    # Polling в основном потоке
    asyncio.run(_polling_loop())
