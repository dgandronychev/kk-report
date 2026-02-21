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
    reset_registration_progress,
)
from app.handlers import work_shift
from app.handlers.damage import DamageState, cmd_damage, try_handle_damage_step, pop_pending_sborka_transfer, reset_damage_progress, warmup_damage_refs
from app.handlers.sborka import SborkaState, cmd_sborka, try_handle_sborka_step, start_from_damage_transfer, reset_sborka_progress, warmup_sborka_refs
from app.handlers.soberi import SoberiState, cmd_soberi, cmd_soberi_belka, try_handle_soberi_step, reset_soberi_progress, warmup_soberi_refs
from app.handlers.nomenclature import NomenclatureState, cmd_nomenclature, try_handle_nomenclature_step, reset_nomenclature_progress, warmup_nomenclature_refs
from app.handlers.open_gate import OpenGateState, cmd_open_gate, try_handle_open_gate_step, reset_open_gate_progress
from app.handlers.finance import (
    FinanceState,
    cmd_parking,
    cmd_zapravka,
    cmd_expense,
    try_handle_finance_step,
    reset_finance_progress,
)
from app.handlers.move import MoveState, cmd_move, try_handle_move_step, reset_move_progress

logger = logging.getLogger(__name__)

# ===== State (позже вынесешь в отдельный storage) =====
_reg = RegistrationState(wait_phone_users=set())
_shift = work_shift.WorkShiftState()
_damage = DamageState()
_sborka = SborkaState()
_soberi = SoberiState()
_nomenclature = NomenclatureState()
_open_gate = OpenGateState()
_finance = FinanceState()
_move = MoveState()

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

def _is_private_chat(msg: dict, chat_id: int, user_id: int) -> bool:
    def _normalize_chat_type(value: object) -> str:
        if isinstance(value, str):
            return value.strip().lower()
        return ""

    # Явные типы личного диалога в разных схемах MAX-подобных апдейтов.
    private_types = {
        "private",
        "dialog",
        "direct",
        "dm",
        "one_to_one",
        "personal",
        "user",
    }

    for node in (
        msg,
        msg.get("recipient") if isinstance(msg.get("recipient"), dict) else None,
        msg.get("sender") if isinstance(msg.get("sender"), dict) else None,
        msg.get("body") if isinstance(msg.get("body"), dict) else None,
        (msg.get("body") or {}).get("recipient") if isinstance((msg.get("body") or {}).get("recipient"), dict) else None,
        msg.get("payload") if isinstance(msg.get("payload"), dict) else None,
        (msg.get("payload") or {}).get("recipient") if isinstance((msg.get("payload") or {}).get("recipient"), dict) else None,
        msg.get("callback") if isinstance(msg.get("callback"), dict) else None,
        (msg.get("callback") or {}).get("recipient") if isinstance((msg.get("callback") or {}).get("recipient"), dict) else None,
    ):
        if not isinstance(node, dict):
            continue
        for key in ("chat_type", "type", "dialog_type", "conversation_type", "peer_type"):
            chat_type = _normalize_chat_type(node.get(key))
            if chat_type in private_types:
                return True
            if chat_type and chat_type not in private_types:
                return False

    # Fallback: в личке chat_id часто совпадает с user_id.
    return chat_id == user_id

async def _warmup_caches() -> None:
    try:
        await asyncio.gather(
            warmup_damage_refs(),
            warmup_sborka_refs(),
            warmup_soberi_refs(),
            warmup_nomenclature_refs(),
        )
        logging.info("reference caches warmed up")
    except Exception:
        logging.exception("failed to warm up reference caches")


def _reset_user_progress(user_id: int, chat_id: int) -> None:
    reset_registration_progress(_reg, user_id)
    reset_damage_progress(_damage, user_id)
    reset_sborka_progress(_sborka, user_id)
    reset_soberi_progress(_soberi, user_id)
    reset_nomenclature_progress(_nomenclature, user_id)
    reset_open_gate_progress(_open_gate, user_id)
    reset_finance_progress(_finance, user_id)
    reset_move_progress(_move, user_id)
    work_shift.reset_work_shift_progress(_shift, user_id, chat_id)

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
        "open_gate": "/open_gate",
        "открыть ворота": "/open_gate",
        "parking": "/parking",
        "zapravka": "/zapravka",
        "expense": "/expense",
        "парковка": "/parking",
        "заправка": "/zapravka",
        "расход": "/expense",
        "move": "/move",
        "перемещение": "/move",
    }
    is_command = False
    if t:
        normalized = t.strip().lower()
        if normalized.startswith("/"):
            normalized = normalized[1:]
            is_command = True
        normalized = normalized.split("@", 1)[0].strip()
        normalized = re.sub(r"\s+", "_", normalized)
        mapped = aliases.get(normalized)
        if mapped:
            t = mapped
            is_command = True

    if is_command:
        _reset_user_progress(user_id, chat_id)

    # 1) Сначала — шаги (stateful). Если ждём телефон — обработаем тут.
    if await try_handle_phone_step(_reg, user_id, chat_id, t, msg):
        return
    if await work_shift.try_handle_work_shift_step(_shift, user_id, chat_id, t, msg):
        return
    if await try_handle_damage_step(_damage, user_id, chat_id, t, msg):
        transfer = pop_pending_sborka_transfer(_damage, user_id)
        if transfer is not None:
            sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
            username = str(sender.get("username") or sender.get("first_name") or user_id)
            await start_from_damage_transfer(_sborka, user_id, chat_id, username, transfer)
        return
    if await try_handle_sborka_step(_sborka, user_id, chat_id, t, msg):
        return
    if await try_handle_soberi_step(_soberi, user_id, chat_id, t, msg):
        return
    if await try_handle_nomenclature_step(_nomenclature, user_id, chat_id, t, msg):
        return
    if await try_handle_open_gate_step(_open_gate, user_id, chat_id, t, msg):
        return
    if await try_handle_finance_step(_finance, user_id, chat_id, t, msg):
        return
    if await try_handle_move_step(_move, user_id, chat_id, t, msg):
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
        transfer = pop_pending_sborka_transfer(_damage, user_id)
        if transfer is not None:
            await start_from_damage_transfer(_sborka, user_id, chat_id, username, transfer)
            return
        await cmd_sborka(_sborka, user_id, chat_id, username, cmd=t.lstrip("/"))
        return
    if t == "/soberi":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_soberi(_soberi, user_id, chat_id, username, msg)
        return
    if t == "/soberi_belka":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_soberi_belka(_soberi, user_id, chat_id, username, msg)
        return
    if t == "/check":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_sborka(_sborka, user_id, chat_id, username, cmd="check")
        return
    if t == "/nomenclature":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_nomenclature(_nomenclature, user_id, chat_id, username)
        return
    if t == "/open_gate":
        await cmd_open_gate(_open_gate, user_id, chat_id, msg)
        return
    if t == "/parking":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_parking(_finance, user_id, chat_id, username, msg)
        return
    if t == "/zapravka":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_zapravka(_finance, user_id, chat_id, username, msg)
        return
    if t == "/expense":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_expense(_finance, user_id, chat_id, username, msg)
        return
    if t == "/move":
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        username = str(sender.get("username") or sender.get("first_name") or user_id)
        await cmd_move(_move, user_id, chat_id, username, msg)
        return

    # 3) Default
    await send_text(chat_id, "Команды: /start, /registration, /start_job_shift, /end_work_shift, /damage, /sborka, /check, /soberi, /soberi_belka, /nomenclature, /open_gate")

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

                if not _is_private_chat(msg, chat_id=chat_id, user_id=user_id):
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

    try:
        asyncio.run(_warmup_caches())
    except Exception:
        logging.exception("warmup stage crashed")

    # Polling в основном потоке
    asyncio.run(_polling_loop())
