from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
from pprint import pformat
from typing import Dict, List, Optional, Set
from app.config import WORK_SHIFT_CHAT_ID
from app.utils.max_api import send_message, send_text, send_text_with_reply_buttons

logger = logging.getLogger(__name__)


def _safe_dump(value: object, max_len: int = 700) -> str:
    try:
        dumped = pformat(value, compact=True, width=120)
    except Exception:
        dumped = str(value)
    if len(dumped) <= max_len:
        return dumped
    return f"{dumped[:max_len]}...<truncated>"

@dataclass
class WorkShiftState:
    wait_files_start: set[int] = field(default_factory=set)
    wait_files_end: set[int] = field(default_factory=set)
    files_by_user: Dict[int, List[dict]] = field(default_factory=dict)
    seen_file_keys_by_user: Dict[int, Set[str]] = field(default_factory=dict)
    active_user_by_chat: Dict[int, int] = field(default_factory=dict)

def _resolve_flow_user(st: WorkShiftState, user_id: int, chat_id: int) -> Optional[int]:
    if user_id in st.wait_files_start or user_id in st.wait_files_end:
        return user_id

    mapped_user_id = st.active_user_by_chat.get(chat_id)
    if mapped_user_id is None:
        return None
    if mapped_user_id in st.wait_files_start or mapped_user_id in st.wait_files_end:
        return mapped_user_id
    return None

def _clear_flow(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    st.wait_files_start.discard(user_id)
    st.wait_files_end.discard(user_id)
    st.files_by_user.pop(user_id, None)
    st.seen_file_keys_by_user.pop(user_id, None)
    if st.active_user_by_chat.get(chat_id) == user_id:
        st.active_user_by_chat.pop(chat_id, None)

def _extract_user_label(msg: dict, user_id: int) -> str:
    sender = msg.get("sender") or {}
    username = sender.get("username") or sender.get("first_name") or str(user_id)
    if isinstance(username, str) and username.startswith("@"):
        return username
    return f"@{username}"


def _extract_fio(msg: dict) -> str:
    sender = msg.get("sender") or {}
    first_name = str(sender.get("first_name") or "").strip()
    last_name = str(sender.get("last_name") or "").strip()
    fio = f"{last_name} {first_name}".strip()
    return fio or "Не указано"


def _extract_attachments(msg: dict, include_nested: bool = True) -> List[dict]:
    attachments = msg.get("attachments")
    if include_nested and not isinstance(attachments, list):
        body = msg.get("body")
        if isinstance(body, dict):
            attachments = body.get("attachments")

    if include_nested and not isinstance(attachments, list):
        payload = msg.get("payload")
        if isinstance(payload, dict):
            attachments = payload.get("attachments")

    if not isinstance(attachments, list):
        return []

    files: List[dict] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        f_type = str(item.get("type") or "unknown")
        if f_type not in {"image", "video", "file", "audio"}:
            continue
        files.append({"type": f_type, "payload": item.get("payload")})
    return files

def _attachment_key(item: dict) -> str:
    payload = item.get("payload")
    try:
        payload_str = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        payload_str = str(payload)
    return f"{item.get('type')}::{payload_str}"

def _caption(action: str, fio: str, username: str) -> str:
    ts = datetime.now() + timedelta(hours=3)
    return (
        f"⌚️ {ts.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"👷 {username}\n\n"
        f"{fio}\n\n"
        f"{action}\n"
    )


async def _send_work_shift_prompt(chat_id: int) -> None:
    await send_text_with_reply_buttons(
        chat_id=chat_id,
        text="Прикрепите фото/видео/файл и нажмите «Готово». Для отмены нажмите «Выход».",
        button_texts=["Готово", "Выход"],
        button_payloads=["work_shift_done", "work_shift_exit"],
    )


async def cmd_start_job_shift(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    old_user_id = st.active_user_by_chat.get(chat_id)
    if old_user_id is not None and old_user_id != user_id:
        _clear_flow(st, old_user_id, chat_id)
    st.wait_files_end.discard(user_id)
    st.wait_files_start.add(user_id)
    st.files_by_user[user_id] = []
    st.seen_file_keys_by_user[user_id] = set()
    st.active_user_by_chat[chat_id] = user_id
    await _send_work_shift_prompt(chat_id)


async def cmd_end_work_shift(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    old_user_id = st.active_user_by_chat.get(chat_id)
    if old_user_id is not None and old_user_id != user_id:
        _clear_flow(st, old_user_id, chat_id)
    st.wait_files_start.discard(user_id)
    st.wait_files_end.add(user_id)
    st.files_by_user[user_id] = []
    st.seen_file_keys_by_user[user_id] = set()
    st.active_user_by_chat[chat_id] = user_id
    await _send_work_shift_prompt(chat_id)


async def _finalize(st: WorkShiftState, user_id: int, chat_id: int, msg: dict, action: str) -> bool:
    files = st.files_by_user.get(user_id, [])
    logger.info(
        "work_shift finalize requested action=%s chat_id=%s user_id=%s files_count=%s",
        action,
        chat_id,
        user_id,
        len(files),
    )
    if not files:
        logger.warning(
            "work_shift finalize rejected: empty attachments action=%s chat_id=%s user_id=%s",
            action,
            chat_id,
            user_id,
        )
        await send_text(chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    fio = _extract_fio(msg)
    username = _extract_user_label(msg, user_id)
    report = _caption(action, fio, username)
    report = f"{report}\n📎 Вложений: {len(files)}"

    await send_message(chat_id=WORK_SHIFT_CHAT_ID, text=report, attachments=files)
    await send_text(chat_id, "Ваша заявка сформирована")
    logger.info(
        "work_shift finalize success action=%s chat_id=%s user_id=%s sent_attachments=%s",
        action,
        chat_id,
        user_id,
        len(files),
    )
    _clear_flow(st, user_id, chat_id)
    return True

def _normalize_control_text(text: str) -> str:
    normalized = text.strip().strip("«»\"'").lower()
    return normalized
def _control_text_candidates(text: str, msg: dict) -> List[str]:
    candidates: List[str] = [text]

    callback = msg.get("callback")
    if not isinstance(callback, dict):
        return candidates

    nodes: List[object] = [callback]
    callback_payload = callback.get("payload")
    if isinstance(callback_payload, dict):
        nodes.append(callback_payload)

    for node in nodes:
        if not isinstance(node, dict):
            continue
        for key in ("payload", "data", "value", "command", "action", "text"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value)

    return candidates

async def try_handle_work_shift_step(st: WorkShiftState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow_user_id = _resolve_flow_user(st, user_id, chat_id)
    if flow_user_id is None:
        return False

    is_start_flow = flow_user_id in st.wait_files_start
    raw_candidates = [
        candidate
        for candidate in _control_text_candidates(text, msg)
        if isinstance(candidate, str) and candidate.strip()
    ]
    normalized_candidates = {_normalize_control_text(candidate) for candidate in raw_candidates}
    callback_data = msg.get("callback")
    logger.info(
        "work_shift step chat_id=%s user_id=%s flow_user_id=%s is_start_flow=%s text=%r raw_candidates=%s normalized_candidates=%s callback=%s",
        chat_id,
        user_id,
        flow_user_id,
        is_start_flow,
        text,
        raw_candidates,
        sorted(normalized_candidates),
        _safe_dump(callback_data),
    )
    if normalized_candidates & {"выход", "work_shift_exit"}:
        logger.info(
            "work_shift exit requested chat_id=%s user_id=%s flow_user_id=%s",
            chat_id,
            user_id,
            flow_user_id,
        )
        _clear_flow(st, flow_user_id, chat_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    if normalized_candidates & {"готово", "work_shift_done"}:
        logger.info(
            "work_shift done requested chat_id=%s user_id=%s flow_user_id=%s is_start_flow=%s",
            chat_id,
            user_id,
            flow_user_id,
            is_start_flow,
        )
        if is_start_flow:
            return await _finalize(st, flow_user_id, chat_id, msg, "Начало смены")
        return await _finalize(st, flow_user_id, chat_id, msg, "Окончание смены")

    is_callback_event = isinstance(msg.get("callback"), dict)
    attachments = _extract_attachments(msg, include_nested=not is_callback_event)
    logger.info(
        "work_shift attachments parsed chat_id=%s user_id=%s flow_user_id=%s is_callback_event=%s extracted=%s",
        chat_id,
        user_id,
        flow_user_id,
        is_callback_event,
        len(attachments),
    )
    if attachments:
        files = st.files_by_user.setdefault(flow_user_id, [])
        seen_keys = st.seen_file_keys_by_user.setdefault(flow_user_id, set())

        new_files: List[dict] = []
        for attachment in attachments:
            key = _attachment_key(attachment)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            new_files.append(attachment)

        if new_files:
            files.extend(new_files)
            logger.info(
                "work_shift attachments added chat_id=%s user_id=%s flow_user_id=%s added=%s total=%s",
                chat_id,
                user_id,
                flow_user_id,
                len(new_files),
                len(files),
            )
            await send_text(chat_id, f"Файлов добавлено: {len(new_files)}. Текущее количество: {len(files)}")
        else:
            logger.info(
                "work_shift duplicate attachments ignored chat_id=%s user_id=%s flow_user_id=%s total=%s",
                chat_id,
                user_id,
                flow_user_id,
                len(files),
            )
            await send_text(chat_id, f"Этот файл уже добавлен. Текущее количество: {len(files)}")
        return True
    logger.warning(
        "work_shift fallback prompt chat_id=%s user_id=%s flow_user_id=%s text=%r candidates=%s callback=%s msg_keys=%s msg=%s",
        chat_id,
        user_id,
        flow_user_id,
        text,
        sorted(normalized_candidates),
        _safe_dump(msg.get("callback")),
        sorted(list(msg.keys())),
        _safe_dump(msg),
    )
    await _send_work_shift_prompt(chat_id)
    return True
