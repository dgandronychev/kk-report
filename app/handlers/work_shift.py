from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
#
from app.config import WORK_SHIFT_CHAT_ID
from app.utils.max_api import send_message, send_text, send_text_with_reply_buttons

@dataclass
class WorkShiftState:
    wait_files_start: set[int] = field(default_factory=set)
    wait_files_end: set[int] = field(default_factory=set)
    files_by_user: Dict[int, List[dict]] = field(default_factory=dict)
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
        files.append({"type": f_type, "payload": item.get("payload")})
    return files


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
    st.active_user_by_chat[chat_id] = user_id
    await _send_work_shift_prompt(chat_id)


async def cmd_end_work_shift(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    old_user_id = st.active_user_by_chat.get(chat_id)
    if old_user_id is not None and old_user_id != user_id:
        _clear_flow(st, old_user_id, chat_id)
    st.wait_files_start.discard(user_id)
    st.wait_files_end.add(user_id)
    st.files_by_user[user_id] = []
    st.active_user_by_chat[chat_id] = user_id
    await _send_work_shift_prompt(chat_id)


async def _finalize(st: WorkShiftState, user_id: int, chat_id: int, msg: dict, action: str) -> bool:
    files = st.files_by_user.get(user_id, [])
    if not files:
        await send_text(chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    fio = _extract_fio(msg)
    username = _extract_user_label(msg, user_id)
    report = _caption(action, fio, username)
    report = f"{report}\n📎 Вложений: {len(files)}"

    await send_message(chat_id=WORK_SHIFT_CHAT_ID, text=report, attachments=files)
    await send_text(chat_id, "Ваша заявка сформирована")

    _clear_flow(st, user_id, chat_id)
    return True

def _normalize_control_text(text: str) -> str:
    normalized = text.strip().strip("«»\"'").lower()
    return normalized


async def try_handle_work_shift_step(st: WorkShiftState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow_user_id = _resolve_flow_user(st, user_id, chat_id)
    if flow_user_id is None:
        return False

    is_start_flow = flow_user_id in st.wait_files_start
    clean_text = text.strip()
    normalized_text = _normalize_control_text(clean_text)

    if normalized_text in {"выход", "work_shift_exit"}:
        _clear_flow(st, flow_user_id, chat_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    if normalized_text in {"готово", "work_shift_done"}:
        if is_start_flow:
            return await _finalize(st, flow_user_id, chat_id, msg, "Начало смены")
        return await _finalize(st, flow_user_id, chat_id, msg, "Окончание смены")

    is_callback_event = isinstance(msg.get("callback"), dict)
    attachments = _extract_attachments(msg, include_nested=not is_callback_event)

    if attachments:
        files = st.files_by_user.setdefault(flow_user_id, [])
        files.extend(attachments)
        await send_text(chat_id, f"Файлов добавлено: {len(attachments)}. Текущее количество: {len(files)}")
        return True

    await _send_work_shift_prompt(chat_id)
    return True
