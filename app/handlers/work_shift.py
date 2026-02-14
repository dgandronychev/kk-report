from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List

from app.config import WORK_SHIFT_CHAT_ID
from app.utils.max_api import send_message, send_text

@dataclass
class WorkShiftState:
    wait_files_start: set[int] = field(default_factory=set)
    wait_files_end: set[int] = field(default_factory=set)
    files_by_user: Dict[int, List[dict]] = field(default_factory=dict)


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


def _extract_attachments(msg: dict) -> List[dict]:
    attachments = msg.get("attachments")
    if not isinstance(attachments, list):
        body = msg.get("body")
        if isinstance(body, dict):
            attachments = body.get("attachments")

    if not isinstance(attachments, list):
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


def cmd_start_job_shift(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    st.wait_files_end.discard(user_id)
    st.wait_files_start.add(user_id)
    st.files_by_user[user_id] = []
    send_text(chat_id, "Прикрепите фото/видео/файл и нажмите 'Готово'. Для отмены напишите 'Выход'.")


def cmd_end_work_shift(st: WorkShiftState, user_id: int, chat_id: int) -> None:
    st.wait_files_start.discard(user_id)
    st.wait_files_end.add(user_id)
    st.files_by_user[user_id] = []
    send_text(chat_id, "Прикрепите фото/видео/файл и нажмите 'Готово'. Для отмены напишите 'Выход'.")


def _finalize(st: WorkShiftState, user_id: int, chat_id: int, msg: dict, action: str) -> bool:
    files = st.files_by_user.get(user_id, [])
    if not files:
        send_text(chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    fio = _extract_fio(msg)
    username = _extract_user_label(msg, user_id)
    report = _caption(action, fio, username)
    report = f"{report}\n📎 Вложений: {len(files)}"

    send_message(chat_id=WORK_SHIFT_CHAT_ID, text=report, attachments=files)
    send_text(chat_id, "Ваша заявка сформирована")

    st.wait_files_start.discard(user_id)
    st.wait_files_end.discard(user_id)
    st.files_by_user.pop(user_id, None)
    return True


def try_handle_work_shift_step(st: WorkShiftState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    is_start_flow = user_id in st.wait_files_start
    is_end_flow = user_id in st.wait_files_end
    if not is_start_flow and not is_end_flow:
        return False

    clean_text = text.strip()

    if clean_text == "Выход":
        st.wait_files_start.discard(user_id)
        st.wait_files_end.discard(user_id)
        st.files_by_user.pop(user_id, None)
        send_text(chat_id, "Оформление заявки отменено")
        return True

    attachments = _extract_attachments(msg)
    if attachments:
        files = st.files_by_user.setdefault(user_id, [])
        files.extend(attachments)
        send_text(chat_id, f"Файлов добавлено: {len(attachments)}. Текущее количество: {len(files)}")
        return True

    if clean_text == "Готово":
        if is_start_flow:
            return _finalize(st, user_id, chat_id, msg, "Начало смены")
        return _finalize(st, user_id, chat_id, msg, "Окончание смены")

    send_text(chat_id, "Прикрепите файл, либо нажмите 'Готово' или 'Выход'.")
    return True
