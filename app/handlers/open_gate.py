from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from typing import Dict

from app.config import SBORKA_CHAT_ID_CITY
from app.utils.gsheets import find_logistics_rows, write_open_gate_row
from app.utils.helper import get_fio_async
from app.utils.max_api import send_message, send_text, send_text_with_reply_buttons

logger = logging.getLogger(__name__)


@dataclass
class OpenGateState:
    waiting_confirm_users: set[int] = field(default_factory=set)
    company_by_user: Dict[int, str] = field(default_factory=dict)

def reset_open_gate_progress(st: OpenGateState, user_id: int) -> None:
    st.waiting_confirm_users.discard(user_id)
    st.company_by_user.pop(user_id, None)

async def cmd_open_gate(st: OpenGateState, user_id: int, chat_id: int, msg: dict) -> None:
    st.waiting_confirm_users.add(user_id)
    st.company_by_user[user_id] = "СитиДрайв"

    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=(
            f"ФИО: {fio}\n"
            "Подтвердите открытие ворот склада"
        ),
        button_texts=["Подтвердить открытие", "Выход"],
        button_payloads=["open_gate_confirm", "open_gate_exit"],
    )


async def try_handle_open_gate_step(st: OpenGateState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    if user_id not in st.waiting_confirm_users:
        return False

    callback = msg.get("callback") if isinstance(msg.get("callback"), dict) else {}
    callback_payload = callback.get("payload") if isinstance(callback.get("payload"), dict) else {}

    control_values = [text]
    for key in ("payload", "data", "value", "command", "action", "text"):
        value = callback_payload.get(key)
        if isinstance(value, str):
            control_values.append(value)

    normalized_candidates = {str(value).strip().lower() for value in control_values if str(value).strip()}

    if not normalized_candidates:
        await send_text(chat_id, "Выберите действие: Подтвердить открытие / Выход")
        return True

    if {"выход", "open_gate_exit"} & normalized_candidates:
        st.waiting_confirm_users.discard(user_id)
        st.company_by_user.pop(user_id, None)
        await send_text(chat_id, "Операция отменена")
        return True

    if not ({"подтвердить открытие", "open_gate_confirm"} & normalized_candidates):
        await send_text(chat_id, "Выберите действие: Подтвердить открытие / Выход")
        return True

    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    company = st.company_by_user.get(user_id, "")
    plate = ""

    tags, fios = find_logistics_rows()
    logist = ""
    if fios:
        logist = " , ".join(f"{name} ({tag})" for name, tag in zip(fios, tags))

    now_msk = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    send_text_value = (
        "#Открытие_Склада\n\n"
        f"{now_msk}\n"
        f"ФИО: {fio}\n"
        "Откройте, пожалуйста, ворота"
    )
    if logist:
        send_text_value += f"\n{logist}"

    await send_message(chat_id=int(SBORKA_CHAT_ID_CITY), text=send_text_value)

    try:
        write_open_gate_row(
            fio=fio,
            car_plate=plate,
            company=company,
        )
    except Exception:
        logger.exception("Ошибка отправки в write_open_gate_row")

    st.waiting_confirm_users.discard(user_id)
    st.company_by_user.pop(user_id, None)
    await send_text(chat_id, f"Сообщение отправлено логисту {logist}" if logist else "Сообщение отправлено логисту")
    return True
