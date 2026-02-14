# app/handlers/registration.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Set

from app.utils.max_api import send_text
from app.utils.helper import normalize_phone, post_registration


@dataclass
class RegistrationState:
    wait_phone_users: Set[int]


def cmd_registration(st: RegistrationState, user_id: int, chat_id: int) -> None:
    st.wait_phone_users.add(user_id)
    send_text(chat_id, "Введите номер телефона в формате 7XXXXXXXXXX (11 цифр).")


def try_handle_phone_step(st: RegistrationState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    """
    True  -> сообщение обработано как шаг регистрации
    False -> это не шаг регистрации, пусть роутер обрабатывает дальше
    """
    if user_id not in st.wait_phone_users:
        return False

    phone = normalize_phone(text.strip())
    if not phone:
        send_text(chat_id, "Некорректный формат. Введите номер в формате 7XXXXXXXXXX.")
        return True

    sender = msg.get("sender") or {}
    first_name = sender.get("first_name") or ""
    last_name = sender.get("last_name") or ""

    err = post_registration(
        phone=phone,
        max_user_id=user_id,
        max_chat_id=chat_id,
    )
    if err:
        send_text(chat_id, f"❌ Ошибка регистрации: {err}")
        return True

    st.wait_phone_users.discard(user_id)
    send_text(chat_id, "✅ Регистрация выполнена")
    return True
