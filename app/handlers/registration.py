from __future__ import annotations

from dataclasses import dataclass
from typing import Set

from app.utils.max_api import send_text
from app.utils.helper import normalize_phone, post_registration_async


@dataclass
class RegistrationState:
    wait_phone_users: Set[int]


async def cmd_registration(st: RegistrationState, user_id: int, chat_id: int) -> None:
    st.wait_phone_users.add(user_id)
    await send_text(chat_id, "Введите номер телефона в формате 7XXXXXXXXXX (11 цифр).")


async def try_handle_phone_step(st: RegistrationState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    """
    True  -> сообщение обработано как шаг регистрации
    False -> это не шаг регистрации, пусть роутер обрабатывает дальше
    """
    if user_id not in st.wait_phone_users:
        return False

    phone = normalize_phone(text.strip())
    if not phone:
        await send_text(chat_id, "Некорректный формат. Введите номер в формате 7XXXXXXXXXX.")
        return True

    err = await post_registration_async(
        phone=phone,
        max_user_id=user_id,
        max_chat_id=chat_id,
    )
    if err:
        await send_text(chat_id, f"❌ Ошибка регистрации: {err}")
        return True

    st.wait_phone_users.discard(user_id)
    await send_text(chat_id, "✅ Регистрация выполнена")
    return True
