from typing import Union, Tuple
from aiogram import types
import aiohttp
from aiogram import types, Dispatcher
from app.config import URL_GET_USER_ID_BY_FIO


def shorten_name(full_name: str) -> str:
    """
    Преобразует строку «Фамилия Имя Отчество» (или более длинную)
    в «Фамилия И.О.» (сразу без пробела между инициалами).
    """
    parts = full_name.strip().split()
    if not parts:
        return ""
    # Первая часть — фамилия
    last_name = parts[0]
    # Берём первые буквы всех остальных частей
    initials = [p[0].upper() + '.' for p in parts[1:]]
    # Склеиваем инициалы без пробелов: ['Д.', 'Г.'] → 'Д.Г.'
    initials_str = "".join(initials)
    return f"{last_name} {initials_str}"

def resolve_event(event: Union[types.Message, types.CallbackQuery]) -> Tuple[types.Message, types.User]:
    if isinstance(event, types.CallbackQuery):
        return event.message, event.from_user
    return event, event.from_user

async def _get_chat_id_by_fio(session: aiohttp.ClientSession, fio: str) -> int:
    try:
        async with session.get(URL_GET_USER_ID_BY_FIO, params={"full_name": fio}, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            chat_id = data.get("chat_id")
            if chat_id:
                return int(chat_id)
    except Exception as e:
        print("Не удалось получить chat_id по ФИО %r: %s", fio, e)
    return None
