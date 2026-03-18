from typing import Union, Tuple
from aiogram import types, Dispatcher

def resolve_event(event: Union[types.Message, types.CallbackQuery]) -> Tuple[types.Message, types.User]:
    if isinstance(event, types.CallbackQuery):
        return event.message, event.from_user
    return event, event.from_user
