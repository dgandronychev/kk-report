# app/handlers/menu.py
import logging
from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from contextlib import suppress
from aiogram.utils.exceptions import MessageCantBeDeleted, MessageToDeleteNotFound

logger = logging.getLogger(__name__)

# --- Главное меню ---
main_menu_inline = InlineKeyboardMarkup(row_width=2)
main_menu_inline.add(
    InlineKeyboardButton("📅 Заполнить график", callback_data="menu_fill_chart"),
    InlineKeyboardButton("➕ Добавить смену", callback_data="menu_add_shift"),
)
main_menu_inline.add(
    InlineKeyboardButton("❌ Отменить смену", callback_data="menu_remove_shift"),
    InlineKeyboardButton("📊 Показать график", callback_data="menu_show_chart"),
)
main_menu_inline.add(
    InlineKeyboardButton("⚠ Сообщить о проблеме", callback_data="menu_problem")
)

# Кнопка "Назад в меню"
def back_to_menu_kb():
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton("⬅ Назад в меню", callback_data="menu_main")
    )

# --- Показ меню ---
async def menu_cmd(message: types.Message):
    await message.answer("Выберите действие:", reply_markup=main_menu_inline)

async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.finish()  # сбрасываем состояние
    await call.message.edit_text("Выберите действие:", reply_markup=main_menu_inline)


async def process_menu_callbacks(call: types.CallbackQuery, state: FSMContext):
    await call.answer()

    with suppress(MessageCantBeDeleted, MessageToDeleteNotFound, Exception):
        await call.message.delete()

    if call.data == "menu_show_chart":
        from app.handlers.show_chart import cmd_show_chart
        await cmd_show_chart(call, state)

    elif call.data == "menu_add_shift":
        from app.handlers.add_shift import cmd_add_shift
        await cmd_add_shift(call, state)

    elif call.data == "menu_remove_shift":
        from app.handlers.remove_shift import cmd_cancel_shift
        await cmd_cancel_shift(call, state)

    elif call.data == "menu_fill_chart":
        from app.handlers.fill_chart import cmd_fill_chart
        await cmd_fill_chart(call, state)

    elif call.data == "menu_problem":
        from app.handlers.problem import cmd_problem
        await cmd_problem(call, state)

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(menu_cmd, commands=["start", "menu"], state="*")
    dp.register_callback_query_handler(back_to_menu, lambda c: c.data == "menu_main", state="*")
    dp.register_callback_query_handler(process_menu_callbacks, lambda c: c.data.startswith("menu_"), state="*")