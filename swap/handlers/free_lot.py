import logging
from datetime import date, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.markdown import escape_md

from app.states import FreeLotStates
from app.config import TOKEN_BOT, CHAT_ID_GRAPH, THREAD_ID_GRAPH

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

async def cmd_free_lot(message: types.Message, state: FSMContext):
    # Команда доступна только в личном чате
    if message.chat.type != 'private':
        return await message.answer(
            escape_md("Команда доступна только в личном чате"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )

    # Сброс предыдущего состояния
    await state.finish()

    # Генерируем даты: сегодня + 14 дней
    today = date.today()
    dates = [today + timedelta(days=i) for i in range(15)]
    await state.update_data(free_lot_dates=dates)

    # Строим inline-кнопки для каждой даты
    kb = InlineKeyboardMarkup(row_width=5)
    for d in dates:
        label = d.strftime("%d.%m")
        cb_data = f"free_date:{d.isoformat()}"
        kb.insert(InlineKeyboardButton(text=label, callback_data=cb_data))
    # Добавляем кнопку "Выход"
    kb.add(InlineKeyboardButton(text="❌ Выход", callback_data="free_exit"))

    # Отправляем сообщение с клавиатурой
    await message.answer(
        escape_md("📅 Выберите дату свободного места:"),
        parse_mode=types.ParseMode.MARKDOWN_V2,
        reply_markup=kb
    )
    await FreeLotStates.WAIT_DATE.set()

async def on_date_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    # Получаем выбранную дату
    d_iso = cb.data.split(":", 1)[1]
    selected_date = date.fromisoformat(d_iso)
    label = selected_date.strftime("%d.%m")

    # Формируем и отправляем сообщение в целевой чат
    text = f"⚠️🆓 Коллеги, имеются свободные места на {label} ⚠️"
    await bot.send_message(
        CHAT_ID_GRAPH,
        escape_md(text),
        parse_mode=types.ParseMode.MARKDOWN_V2,
        message_thread_id=THREAD_ID_GRAPH
    )

    # Сообщение пользователю
    await cb.message.answer(
        escape_md("✅ Сообщение отправлено"),
        parse_mode=types.ParseMode.MARKDOWN_V2
    )

    # Обновляем интерфейс пользователя
    await cb.message.edit_text(
        escape_md(f"Сообщение отправлено для даты {label}"),
        parse_mode=types.ParseMode.MARKDOWN_V2
    )
    await state.finish()

async def on_exit(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.edit_text(
        escape_md("❌ Операция отменена"),
        parse_mode=types.ParseMode.MARKDOWN_V2
    )
    await state.finish()


def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_free_lot, commands=["free_lot"], state="*")
    dp.register_callback_query_handler(
        on_date_selected,
        lambda cb: cb.data.startswith("free_date:"),
        state=FreeLotStates.WAIT_DATE
    )
    dp.register_callback_query_handler(
        on_exit,
        lambda cb: cb.data == "free_exit",
        state=FreeLotStates.WAIT_DATE
    )
