import logging
import requests
from datetime import date

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md
from typing import Union

from app.config import TOKEN_BOT, URL_TASK_FIO
from app.utils.script import shorten_name
from app.utils.gsheets import get_upcoming_shifts_for_fio, weeks_below_minimum, get_shifts_grouped_by_week, tag_exists_in_schedule
from app.utils.script import resolve_event
from app.keyboards import kb_fill_chart_start

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

#############################################
async def cmd_show_chart(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    message, actor = resolve_event(event)
    if message.chat.type != 'private':
        return await message.answer(
            escape_md("Эта команда доступна только в личных сообщениях с ботом"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )

    # Проверяем регистрацию пользователя
    try:
        resp = requests.get(URL_TASK_FIO, data={"chat_id": str(actor.id)})
        resp.raise_for_status()
        user = resp.json().get("user", {})
        fullname = user.get("fullname")
        if not fullname:
            raise ValueError("Не зарегистрирован")
        fio = fullname
    except Exception:
        return await message.answer(
            escape_md("⚠️ Похоже, вы ещё не зарегистрированы. Пожалуйста, пройдите регистрацию командой /registration"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )

    username = actor.username or ""
    if not tag_exists_in_schedule(username):
        return await message.answer(
            escape_md("⚠️ Похоже, вы ещё не зарегистрированы (Google Sheets). Пройдите регистрацию: /registration")
        )

    logger.info(f"[show_chart] User @{actor.username} ({actor.id}) начал показ графика")
    await state.finish()
    await state.update_data(user_id=actor.id, username=actor.username, fullname=fullname, fio=fio)

    shifts = get_upcoming_shifts_for_fio(fio)

    if not shifts:
        text = f"Для {fio} нет записей с датой выхода от сегодня"
    else:
        # Показываем только даты (вне зависимости от наличия/формата локаций)
        only_dates = []
        for item in shifts:
            if isinstance(item, (list, tuple)) and item:
                only_dates.append(item[0])
            else:
                only_dates.append(str(item))
        text = "📅 Предстоящие даты выхода для {}:\n\n  • {}".format(fio, "\n  • ".join(only_dates))

    from app.handlers.menu import back_to_menu_kb
    await message.answer(escape_md(text), parse_mode=types.ParseMode.MARKDOWN_V2, reply_markup=back_to_menu_kb())

    # Предупреждение по неделям с < 3 смен
    weeks_low = weeks_below_minimum(fio)
    if weeks_low:
        warn_lines = []
        for year, week in weeks_low:
            cnt = get_shifts_grouped_by_week(fio)[(year, week)]
            start_of_week = date.fromisocalendar(year, week, 1)  # понедельник
            end_of_week = date.fromisocalendar(year, week, 7)   # воскресенье
            warn_lines.append(
                f"– {year}, неделя {week} "
                f"({start_of_week.strftime('%d.%m.%Y')}–{end_of_week.strftime('%d.%m.%Y')}): {cnt} смен"
            )

        warning = (
            "⚠️ Внимание! В следующих календарных неделях меньше 3-х смен:\n"
            + "\n".join(warn_lines)
            + "\n\nПожалуйста, добавьте ещё смены"
        )
        await message.answer(
            escape_md(warning),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=kb_fill_chart_start(),
        )

#############################################
# Регистрация хэндлеров
#############################################

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_show_chart, commands=["show_chart"], state="*")