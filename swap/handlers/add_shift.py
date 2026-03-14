import logging
import requests
from datetime import datetime, date
from typing import Union, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md
from app.keyboards import kb_fill_chart_start

from app.config import TOKEN_BOT, URL_TASK_FIO, CHAT_ID_GRAPH, THREAD_ID_GRAPH, CHAT_ID_PUSH, THREAD_ID_PUSH_SHIFT
from app.states import AddStates
from app.utils.script import resolve_event, shorten_name
from app.utils.gsheets import (
    get_available_dates_for_fio_add_shift,
    vvod_grafica,
    weeks_below_minimum,
    get_shifts_grouped_by_week,
    fetch_dates_availability,
    get_upcoming_shifts_for_fio,
    tag_exists_in_schedule,
)
from app.keyboards import (
    build_dates_kb,
    build_fix_dates_kb,
    build_confirmation_kb_fill,   # подтверждение без «Изменить локации»
)

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

def _extract_mention_from_message(msg: types.Message) -> Optional[str]:
    if not msg or not msg.entities:
        return None
    for ent in msg.entities:
        if ent.type == "mention":
            return msg.text[ent.offset: ent.offset + ent.length]
    return None

#############################################
# Старт «Добавить смену»
#############################################
async def cmd_add_shift(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    if isinstance(event, types.CallbackQuery):
        message = event.message
        actor = event.from_user
    else:
        message = event
        actor = event.from_user

    if message.chat.type != 'private':
        return await message.answer(
            escape_md("Эта команда доступна только в личных сообщениях с ботом"),
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
        )

    username = actor.username or ""
    if not tag_exists_in_schedule(username):
        return await message.answer(
            escape_md("⚠️ Похоже, вы ещё не зарегистрированы (Google Sheets). Пройдите регистрацию: /registration")
        )

    logger.info(f"[add_shift] User @{actor.username} ({actor.id}) начал оформление добавление смены")
    await state.finish()

    username = actor.username

    if not username:
        username = _extract_mention_from_message(message)

    await state.update_data(user_id=actor.id, username=username, fullname=fullname, fio=fio)

    # Получаем свободные и занятые даты ТЕКУЩЕГО периода (для add_shift)
    date_map = get_available_dates_for_fio_add_shift(fio)
    used = date_map.get("used", [])
    available = date_map.get("available", [])

    # Проверяем общую вместимость по дням одним запросом
    availability = fetch_dates_availability(available)
    actual_free = [d for d in available if availability.get(d)]

    if not actual_free:
        return await message.answer(
            escape_md("❌ К сожалению, свободных смен не осталось на доступные даты."),
        )

    # Сохраняем список доступных дат и пустой выбор
    await state.update_data(dates=actual_free, picked=[])

    # Текст для инфо
    if used:
        used_text = "\n".join(f"• {d}" for d in used)
    else:
        used_text = "— Нет занятых дат в этой половине месяца"

    prompt = (
        "📅 Занятые даты в текущей половине месяца:\n"
        f"{used_text}\n\n"
        "✅ Свободные даты — выберите одну или несколько:"
    )

    kb = build_dates_kb(actual_free, set())
    await message.answer(escape_md(prompt), reply_markup=kb)
    await AddStates.WAIT_DATES.set()


#############################################
# Выбор дат
#############################################
async def on_date_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    dates: list[str] = data.get("dates", [])
    picked: set[str] = set(data.get("picked", []))

    idx = int(cb.data.split(":", 1)[1])
    date_str = dates[idx]
    if date_str in picked:
        picked.remove(date_str)
    else:
        picked.add(date_str)

    await state.update_data(picked=list(picked))
    kb = build_dates_kb(dates, picked)
    await cb.message.edit_reply_markup(reply_markup=kb)


async def on_exit(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    picked_dates: list[str] = data.get("picked", [])
    if not picked_dates:
        await cb.message.answer(escape_md("Вы не выбрали ни одной даты"))
        await state.finish()
        return

    # Сохраняем выбранные даты и сразу идём на итог
    await state.update_data(picked_dates=picked_dates)
    await show_summary(cb, state)

#############################################
# Итог + подтверждение
#############################################
async def show_summary(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    dates = data.get("picked_dates", [])

    dates_sorted = sorted(dates, key=lambda s: datetime.strptime(s, "%d.%m.%Y"))
    text = (
        "Проверьте, пожалуйста, ваши данные:\n\n"
        f"📅 Даты: {', '.join(dates_sorted)}"
    )
    await cb.message.edit_text(escape_md(text), reply_markup=build_confirmation_kb_fill())
    await AddStates.WAIT_CONFIRMATION.set()


#############################################
# Подтверждение — «Всё ок»
#############################################
async def on_confirm_yes(cb: types.CallbackQuery, state: FSMContext):
    from app.handlers.menu import back_to_menu_kb
    await cb.message.edit_text("График заполнен", reply_markup=back_to_menu_kb())

    data = await state.get_data()
    fio = data.get("fio", "")
    username = data.get("username", "")
    user_tag = f"@{username}" if username else ""
    picked_dates = data.get("picked_dates", [])

    # Локации больше не используем
    picked_locs: list[str] = []
    location = "1"  # Всегда «1» в Google Sheets

    # 1) Запись в таблицу
    vvod_grafica(fio, username, picked_dates, picked_locs, location)

    # 2) Уведомление в общий чат — показываем только даты
    try:
        dates_sorted = sorted(picked_dates, key=lambda s: datetime.strptime(s, "%d.%m.%Y"))
        lines = [f"{fio} {user_tag} добавил(а) смены:"]
        for dt in dates_sorted:
            lines.append(f"✅ {dt}")
        text = "\n".join(lines)

        await bot.send_message(
            CHAT_ID_PUSH,
            escape_md(text),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            message_thread_id=THREAD_ID_PUSH_SHIFT
        )
    except Exception as e:
        logger.exception("Ошибка отправки уведомления в общий чат: %s", e)

    # 3) Предупреждения по неделям < 3 смен — без изменений
    low = weeks_below_minimum(fio)
    if low:
        text_warn = "⚠️ После добавления у вас в некоторых неделях меньше 3 смен:\n"
        for year, week in low:
            cnt = get_shifts_grouped_by_week(fio)[(year, week)]
            start_of_week = date.fromisocalendar(year, week, 1)
            end_of_week = date.fromisocalendar(year, week, 7)
            text_warn += (
                f"  • {year}, неделя {week} "
                f"({start_of_week.strftime('%d.%m.%Y')}–{end_of_week.strftime('%d.%m.%Y')}): "
                f"{cnt} смен\n"
            )
        await bot.send_message(
            cb.from_user.id,
            escape_md(text_warn),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=kb_fill_chart_start(),
        )
    # 4) Полный график «от сегодня» — показываем только даты
    shifts = get_upcoming_shifts_for_fio(fio)
    if shifts:
        shifts_sorted = sorted(shifts, key=lambda t: datetime.strptime(t[0], "%d.%m.%Y"))
        lines = [dt for dt, _ in shifts_sorted]  # только даты
        msg = "🗓 Ваш график с сегодняшнего дня:\n\n" + "\n".join(lines)
        await bot.send_message(
            cb.from_user.id,
            escape_md(msg),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=back_to_menu_kb()
        )
    else:
        await bot.send_message(
            cb.from_user.id,
            escape_md("Пока нет записей в графике"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=back_to_menu_kb()
        )

    await state.finish()


#############################################
# Подтверждение — «❗ Есть ошибка»
# Показать только выбранные даты, отметить неверные, удалить,
# затем вернуть пользователя к выбору дат (можно добрать) и снова подтвердить.
#############################################
async def on_confirm_fix(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    chosen = data.get("picked_dates", [])
    await state.update_data(fix_remove=[])
    kb = build_fix_dates_kb(chosen, set())
    await cb.message.edit_text(
        "Выбери даты, которые ошибочны:",
        reply_markup=kb
    )
    # остаёмся в AddStates.WAIT_CONFIRMATION


async def on_fix_toggle(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    chosen: list[str] = data.get("picked_dates", [])
    to_remove: set[int] = set(data.get("fix_remove", []))

    idx = int(cb.data.split(":", 1)[1])
    if idx in to_remove:
        to_remove.remove(idx)
    else:
        to_remove.add(idx)

    await state.update_data(fix_remove=list(to_remove))
    kb = build_fix_dates_kb(chosen, to_remove)
    await cb.message.edit_reply_markup(reply_markup=kb)


async def on_fix_apply(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    dates_all: list[str] = data.get("dates", [])
    chosen: list[str] = data.get("picked_dates", [])
    to_remove: set[int] = set(data.get("fix_remove", []))

    # удаляем отмеченные индексы
    new_chosen = [d for i, d in enumerate(chosen) if i not in to_remove]

    # сохраняем и возвращаемся на экран выбора дат — можно добрать/изменить
    await state.update_data(
        picked=new_chosen,
        picked_dates=new_chosen,
        fix_remove=[]
    )
    selected_set = set(new_chosen)
    kb = build_dates_kb(dates_all, selected_set)
    await cb.message.edit_text(
        escape_md("📅 Проверьте/измените выбор дат. Нажмите «Готово», когда закончите:"),
        reply_markup=kb
    )
    await AddStates.WAIT_DATES.set()


async def on_confirm_back(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    await show_summary(cb, state)


#############################################
# «Выход» с экрана подтверждения
#############################################
async def on_exit_confirm(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    from app.handlers.menu import back_to_menu_kb
    await cb.message.edit_text("Оформление завершено", reply_markup=back_to_menu_kb())
    await state.finish()


#############################################
# Регистрация хэндлеров
#############################################
def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_add_shift, commands=["add_shift"], state="*")

    # выбор дат
    dp.register_callback_query_handler(
        on_date_selected,
        lambda cb: cb.data.startswith('select_date:'),
        state=AddStates.WAIT_DATES,
    )
    dp.register_callback_query_handler(
        on_exit,
        lambda cb: cb.data == 'exit',
        state=AddStates.WAIT_DATES,
    )
    # подтверждение / исправление
    dp.register_callback_query_handler(
        on_confirm_yes,
        lambda cb: cb.data == 'confirm:yes',
        state=AddStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_confirm_fix,
        lambda cb: cb.data == 'confirm:fix',
        state=AddStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_fix_toggle,
        lambda cb: cb.data.startswith('fix_date:'),
        state=AddStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_fix_apply,
        lambda cb: cb.data == 'fix_dates:apply',
        state=AddStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_confirm_back,
        lambda cb: cb.data == 'confirm:back',
        state=AddStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_exit_confirm,
        lambda cb: cb.data == "exit",
        state=AddStates.WAIT_CONFIRMATION,
    )
