import logging
import requests
from datetime import datetime, date, timedelta
from typing import Union, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md
from app.keyboards import kb_fill_chart_start

from calendar import monthrange
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.utils.script import resolve_event, shorten_name
from app.config import TOKEN_BOT, URL_TASK_FIO
from app.states import FillStates
from app.keyboards import (
    build_dates_kb,
    build_fix_dates_kb,
    build_confirmation_kb_fill,
)
from app.utils.gsheets import (
    get_available_dates_for_fio,
    get_upcoming_shifts_for_fio,
    fetch_dates_availability,
    vvod_grafica,
    weeks_below_minimum,
    get_shifts_grouped_by_week,
    get_available_dates_for_fio_add_shift,
    tag_exists_in_schedule,
)

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

def _half_bounds(today: date):
    y, m, d = today.year, today.month, today.day

    last = monthrange(y, m)[1]

    def next_month(y, m):
        return (y + 1, 1) if m == 12 else (y, m + 1)

    def prev_month(y, m):
        return (y - 1, 12) if m == 1 else (y, m - 1)

    # 10–25 текущего месяца
    p1_start = date(y, m, 10)
    p1_end   = date(y, m, 25)

    # 26–последний текущего месяца
    p2_start = date(y, m, 26)
    p2_end   = date(y, m, last)

    # 1–9 следующего месяца
    ny, nm = next_month(y, m)
    p2_next_start = date(ny, nm, 1)
    p2_next_end   = date(ny, nm, 9)

    # полный период 26–9
    p26_9_start = p2_start
    p26_9_end   = p2_next_end

    # 10–25 следующего месяца (для "следующего" периода после 26-го числа)
    p1_next_start = date(ny, nm, 10)
    p1_next_end   = date(ny, nm, 25)

    if 10 <= d <= 25:
        # Текущий: 10–25, следующий: 26–9
        cur_start = p1_start
        cur_end   = p1_end
        next_start = p26_9_start
        next_end   = p26_9_end

    elif d >= 26:
        # Текущий: 26–9, следующий: 10–25 следующего месяца
        cur_start = p26_9_start
        cur_end   = p26_9_end
        next_start = p1_next_start
        next_end   = p1_next_end

    else:  # 1–9
        # Текущий: хвост предыдущего 26–9, следующий: 10–25 текущего месяца
        py, pm = prev_month(y, m)
        last_prev = monthrange(py, pm)[1]
        cur_start = date(py, pm, 26)
        cur_end   = date(y, m, 9)

        next_start = p1_start
        next_end   = p1_end

    return cur_start, cur_end, next_start, next_end

def _fmt_range(a: date, b: date) -> str:
    return f"{a.strftime('%d.%m')}-{b.strftime('%d.%m')}"


def _extract_mention_from_message(msg: types.Message) -> Optional[str]:
    if not msg or not msg.entities:
        return None
    for ent in msg.entities:
        if ent.type == "mention":
            return msg.text[ent.offset: ent.offset + ent.length]
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Старт «Заполнить график» с логикой периодов:
# 1) Если в текущем периоде у пользователя НЕТ записей — предлагаем текущий.
# 2) Иначе — сразу предлагаем следующий период.
# ──────────────────────────────────────────────────────────────────────────────
async def cmd_fill_chart(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
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

    # Проверка регистрации (как было)
    try:
        resp = requests.get(URL_TASK_FIO, data={"chat_id": str(actor.id)})
        resp.raise_for_status()
        user = resp.json().get("user", {})
        fullname = user.get("fullname")
        if not fullname:
            raise ValueError
        fio = fullname
    except Exception:
        return await message.answer(
            escape_md("⚠️ Похоже, вы ещё не зарегистрированы. Пройдите регистрацию: /registration")
        )

    username = actor.username or ""
    if not tag_exists_in_schedule(username):
        return await message.answer(
            escape_md("⚠️ Похоже, вы ещё не зарегистрированы (Google Sheets). Пройдите регистрацию: /registration")
        )
    logger.info(f"[fill_chart] User @{actor.username} ({actor.id}) начал заполнение графика")
    await state.finish()

    # Диапазоны текущей/следующей половины
    cur_start, cur_end, next_start, next_end = _half_bounds(date.today())
    label_cur  = _fmt_range(cur_start, cur_end)
    label_next = _fmt_range(next_start, next_end)

    # Сохраним базовые данные и подписи периодов
    username = actor.username

    if not username:
        username = _extract_mention_from_message(message)

    # Сохраним базовые данные и подписи периодов
    await state.update_data(
        fio = fio,
        username = username,
        label_cur = label_cur,
        label_next = label_next
    )

    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(f"Текущий период: {label_cur}", callback_data="pick_period:current"),
        InlineKeyboardButton(f"Следующий период: {label_next}", callback_data="pick_period:next"),
        InlineKeyboardButton("Выход", callback_data="exit")
    )
    await message.answer("На какой период сделать запись?", reply_markup=kb)
    await FillStates.WAIT_PERIOD.set()

async def on_period_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    choice = cb.data.split(":", 1)[1]  # "current" | "next"

    data = await state.get_data()
    fio = data["fio"]
    label_cur  = data.get("label_cur", "")
    label_next = data.get("label_next", "")

    # 1) Получаем список дат по выбранному периоду
    if choice == "current":
        # Даты от сегодня до конца текущей половины месяца (включая сегодня)
        cur = get_available_dates_for_fio_add_shift(fio)
        candidate_dates = cur.get("available", [])
        period_label = f"текущего периода ({label_cur})"
    else:
        # «Следующая половина» (1–15 следующего месяца или 16–конец текущего)
        next_map = get_available_dates_for_fio(fio)
        _, candidate_dates = next(iter(next_map.items()), (None, []))
        period_label = f"следующего периода ({label_next})"

    # 2) Убираем уже записанные пользователем будущие даты
    upcoming = get_upcoming_shifts_for_fio(fio)
    busy_dates = {dt for dt, _ in upcoming}
    filtered = [d for d in candidate_dates if d not in busy_dates]

    if not filtered:
        # Предложим сменить период
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(f"Текущий: {label_cur}", callback_data="pick_period:current"),
            InlineKeyboardButton(f"Следующий: {label_next}", callback_data="pick_period:next"),
            InlineKeyboardButton("Выход", callback_data="exit")
        )
        await cb.message.edit_text(
            escape_md("❌ В выбранном периоде нет свободных дат. Выберите другой период:"),
            reply_markup=kb
        )
        await FillStates.WAIT_PERIOD.set()
        return

    # 3) Проверяем вместимость по каждой дате (есть ли места)
    availability = fetch_dates_availability(filtered)
    free_dates = [d for d in filtered if availability.get(d)]

    if filtered and not free_dates:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(f"Текущий: {label_cur}", callback_data="pick_period:current"),
            InlineKeyboardButton(f"Следующий: {label_next}", callback_data="pick_period:next"),
            InlineKeyboardButton("Выход", callback_data="exit")
        )
        await cb.message.edit_text(
            escape_md("❌ На даты выбранного периода все места уже заняты. Выберите другой период:"),
            reply_markup=kb
        )
        await FillStates.WAIT_PERIOD.set()
        return

    # 4) Отрисовываем выбор дат
    await state.update_data(
        dates=free_dates,
        picked=[],
        period_label=period_label
    )
    kb = build_dates_kb(free_dates, set())
    await cb.message.edit_text(
        escape_md(f"📅 Выберите даты для записи ({period_label}):"),
        reply_markup=kb
    )
    await FillStates.WAIT_DATES.set()

# ── Выбор даты (toggle) ──
async def on_date_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    dates: list[str] = data["dates"]
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

# ── Готово на экране дат: переходим к выбору локации или к итогу ──
async def on_exit(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    picked_dates: list[str] = data.get("picked", [])
    if not picked_dates:
        await cb.message.answer("Вы не выбрали ни одной даты")
        await state.finish()
        return

    await state.update_data(picked_dates=picked_dates)
    await show_summary(cb, state)

# ── Итоговый экран ──
async def show_summary(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    dates = data.get("picked_dates", [])
    dates_sorted = sorted(dates, key=lambda s: datetime.strptime(s, "%d.%m.%Y"))

    text = (
        "Проверьте, пожалуйста, ваши данные:\n\n"
        f"📅 Даты: {', '.join(dates_sorted)}"
    )
    await cb.message.edit_text(escape_md(text), reply_markup=build_confirmation_kb_fill())
    await FillStates.WAIT_CONFIRMATION.set()

# ── «Всё ок» ──
async def on_confirm_yes(cb: types.CallbackQuery, state: FSMContext):
    from app.handlers.menu import back_to_menu_kb
    await cb.message.edit_text("График заполнен")

    data = await state.get_data()
    fio = data.get("fio", "")
    username = data.get("username", "")
    picked_dates = data.get("picked_dates", [])

    # Локации больше не используем
    picked_locs: list[str] = []
    location = "1"  # в таблицу пишем "1"

    # Сохраняем график
    vvod_grafica(fio, username, picked_dates, picked_locs, location)

    # Предупреждение по неделям с <3 смен
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
        await bot.send_message(cb.from_user.id, escape_md(text_warn), parse_mode=types.ParseMode.MARKDOWN_V2, reply_markup=kb_fill_chart_start())

    # Полный график с текущей даты — показываем только даты
    shifts = get_upcoming_shifts_for_fio(fio)
    if shifts:
        shifts_sorted = sorted(shifts, key=lambda t: datetime.strptime(t[0], "%d.%m.%Y"))
        lines = [dt for dt, _ in shifts_sorted]
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

# ── «Есть ошибка» → режим удаления дат ──
async def on_confirm_fix(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    chosen = data.get("picked_dates", [])
    await state.update_data(fix_remove=set())  # множество индексов для удаления
    kb = build_fix_dates_kb(chosen, set())
    await cb.message.edit_text(
        "Выбери даты, которые ошибочны:",
        reply_markup=kb
    )
    # остаёмся в FillStates.WAIT_CONFIRMATION

# Переключение пометки даты на удаление
async def on_fix_toggle(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    chosen = data.get("picked_dates", [])
    to_remove: set[int] = set(data.get("fix_remove", []))

    idx = int(cb.data.split(":", 1)[1])
    if idx in to_remove:
        to_remove.remove(idx)
    else:
        to_remove.add(idx)

    await state.update_data(fix_remove=list(to_remove))
    kb = build_fix_dates_kb(chosen, to_remove)
    await cb.message.edit_reply_markup(reply_markup=kb)

# Применить удаление выбранных дат
async def on_fix_apply(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    dates_all: list[str] = data.get("dates", [])
    chosen: list[str] = data.get("picked_dates", [])
    to_remove: set[int] = set(data.get("fix_remove", []))

    if to_remove:
        new_chosen = [d for i, d in enumerate(chosen) if i not in to_remove]
    else:
        new_chosen = chosen

    # сохраняем и показываем снова экран выбора дат,
    # чтобы можно было добрать/изменить выбор
    await state.update_data(
        picked=new_chosen,
        picked_dates=new_chosen,
        fix_remove=[]
    )
    selected_set = set(new_chosen)
    kb = build_dates_kb(dates_all, selected_set)
    period_label = data.get("period_label", "")
    await cb.message.edit_text(
        escape_md(f"📅 Проверьте/измените выбор дат ({period_label}). Нажмите «Готово», когда закончите:"),
        reply_markup=kb
    )
    await FillStates.WAIT_DATES.set()

# Назад к итогу без изменений
async def on_confirm_back(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    # просто перерисовываем итог
    await show_summary(cb, state)

# ── «Выход» с экрана подтверждения ──
async def on_exit_confirm(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    from app.handlers.menu import back_to_menu_kb
    await cb.message.edit_text("Оформление завершено", reply_markup=back_to_menu_kb())
    await state.finish()

# ──────────────────────────────────────────────────────────────────────────────
# Регистрация хэндлеров
# ──────────────────────────────────────────────────────────────────────────────
def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_fill_chart, commands=["fill_chart"], state="*")
    dp.register_callback_query_handler(cmd_fill_chart, lambda cb: cb.data == "fill:start", state="*")
    # выбор дат
    dp.register_callback_query_handler(
        on_date_selected, lambda cb: cb.data.startswith('select_date:'), state=FillStates.WAIT_DATES,
    )
    dp.register_callback_query_handler(
        on_exit, lambda cb: cb.data == 'exit', state=FillStates.WAIT_DATES,
    )
    # выбор периода
    dp.register_callback_query_handler(
        on_period_selected, lambda cb: cb.data.startswith('pick_period:'), state=FillStates.WAIT_PERIOD,
    )
    # подтверждение / исправление
    dp.register_callback_query_handler(
        on_confirm_yes, lambda cb: cb.data == 'confirm:yes', state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_confirm_fix, lambda cb: cb.data == 'confirm:fix', state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_fix_toggle, lambda cb: cb.data.startswith('fix_date:'), state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_fix_apply, lambda cb: cb.data == 'fix_dates:apply', state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_confirm_back, lambda cb: cb.data == 'confirm:back', state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_exit_confirm, lambda cb: cb.data == "exit", state=FillStates.WAIT_CONFIRMATION,
    )
    dp.register_callback_query_handler(
        on_exit_confirm, lambda cb: cb.data == "exit", state=FillStates.WAIT_PERIOD,
    )
