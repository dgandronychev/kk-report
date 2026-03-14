import logging
import requests

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md

from app.config import TOKEN_BOT, URL_TASK_FIO, CHAT_ID_PUSH, THREAD_ID_PUSH_SHTRAF, THREAD_ID_PUSH_SHIFT
from app.utils.script import shorten_name
from app.states import CancelStates
from app.utils.gsheets import get_upcoming_shifts_for_fio, cancel_shift_in_sheet, log_late_cancel_to_sheet, tag_exists_in_schedule
from app.keyboards import build_cancellation_kb, build_confirm_cancel_kb
from app.config import CHAT_ID_GRAPH, THREAD_ID_GRAPH
from typing import Union

from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from app.utils.script import resolve_event

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)


#############################################
# Хэндлеры для отмены смены
#############################################

async def cmd_cancel_shift(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    message, actor = resolve_event(event)
    if message.chat.type != 'private':
        return await message.answer(escape_md("Эта команда доступна только в личных сообщениях с ботом"),
                                    parse_mode=types.ParseMode.MARKDOWN_V2)

    # Проверка регистрации
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

    logger.info(f"[remove_shift] User @{actor.username} ({actor.id}) начал отмену смены")
    await state.finish()
    await state.update_data(user_id=actor.id, username=actor.username, fullname=fullname, fio=fio)

    shifts = get_upcoming_shifts_for_fio(fio)  # [(date_str, loc), ...]
    if not shifts:
        return await message.answer(escape_md("У вас нет предстоящих смен для отмены"),
                                    parse_mode=types.ParseMode.MARKDOWN_V2)

    # Сохраняем ОРИГИНАЛЬНЫЕ с локациями, но в клавиатуре показываем только даты
    await state.update_data(shifts=shifts, picked=set())
    shifts_view = [(dt, "") for dt, _ in shifts]  # без локаций

    kb = build_cancellation_kb(shifts_view, set())
    await message.answer(escape_md("📅 Выберите смены для отмены:"),
                         parse_mode=types.ParseMode.MARKDOWN_V2, reply_markup=kb)
    await CancelStates.WAIT_SELECTION.set()

async def on_shift_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    shifts: list[tuple[str, str]] = data.get("shifts", [])   # оригинал с локациями
    picked: set[int] = set(data.get("picked", []))

    idx = int(cb.data.split(':', 1)[1])
    if idx in picked:
        picked.remove(idx)
    else:
        picked.add(idx)

    await state.update_data(picked=list(picked))

    # Для UI соберём «вид» без локаций
    shifts_view = [(dt, "") for dt, _ in shifts]
    kb = build_cancellation_kb(shifts_view, picked)
    await cb.message.edit_reply_markup(reply_markup=kb)

async def on_cancel_exit(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    fio = data.get("fio")
    shifts: list[tuple[str, str]] = data.get("shifts", [])
    picked: set[int] = set(data.get("picked", []))

    if not picked:
        await cb.message.answer(escape_md("Вы не выбрали ни одной смены для отмены"),
                                parse_mode=types.ParseMode.MARKDOWN_V2)
        await state.finish()
        return

    # Проверка «< 24 часа» — собираем ИНДЕКСЫ поздних отмен
    moscow_tz = timezone(timedelta(hours=3))
    now_msk = datetime.now(moscow_tz)
    late_indices: list[int] = []
    close_list: list[str] = []  # только даты для показа пользователю

    for idx in picked:
        dt_str, _loc = shifts[idx]
        d = datetime.strptime(dt_str, "%d.%m.%Y").date()
        shift_start = datetime(d.year, d.month, d.day, 22, 0, tzinfo=moscow_tz)
        if (shift_start - now_msk).total_seconds() < 24 * 3600:
            late_indices.append(idx)
            close_list.append(dt_str)

    if close_list:
        lines = [f"• {dt}" for dt in close_list]
        text24 = ("⚠️ *Внимание!* До начала следующей смены осталось менее 24 часов:\n"
                  + "\n".join(lines) + "\n\n")
    else:
        text24 = ""

    # 1) Результат после отмены (для расчётов) — локации не нужны
    remaining = [(dt, loc) for idx, (dt, loc) in enumerate(shifts) if idx not in picked]

    # 2) Группировка по ISO-неделе
    counts = defaultdict(int)
    for dt_str, _ in remaining:
        d = datetime.strptime(dt_str, "%d.%m.%Y").date()
        year, week, _ = d.isocalendar()
        counts[(year, week)] += 1

    # 3) Недели с < 3 смен
    low_weeks = [((y, w), cnt) for (y, w), cnt in counts.items() if cnt < 3]

    # 4) Текст предупреждения
    warning = ""
    if low_weeks:
        lines = []
        for (year, week), cnt in low_weeks:
            start_of_week = date.fromisocalendar(year, week, 1)
            end_of_week = date.fromisocalendar(year, week, 7)
            lines.append(
                f"• {year}, неделя {week} "
                f"({start_of_week.strftime('%d.%m.%Y')}–{end_of_week.strftime('%d.%m.%Y')}): "
                f"останется {cnt} смен"
            )
        warning = "⚠️ Внимание! После отмены в некоторых неделях останется меньше 3 смен:\n" + "\n".join(lines) + "\n\n"

    contract = ""
    if text24 or warning:
        contract = (
            "\n\n"
            "4.1.10. На каждый отчетный период, указанный п.5.2 настоящего Договора "
            "(максимум до 2-х дней до начала периода), предоставлять информацию о "
            "планируемых датах оказания услуг (индивидуальное расписание), при условии "
            "минимального наличия 3-х дней в неделю.\n"
            "\n"
            "Изменение индивидуального расписания возможно, при соответствующем "
            "уведомлении Ответственного исполнителя со стороны Заказчика, минимум за "
            "24 часа до предстоящей даты оказания услуг.\n\n"
        )

    # сохраняем выбранные индексы и СТРОГО late_indices
    await state.update_data(picked_indices=list(picked), late_indices=late_indices, text24=text24)

    # Клавиатура подтверждения
    confirm_kb = build_confirm_cancel_kb()
    final_text = text24 + warning + contract + "❗️ Вы уверены, что хотите отменить выбранные смены?\nЕсли да — нажмите «Да», чтобы подтвердить."
    await cb.message.edit_text(escape_md(final_text),
                               parse_mode=types.ParseMode.MARKDOWN_V2,
                               reply_markup=confirm_kb)
    await CancelStates.WAIT_CONFIRM.set()

async def on_confirm_yes(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    fio = data.get("fio", "")
    username = data.get("username", "")
    user_tag = f"@{username}" if username else ""
    shifts: list[tuple[str, str]] = data.get("shifts", [])
    picked: list[int] = data.get("picked_indices", [])
    late_set = set(data.get("late_indices", []))  # только эти считаются «менее 24 часов»

    # 1) Отмена в таблице (передаём loc как есть, но нигде не показываем)
    for idx in picked:
        date_str, loc = shifts[idx]
        cancel_shift_in_sheet(fio, date_str, loc)

    # 2) Ответ пользователю
    from app.handlers.menu import back_to_menu_kb
    await cb.message.edit_text(escape_md("Смены/а удалены/а"),
                               parse_mode=types.ParseMode.MARKDOWN_V2,
                               reply_markup=back_to_menu_kb())

    # 3) Уведомления в чаты — построчная пометка "< 24 часов"
    lines = [f"{fio} {user_tag} отменил(а) запись на:"]
    for idx in picked:
        date_str, _loc = shifts[idx]
        tag = "менее 24 часов " if idx in late_set else ""
        lines.append(f"❌ {tag}{date_str}")
    text = "\n".join(lines) + "\n\n@SerezhaMatukaytis"

    await bot.send_message(CHAT_ID_PUSH, escape_md(text),
                           parse_mode=types.ParseMode.MARKDOWN_V2,
                           message_thread_id=THREAD_ID_PUSH_SHIFT)

    # 3.1) Штрафной тред и лог — ТОЛЬКО поздние даты
    if late_set:
        late_dates_bulleted = []
        for idx in sorted(late_set):
            date_str, loc = shifts[idx]
            log_late_cancel_to_sheet(fio=fio, date_str=date_str, loc=loc, username=username)
            late_dates_bulleted.append(f"• {date_str}")

        if late_dates_bulleted:
            penalty_text = "⚠️ Отмена менее чем за 24 часа\n" + f"{fio} {user_tag}\n" + "\n".join(late_dates_bulleted)
            await bot.send_message(CHAT_ID_PUSH, escape_md(penalty_text),
                                   parse_mode=types.ParseMode.MARKDOWN_V2,
                                   message_thread_id=THREAD_ID_PUSH_SHTRAF)

    # 4) «Ваш график с сегодняшнего дня…» — только даты
    schedule = get_upcoming_shifts_for_fio(fio)
    if schedule:
        schedule_sorted = sorted(schedule, key=lambda t: datetime.strptime(t[0], "%d.%m.%Y"))
        lines = [dt for dt, _loc in schedule_sorted]
        msg = "🗓 Ваш график с сегодняшнего дня:\n\n" + "\n".join(lines)
    else:
        msg = "Пока нет записей в графике"

    await bot.send_message(cb.from_user.id, escape_md(msg),
                           parse_mode=types.ParseMode.MARKDOWN_V2,
                           reply_markup=back_to_menu_kb())

    await state.finish()

async def on_confirm_no(cb: types.CallbackQuery, state: FSMContext):
    """
    Отмена подтверждения — возвращаем выбор смен снова.
    """
    await cb.answer()
    data = await state.get_data()
    shifts: list[tuple[str, str]] = data.get("shifts", [])
    picked: set[int] = set(data.get("picked_indices", []))

    # Для UI снова показ без локаций
    shifts_view = [(dt, "") for dt, _ in shifts]
    kb = build_cancellation_kb(shifts_view, picked)
    await cb.message.edit_text(escape_md("📅 Выберите смены для отмены:"),
                               parse_mode=types.ParseMode.MARKDOWN_V2,
                               reply_markup=kb)
    await CancelStates.WAIT_SELECTION.set()


def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_cancel_shift, commands=["cancel_shift"], state="*")
    dp.register_callback_query_handler(
        on_shift_selected,
        lambda cb: cb.data.startswith('select_shift:'),
        state=CancelStates.WAIT_SELECTION,
    )
    dp.register_callback_query_handler(
        on_cancel_exit,
        lambda cb: cb.data == 'exit_cancel',
        state=CancelStates.WAIT_SELECTION,
    )
    dp.register_callback_query_handler(
        on_confirm_yes,
        lambda cb: cb.data == 'confirm:yes',
        state=CancelStates.WAIT_CONFIRM,
    )
    dp.register_callback_query_handler(
        on_confirm_no,
        lambda cb: cb.data == 'confirm:no',
        state=CancelStates.WAIT_CONFIRM,
    )
