# app/handlers/request_tmc.py

import logging
import requests
import asyncio
from datetime import datetime, timedelta
from typing import Union

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.utils.script import resolve_event
from app.config import TOKEN_BOT, TELEGRAM_CHAT_ID_ARRIVAL, TELEGRAM_THREAD_MESSAGE_ID, PAGE_SIZE, URL_TASK_FIO
from app.keyboards import (
    inline_kb_paginated,
    inline_kb_yes_no_exit,
    inline_kb_exit_and_list,
)
from app.utils.gsheets import (
    get_material_names,
    get_material_quantity,
    get_next_request_number,
    write_request_tmc_rows,
)
from app.states import RequestTmcStates


bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.HTML)
logger = logging.getLogger(__name__)

DEPARTMENTS = [
    "Склад",
    "Ст техник",
    "Зона ШМ",
    "Локации СШМ",
    "Запчасти техничка",
]


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============================================

def build_request_fix_keyboard(items: list[dict]) -> InlineKeyboardMarkup:
    """
    Клавиатура для исправления списка позиций:
    - одна кнопка на каждую позицию (удаление)
    - 'Добавить позицию'
    - 'Закончить редактирование'
    - 'Выход'
    """
    kb = InlineKeyboardMarkup(row_width=1)

    for idx, it in enumerate(items):
        name = it.get("name", "")
        qty = it.get("quantity", "")
        text = f"❌ {name} × {qty}"
        kb.insert(InlineKeyboardButton(text, callback_data=f"req_delete:{idx}"))

    kb.row(
        InlineKeyboardButton("➕ Добавить позицию", callback_data="req_fix:add"),
        InlineKeyboardButton("✅ Закончить", callback_data="req_fix:finish"),
    )
    kb.row(InlineKeyboardButton("Выход", callback_data="req_exit"))
    return kb


async def build_request_summary(items: list[dict], department: str) -> str:
    """
    Строка-проверка перед отправкой в чат.
    """
    summary = "Проверка данных:\n"
    summary += f"Отдел: {department}\n\n"
    for it in items:
        summary += f"🔻 {it['name']}\nКоличество: {it['quantity']}\n\n"
    return summary


async def preload_material_names(state: FSMContext) -> None:
    """
    Фоновая предзагрузка списка ТМЦ.
    Выполняется в отдельном потоке, чтобы не блокировать event loop.
    """
    try:
        loop = asyncio.get_running_loop()
        names = await loop.run_in_executor(None, get_material_names)
        names_sorted = sorted(set(names), key=lambda s: s.lower())
        await state.update_data(options=names_sorted)
    except Exception as e:
        logger.exception("Ошибка предзагрузки списка ТМЦ: %s", e)


# ---- СТАРТ КОМАНДЫ ----
async def cmd_request_tmc(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    """
    /request_tmc — запрос выдачи ТМЦ:
    1) Выбор отдела
    2) Выбор ТМЦ + ввод количества (с проверкой остатка)
    3) Можно добавить ещё ТМЦ
    4) Проверка данных, при необходимости — исправление
    5) Отправка сводки в чат
    """
    message, actor = resolve_event(event)

    if getattr(message.chat, "type", None) != "private":
        await message.answer("Команда доступна только в ЛС")
        return

    await state.finish()

    username = getattr(actor, "username", None) or getattr(message.chat, "username", None) or ""
    tag = f"@{username}" if username else ""
    logger.info("[request_tmc] start: %s chat_id=%s", tag or "<no_username>", message.chat.id)

    try:
        resp = requests.get(
            URL_TASK_FIO,
            params={"tg_chat_id": str(message.chat.id)},
            timeout=5,
            verify=False
        )
        resp.raise_for_status()
        user = resp.json().get("user", {})
        fullname = user.get("fullname")

    except Exception as e:
        logger.exception(
            "Ошибка получения ФИО из Клинка (URL_TASK_FIO=%s, chat_id=%s): %s",
            URL_TASK_FIO, actor.id, e
        )
        return await message.answer(
            "⚠️ Невозможно получить данные из КЛИНКа. Обратитесь к администратору."
        )

    # сохраняем ФИО и пустой список позиций
    await state.update_data(items=[], fullname=fullname)

    # ⚡️ стартуем фоновую предзагрузку списка ТМЦ
    asyncio.create_task(preload_material_names(state))

    kb = InlineKeyboardMarkup(row_width=2)
    for idx, dep in enumerate(DEPARTMENTS):
        kb.insert(InlineKeyboardButton(dep, callback_data=f"req_dep:{idx}"))
    kb.row(InlineKeyboardButton("Выход", callback_data="req_exit"))

    await message.answer("Выберите отдел, который запрашивает выдачу:", reply_markup=kb)
    await RequestTmcStates.WAIT_DEPARTMENT.set()


# ---- ВЫБОР ОТДЕЛА ----
async def process_department(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()

    # сразу убираем клавиатуру выбора отдела
    try:
        await cb.message.delete()
    except Exception:
        pass

    if cb.data == "req_exit":
        await state.finish()
        await cb.message.answer("Процесс отменён")
        return

    try:
        _, idx_str = cb.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    if not (0 <= idx < len(DEPARTMENTS)):
        await cb.message.answer("Неверный выбор отдела.")
        return

    department = DEPARTMENTS[idx]
    await state.update_data(department=department)

    data = await state.get_data()
    names_sorted = data.get("options")

    # если по какой-то причине предзагрузка не успела — грузим синхронно
    if not names_sorted:
        names = get_material_names()
        names_sorted = sorted(set(names), key=lambda s: s.lower())
        await state.update_data(options=names_sorted)

    await state.update_data(page=0)

    kb = inline_kb_paginated(
        options=names_sorted,
        base_callback="req",
        cb_prev="req_page_prev",
        cb_next="req_page_next",
        cb_item="req_select",
        cb_exit="req_exit",
        page=0,
    )

    await cb.message.answer(
        f"Отдел: <b>{department}</b>\n\nВыберите наименование ТМЦ:",
        reply_markup=kb,
    )
    await RequestTmcStates.WAIT_MATERIAL_SELECTION.set()


# ---- ПАГИНАЦИЯ СПИСКА ТМЦ ----
async def req_page_prev(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    options = data.get("options", [])
    try:
        _, page_str = callback.data.split(":", 1)
        page = int(page_str)
    except Exception:
        return

    await state.update_data(page=page)
    kb = inline_kb_paginated(
        options=options,
        base_callback="req",
        cb_prev="req_page_prev",
        cb_next="req_page_next",
        cb_item="req_select",
        cb_exit="req_exit",
        page=page,
    )
    await callback.message.edit_reply_markup(reply_markup=kb)


async def req_page_next(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    options = data.get("options", [])
    try:
        _, page_str = callback.data.split(":", 1)
        page = int(page_str)
    except Exception:
        return

    await state.update_data(page=page)
    kb = inline_kb_paginated(
        options=options,
        base_callback="req",
        cb_prev="req_page_prev",
        cb_next="req_page_next",
        cb_item="req_select",
        cb_exit="req_exit",
        page=page,
    )
    await callback.message.edit_reply_markup(reply_markup=kb)


# ---- ВЫБОР ТМЦ ----
async def process_material_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, page_str, idx_str = callback.data.split(":", 2)
        page = int(page_str)
        idx = int(idx_str)
    except Exception:
        return

    data = await state.get_data()
    options = data.get("options", [])
    global_idx = page * PAGE_SIZE + idx

    if not (0 <= global_idx < len(options)):
        await bot.send_message(callback.from_user.id, "Ошибка выбора ТМЦ")
        return

    # сначала убираем клавиатуру с выбором позиции
    try:
        await callback.message.delete()
    except Exception:
        pass

    name = options[global_idx]
    await state.update_data(current_material=name)

    # текущее количество этого ТМЦ
    current_qty = get_material_quantity(name)
    if not current_qty:
        current_qty = "0"

    # сохраняем остаток в state для последующей проверки
    await state.update_data(current_qty=current_qty)

    await bot.send_message(
        callback.from_user.id,
        (
            f"Вы выбрали: <b>{name}</b>\n"
            f"Текущее количество на складе: <b>{current_qty}</b>\n\n"
            f"Введите требуемое количество:"
        ),
        parse_mode=types.ParseMode.HTML,
    )
    await RequestTmcStates.WAIT_QUANTITY.set()


# ---- ВВОД КОЛИЧЕСТВА ----
async def process_quantity(message: types.Message, state: FSMContext):
    qty_raw = message.text.strip()
    if not qty_raw.isdigit():
        await message.answer("Количество должно быть целым числом. Введите ещё раз")
        return

    qty = int(qty_raw)
    if qty <= 0:
        await message.answer("Количество должно быть больше нуля. Введите ещё раз")
        return

    data = await state.get_data()
    name = data.get("current_material")
    if not name:
        await message.answer("Ошибка состояния: не выбрано наименование ТМЦ")
        await state.finish()
        return

    current_qty_raw = data.get("current_qty", "0")
    try:
        current_qty = int(current_qty_raw)
    except ValueError:
        current_qty = 0

    # если запрашиваемое количество больше остатка — спрашиваем подтверждение
    if qty > current_qty:
        diff = qty - current_qty

        await state.update_data(pending_qty=str(qty))

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("Запросить всё равно", callback_data="req_overflow_yes"))
        kb.add(InlineKeyboardButton("Изменить количество", callback_data="req_overflow_no"))
        kb.row(InlineKeyboardButton("Выход", callback_data="req_exit"))

        await message.answer(
            f"⚠️ Вы запрашиваете <b>{qty}</b> шт., но на складе только <b>{current_qty}</b> шт.\n"
            f"Недостача составляет: <b>{diff}</b> шт.\n\n"
            f"Выберите действие:",
            reply_markup=kb,
        )
        await RequestTmcStates.WAIT_OVERFLOW_CONFIRM.set()
        return

    # нормальный случай — остатка хватает
    items = data.get("items", [])
    items.append({"name": name, "quantity": str(qty)})
    await state.update_data(items=items, current_material=None, pending_qty=None)

    kb = inline_kb_yes_no_exit("req_exit")
    await message.answer("Требуется ещё ТМЦ?", reply_markup=kb)
    await RequestTmcStates.WAIT_MORE.set()


# ---- ПОДТВЕРЖДЕНИЕ ПЕРЕЗАПРОСА ПРИ НЕДОСТАТКЕ ----
async def process_overflow_confirm(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    # убираем клавиатуру с вариантами "Запросить всё равно / Изменить количество"
    try:
        await callback.message.delete()
    except Exception:
        pass

    data = await state.get_data()
    name = data.get("current_material")
    if not name:
        await bot.send_message(callback.from_user.id, "Ошибка состояния: не выбрано наименование ТМЦ")
        await state.finish()
        return

    current_qty_raw = data.get("current_qty", "0")
    try:
        current_qty = int(current_qty_raw)
    except ValueError:
        current_qty = 0

    pending_qty_raw = data.get("pending_qty", "0")
    try:
        pending_qty = int(pending_qty_raw)
    except ValueError:
        pending_qty = 0

    if callback.data == "req_overflow_yes":
        # добавляем позицию с запрошенным (большим) количеством
        items = data.get("items", [])
        items.append({"name": name, "quantity": str(pending_qty)})
        await state.update_data(items=items, current_material=None, pending_qty=None)

        kb = inline_kb_yes_no_exit("req_exit")
        await bot.send_message(callback.from_user.id, "Требуется ещё ТМЦ?", reply_markup=kb)
        await RequestTmcStates.WAIT_MORE.set()
        return

    if callback.data == "req_overflow_no":
        # просим ввести количество ещё раз
        await bot.send_message(
            callback.from_user.id,
            (
                f"На складе доступно только <b>{current_qty}</b> шт.\n"
                f"Введите требуемое количество (не больше доступного) "
                f"или меньшую величину:"
            ),
            parse_mode=types.ParseMode.HTML,
        )
        await RequestTmcStates.WAIT_QUANTITY.set()
        return


# ---- НУЖЕН ЛИ ЕЩЁ ТМЦ? ----
async def process_more(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    # сразу удаляем клавиатуру YES/NO
    try:
        await callback.message.delete()
    except Exception:
        pass

    data = await state.get_data()

    if callback.data == "YES":
        # используем уже загруженный список options, без повторного get_material_names()
        names_sorted = data.get("options")
        if not names_sorted:
            names = get_material_names()
            names_sorted = sorted(set(names), key=lambda s: s.lower())
            await state.update_data(options=names_sorted)

        await state.update_data(page=0)

        kb = inline_kb_paginated(
            options=names_sorted,
            base_callback="req",
            cb_prev="req_page_prev",
            cb_next="req_page_next",
            cb_item="req_select",
            cb_exit="req_exit",
            page=0,
        )

        await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
        await RequestTmcStates.WAIT_MATERIAL_SELECTION.set()
        return

    # NO → показываем сводку и спрашиваем "Всё ОК / Есть ошибка"
    department = data.get("department", "—")
    items = data.get("items", [])

    summary = await build_request_summary(items, department)
    kb = inline_kb_exit_and_list(
        ["Всё ОК", "Есть ошибка"],
        base_callback="req_confirm:",
        cb_exit="req_exit",
    )
    await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
    await RequestTmcStates.WAIT_CONFIRM.set()


# ---- ПОДТВЕРЖДЕНИЕ СПИСКА (Всё ОК / Есть ошибка) ----
async def process_request_confirm(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, choice = callback.data.split(":", 1)
    except ValueError:
        return

    # убираем клавиатуру подтверждения
    try:
        await callback.message.delete()
    except Exception:
        pass

    data = await state.get_data()
    department = data.get("department", "—")
    fullname = data.get("fullname", "-")
    items = data.get("items", [])

    if choice == "0":
        # Всё ОК → формируем окончательное сообщение и шлём в складской чат

        # 1) получаем номер заявки из гугл-таблицы
        request_number = get_next_request_number()

        dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
        caption = (
            f"#Запрос_ТМЦ\n"
            f"{dt}\n"
            f"Номер заявки: {request_number}\n"
            f"Отдел: {department}\n\n"
        )
        for it in items:
            caption += f"🔻 {it['name']}\nКоличество: {it['quantity']}\n\n"

        user = callback.from_user
        username = user.username or ""
        tag = f"@{username}" if username else ""
        caption += f"Запросил: {fullname} {tag}"

        # 2) отправляем сообщение в чат склада
        sent = await bot.send_message(
            TELEGRAM_CHAT_ID_ARRIVAL,
            caption,
            message_thread_id=TELEGRAM_THREAD_MESSAGE_ID,
        )

        # 3) формируем ссылку на сообщение
        cid_str = str(TELEGRAM_CHAT_ID_ARRIVAL)
        if cid_str.startswith("-100"):
            chat_link_id = cid_str[4:]
        else:
            chat_link_id = cid_str
        message_link = f"https://t.me/c/{chat_link_id}/{sent.message_id}"

        # 4) записываем строки в "Реестр заказ ТМЦ"
        write_request_tmc_rows(
            request_number=request_number,
            fio=fullname,
            tag=tag,
            department=department,
            items=items,
            message_link=message_link,
        )

        # Формируем красивый список позиций для пользователя
        summary_user = "Запрос отправлен ✅\n\n📦 <b>Список позиций:</b>\n"
        for it in items:
            summary_user += f"• {it['name']} — <b>{it['quantity']}</b> шт.\n"
        summary_user += f"\nНомер заявки: <b>{request_number}</b>"

        await bot.send_message(
            callback.from_user.id,
            summary_user,
            parse_mode="HTML"
        )

        await state.finish()
        return

    if choice == "1":
        kb = build_request_fix_keyboard(items)
        await bot.send_message(
            callback.from_user.id,
            "Выберите позицию для удаления или следующее действие:",
            reply_markup=kb,
        )
        # остаёмся в WAIT_CONFIRM для дальнейшего редактирования
        return


# ---- УДАЛЕНИЕ ПОЗИЦИИ ИЗ СПИСКА ----
async def process_request_delete(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    # удаляем старую клавиатуру перед показом новой
    try:
        await callback.message.delete()
    except Exception:
        pass

    data = await state.get_data()
    items = data.get("items", [])

    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except ValueError:
        return

    if 0 <= idx < len(items):
        items.pop(idx)
        await state.update_data(items=items)

    kb = build_request_fix_keyboard(items)
    await bot.send_message(
        callback.from_user.id,
        "Редактирование списка:",
        reply_markup=kb,
    )


# ---- ДЕЙСТВИЯ С КЛАВИАТУРЫ ИСПРАВЛЕНИЙ (add / finish) ----
async def process_request_fix(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, action = callback.data.split(":", 1)
    except ValueError:
        return

    # сразу убираем текущую клавиатуру редактирования
    try:
        await callback.message.delete()
    except Exception:
        pass

    if action == "add":
        # заново список ТМЦ (но без повторного запроса к Google, если уже есть options)
        data = await state.get_data()
        names_sorted = data.get("options")
        if not names_sorted:
            names = get_material_names()
            names_sorted = sorted(set(names), key=lambda s: s.lower())
            await state.update_data(options=names_sorted)

        await state.update_data(page=0)

        kb = inline_kb_paginated(
            options=names_sorted,
            base_callback="req",
            cb_prev="req_page_prev",
            cb_next="req_page_next",
            cb_item="req_select",
            cb_exit="req_exit",
            page=0,
        )
        await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
        await RequestTmcStates.WAIT_MATERIAL_SELECTION.set()

    elif action == "finish":
        # снова показываем проверку данных
        data = await state.get_data()
        department = data.get("department", "—")
        items = data.get("items", [])

        summary = await build_request_summary(items, department)
        kb = inline_kb_exit_and_list(
            ["Всё ОК", "Есть ошибка"],
            base_callback="req_confirm:",
            cb_exit="req_exit",
        )
        await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
        await RequestTmcStates.WAIT_CONFIRM.set()


# ---- ВЫХОД ----
async def request_exit(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Отмена")
    # удаляем сообщение с inline-клавиатурой "Выход"
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(callback.from_user.id, "Процесс отменён")
    await state.finish()


# ---- РЕГИСТРАЦИЯ ХЭНДЛЕРОВ ----
def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_request_tmc, commands=["request_tmc"], state="*")

    dp.register_callback_query_handler(
        process_department,
        lambda c: c.data.startswith("req_dep:") or c.data == "req_exit",
        state=RequestTmcStates.WAIT_DEPARTMENT,
    )

    dp.register_callback_query_handler(
        req_page_prev,
        lambda c: c.data.startswith("req_page_prev:"),
        state=RequestTmcStates.WAIT_MATERIAL_SELECTION,
    )
    dp.register_callback_query_handler(
        req_page_next,
        lambda c: c.data.startswith("req_page_next:"),
        state=RequestTmcStates.WAIT_MATERIAL_SELECTION,
    )
    dp.register_callback_query_handler(
        process_material_selection,
        lambda c: c.data.startswith("req_select:"),
        state=RequestTmcStates.WAIT_MATERIAL_SELECTION,
    )

    dp.register_message_handler(
        process_quantity,
        state=RequestTmcStates.WAIT_QUANTITY,
    )

    dp.register_callback_query_handler(
        process_overflow_confirm,
        lambda c: c.data in ["req_overflow_yes", "req_overflow_no"],
        state=RequestTmcStates.WAIT_OVERFLOW_CONFIRM,
    )

    dp.register_callback_query_handler(
        process_more,
        lambda c: c.data in ["YES", "NO"],
        state=RequestTmcStates.WAIT_MORE,
    )

    dp.register_callback_query_handler(
        process_request_confirm,
        lambda c: c.data.startswith("req_confirm:"),
        state=RequestTmcStates.WAIT_CONFIRM,
    )

    dp.register_callback_query_handler(
        process_request_delete,
        lambda c: c.data.startswith("req_delete:"),
        state=RequestTmcStates.WAIT_CONFIRM,
    )

    dp.register_callback_query_handler(
        process_request_fix,
        lambda c: c.data.startswith("req_fix:"),
        state=RequestTmcStates.WAIT_CONFIRM,
    )

    dp.register_callback_query_handler(
        request_exit,
        lambda c: c.data == "req_exit",
        state="*",
    )
