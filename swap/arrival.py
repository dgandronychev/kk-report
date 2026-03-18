import logging
import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import ReplyKeyboardRemove, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from typing import Union
from app.utils.script import resolve_event
#

from app.config import TOKEN_BOT, TELEGRAM_CHAT_ID_ARRIVAL, TELEGRAM_THREAD_MESSAGE_ID, PAGE_SIZE
from app.keyboards import (
    inline_kb_paginated,
    inline_kb_yes_no_exit,
    inline_kb_exit_and_list,
    build_arrival_fix_keyboard
)
from app.utils.gsheets import (
    get_material_names,
    get_material_cells,
    get_free_cells,
    get_material_quantity,
    write_arrival_row,
    remove_free_cell
)

bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.HTML)
logger = logging.getLogger(__name__)
photo_locks: dict[int, asyncio.Lock] = {}

class ArrivalStates(StatesGroup):
    WAIT_MATERIAL_SELECTION = State()
    WAIT_MANUAL_MATERIAL = State()
    WAIT_CELL_SELECTION = State()
    WAIT_FREE_CELL_SELECTION = State()
    WAIT_QUANTITY = State()
    WAIT_MORE = State()
    WAIT_CONFIRM = State()
    WAIT_PHOTO = State()

async def cmd_arrival(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    message, actor = resolve_event(event)
    if getattr(message.chat, "type", None) != "private":
        await message.answer("Команда доступна можно только в ЛС")
        return

    await state.finish()

    username = getattr(actor, "username", None) or getattr(message.chat, "username", None) or ""
    tag = f"@{username}" if username else ""
    logger.info("[arrival] start: %s chat_id=%s", tag or "<no_username>", message.chat.id)
    await state.finish()
    await state.update_data(arrival_items=[], photos=[])

    # unique names
    names = get_material_names()
    unique = sorted(set(names), key=lambda s: s.lower())
    options = unique + ["Нет необходимого ТМЦ"]
    await state.update_data(options=options, page=0)

    kb = inline_kb_paginated(
        options=options,
        base_callback="arrival",
        cb_prev="arrival_page_prev",
        cb_next="arrival_page_next",
        cb_item="arrival_select",
        cb_exit="arrival_exit",
        page=0
    )
    await message.answer("Выберите наименование ТМЦ:", reply_markup=kb)
    await ArrivalStates.WAIT_MATERIAL_SELECTION.set()

async def arrival_page_prev(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    opts = data.get('options', [])
    _, pg = callback.data.split(':',1)
    page = int(pg)
    await state.update_data(page=page)
    kb = inline_kb_paginated(options=opts, base_callback="arrival",
                              cb_prev="arrival_page_prev", cb_next="arrival_page_next",
                              cb_item="arrival_select", cb_exit="arrival_exit", page=page)
    await callback.message.edit_reply_markup(reply_markup=kb)

async def arrival_page_next(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    opts = data.get('options', [])
    _, pg = callback.data.split(':',1)
    page = int(pg)
    await state.update_data(page=page)
    kb = inline_kb_paginated(options=opts, base_callback="arrival",
                              cb_prev="arrival_page_prev", cb_next="arrival_page_next",
                              cb_item="arrival_select", cb_exit="arrival_exit", page=page)
    await callback.message.edit_reply_markup(reply_markup=kb)

async def process_arrival_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, page_str, idx_str = callback.data.split(':',2)
        page, idx = int(page_str), int(idx_str)
    except ValueError:
        return
    data = await state.get_data()
    opts = data.get('options', [])
    idx_global = page * PAGE_SIZE + idx
    if idx_global == len(opts) - 1:

        await bot.send_message(callback.from_user.id, "Введите наименование ТМЦ:")
        await ArrivalStates.WAIT_MANUAL_MATERIAL.set()
    else:
        name = opts[idx_global]
        await state.update_data(current_material=name)
        # fetch storage cells
        cells = get_material_cells(name)
        await state.update_data(cells=cells)
        await callback.message.delete()
        print(len(cells))
        if len(cells) > 1:
            kb = InlineKeyboardMarkup(row_width=1)
            for i,c in enumerate(cells):
                kb.insert(InlineKeyboardButton(f"🗂 {c['cell']} {c['quantity']} шт", callback_data=f"arrival_cell:{i}"))
            await bot.send_message(callback.from_user.id, "Выберите ячейку:", reply_markup=kb)
            await ArrivalStates.WAIT_CELL_SELECTION.set()
        else:
            cell = cells[0]['cell']
            await state.update_data(current_cell=cell)
            total = int(cells[0]['quantity'])
            await bot.send_message(
                callback.from_user.id,
                f"Вы выбрали: <b>{name}</b> 🗂 {cell}\nВведите количество:",
                parse_mode=types.ParseMode.HTML
            )
            await ArrivalStates.WAIT_QUANTITY.set()

async def process_arrival_cell(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    idx = int(callback.data.split(':',1)[1])
    cell_info = data['cells'][idx]
    name = data['current_material']
    cell = cell_info['cell']
    await state.update_data(current_cell=cell)
    await callback.message.delete()
    await bot.send_message(
        callback.from_user.id,
        f"Вы выбрали: <b>{name}</b> 🗂 {cell}\nВведите количество:",
        parse_mode=types.ParseMode.HTML
    )
    await ArrivalStates.WAIT_QUANTITY.set()

async def process_manual_material(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        return await message.answer("Наименование не может быть пустым")
    await state.update_data(current_material=name)
    # free cells sheet
    free = get_free_cells()
    await state.update_data(cells=[{'cell':c,'quantity':''} for c in free])
    if len(free) > 1:
        kb = InlineKeyboardMarkup(row_width=1)
        for i,c in enumerate(free):
            kb.insert(InlineKeyboardButton(f"🗂 {c}", callback_data=f"arrival_cell:{i}"))
        await message.answer("Выберите свободную ячейку:", reply_markup=kb)
        await ArrivalStates.WAIT_CELL_SELECTION.set()
    else:
        cell = free[0] if free else ''
        await state.update_data(current_cell=cell)
        await message.answer(f"Вы ввели: <b>{name}</b> 🗂 {cell}\nВведите количество:", parse_mode=types.ParseMode.HTML)
        await ArrivalStates.WAIT_QUANTITY.set()

async def process_arrival_quantity(message: types.Message, state: FSMContext):
    qty = message.text.strip()
    if not qty.isdigit():
        return await message.answer("Количество должно быть числом")
    data = await state.get_data()
    name = data.get('current_material')
    cell = data.get('current_cell', '')

    if not name:
        await message.answer("Ошибка состояния: нет названия материала")
        return await state.finish()

    # Если ячейка не выбрана — сначала выбор свободной ячейки
    if not cell:
        # Сохраняем количество, но ещё не добавляем в список
        await state.update_data(pending_qty=qty)

        free = get_free_cells()  # список вида ['A1','B3',…]
        if not free:
            await message.answer("Нет свободных ячеек для выбора!")
            return await state.finish()

        kb = InlineKeyboardMarkup(row_width=1)
        for i, c in enumerate(free):
            kb.insert(InlineKeyboardButton(f"🗂 {c}", callback_data=f"arrival_free:{i}"))

        await message.answer(
            "Ячейка не задана. Выберите одну из свободных ячеек:",
            reply_markup=kb
        )
        return await ArrivalStates.WAIT_FREE_CELL_SELECTION.set()

    # Если ячейка уже есть — обычный сценарий
    items = data.get('arrival_items', [])
    items.append({'name': name, 'quantity': qty, 'cell': cell})
    await state.update_data(arrival_items=items, current_material=None, current_cell=None)

    kb = inline_kb_yes_no_exit("arrival_exit")
    await message.answer("Требуется еще ТМЦ?", reply_markup=kb)
    return await ArrivalStates.WAIT_MORE.set()

async def process_free_cell_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    idx = int(callback.data.split(":",1)[1])
    free = get_free_cells()
    cell = free[idx]
    name = data['current_material']
    qty  = data['pending_qty']

    # теперь формируем окончательный элемент
    items = data.get('arrival_items', [])
    items.append({'name': name, 'quantity': qty, 'cell': cell})
    await state.update_data(
        arrival_items=items,
        current_material=None,
        current_cell=None,
        pending_qty=None
    )

    # удаляем клавиатуру выбора ячейки
    try:
        await callback.message.delete()
    except: pass

    kb = inline_kb_yes_no_exit("arrival_exit")
    await callback.message.answer("Требуется еще ТМЦ?", reply_markup=kb)
    return await ArrivalStates.WAIT_MORE.set()


async def process_arrival_more(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    if callback.data == "YES":
        names = get_material_names()
        unique = sorted(set(names), key=lambda s: s.lower())
        options = unique + ["Нет необходимого ТМЦ"]
        await state.update_data(options=options, page=0)
        kb = inline_kb_paginated(
            options=options, base_callback="arrival",
            cb_prev="arrival_page_prev", cb_next="arrival_page_next",
            cb_item="arrival_select", cb_exit="arrival_exit", page=0
        )
        msg = await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
        await ArrivalStates.WAIT_MATERIAL_SELECTION.set()
        return msg
    # NO → summary
    summary = "Проверка данных:\n"
    for it in data.get('arrival_items', []):
        summary += f"🔻 {it['name']} 🗂 {it['cell']}\nКоличество: {it['quantity']}\n\n"
    kb = inline_kb_exit_and_list(["Всё ОК","Есть ошибка"], "arrival_confirm:", "arrival_exit")
    await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
    await ArrivalStates.WAIT_CONFIRM.set()
    await callback.message.delete()

async def process_arrival_confirm(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, choice = callback.data.split(":", 1)
    except ValueError:
        logger.exception("Ошибка подтверждения: неверный формат callback_data")
        return

    if choice == "0":  # Всё ОК
        await bot.send_message(
            callback.from_user.id,
            "Прикрепите фото для подтверждения поступления ТМЦ (минимум 1)\n"
            "Можно отправить несколько фото, когда закончите, нажмите 'Готово'",
            reply_markup=ReplyKeyboardRemove()
        )
        await ArrivalStates.WAIT_PHOTO.set()

    elif choice == "1":  # Есть ошибка
        data = await state.get_data()
        items = data.get("arrival_items", [])
        from app.keyboards import build_arrival_fix_keyboard

        kb = build_arrival_fix_keyboard(items)
        await bot.send_message(
            callback.from_user.id,
            "Выберите позицию для удаления или следующее действие:",
            reply_markup=kb
        )
        await ArrivalStates.WAIT_CONFIRM.set()

    try:
        await callback.message.delete()
    except Exception as e:
        logger.exception("Ошибка удаления сообщения при подтверждении: %s", e)

async def process_arrival_delete(callback: types.CallbackQuery, state: FSMContext):
    """
    Удаляет выбранную позицию из arrival_items и показывает обновлённую клавиатуру исправлений.
    """
    await callback.answer()
    data = await state.get_data()
    items = data.get("arrival_items", [])

    # парсим индекс из callback_data
    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except (ValueError, IndexError):
        return

    # удаляем, если в диапазоне
    if 0 <= idx < len(items):
        items.pop(idx)
        await state.update_data(arrival_items=items)

    # удаляем старое сообщение
    try:
        await callback.message.delete()
    except:
        pass

    # показываем клавиатуру исправлений заново
    kb = build_arrival_fix_keyboard(items)
    await callback.message.answer(
        "Выберите позицию для удаления или следующее действие:",
        reply_markup=kb
    )


async def process_arrival_fix(callback: types.CallbackQuery, state: FSMContext):
    """
    Обрабатывает действия 'add' или 'finish' из клавиатуры исправлений.
    - add: возвращает к выбору материала;
    - finish: показывает итоговую сводку для подтверждения.
    """
    await callback.answer()
    try:
        _, action = callback.data.split(":", 1)
    except ValueError:
        return

    # удаляем старое сообщение
    try:
        await callback.message.delete()
    except:
        pass

    if action == "add":
        # заново формируем уникальный список и показываем paginate
        names = get_material_names()
        unique = sorted(set(names), key=lambda s: s.lower())
        options = unique + ["Нет необходимого ТМЦ"]
        await state.update_data(options=options, page=0)

        kb = inline_kb_paginated(
            options=options,
            base_callback="arrival",
            cb_prev="arrival_page_prev",
            cb_next="arrival_page_next",
            cb_item="arrival_select",
            cb_exit="arrival_exit",
            page=0
        )
        await callback.message.answer("Выберите наименование ТМЦ:", reply_markup=kb)
        await ArrivalStates.WAIT_MATERIAL_SELECTION.set()

    elif action == "finish":
        data = await state.get_data()
        items = data.get("arrival_items", [])

        # составляем итоговую сводку
        summary = "Проверка данных:\n"
        for it in items:
            summary += f"🔻 {it['name']} 🗂 {it['cell']}\nКоличество: {it['quantity']}\n\n"

        kb = inline_kb_exit_and_list(
            ["Всё ОК", "Есть ошибка"],
            base_callback="arrival_confirm:",
            cb_exit="arrival_exit"
        )
        await callback.message.answer(summary, reply_markup=kb)
        await ArrivalStates.WAIT_CONFIRM.set()


async def process_arrival_photo(message: types.Message, state: FSMContext):
    lock = photo_locks.setdefault(message.chat.id, asyncio.Lock())
    async with lock:
        data = await state.get_data()
        photos = data.get('photos', [])
        if message.photo:
            photos.append(message.photo[-1].file_id)
            await state.update_data(photos=photos)
            last = data.get('last_photo_msg_id')
            if last:
                await bot.delete_message(message.chat.id, last)
            nkb = inline_kb_exit_and_list(["Готово"], base_callback="arrival_done:", cb_exit="arrival_exit")
            new = await message.answer(f"Фото получено: {len(photos)}", reply_markup=nkb)
            await state.update_data(last_photo_msg_id=new.message_id)
        else:
            await message.answer("Не удалось получить фото")

async def arrival_done_photos(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    photos = data.get('photos', [])
    if not photos:
        return await bot.send_message(callback.from_user.id, "Минимум 1 фото")
    items = data.get('arrival_items', [])
    dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    caption = f"{dt}\n#Поступление_ТМЦ\n📥 Поступление\n\n"
    for it in items:
        caption += f"🔻 {it['name']} 🗂 {it['cell']}\nКоличество: {it['quantity']}\n\n"
    user = callback.from_user
    caption += f"@{user.username}" if user.username else user.first_name
    if len(photos) == 1:
        sent = await bot.send_photo(TELEGRAM_CHAT_ID_ARRIVAL, message_thread_id=TELEGRAM_THREAD_MESSAGE_ID, photo=photos[0], caption=caption)
    else:
        media = [InputMediaPhoto(media=photos[0], caption=caption)]
        for p in photos[1:]: media.append(InputMediaPhoto(media=p))
        msgs = await bot.send_media_group(TELEGRAM_CHAT_ID_ARRIVAL, media=media, message_thread_id=TELEGRAM_THREAD_MESSAGE_ID)
        sent = msgs[0] if msgs else None
    link = f"https://t.me/c/{str(TELEGRAM_CHAT_ID_ARRIVAL)[4:]}/{sent.message_id}" if sent else ""
    await bot.send_message(callback.from_user.id, "Данные успешно отправлены")
    await callback.message.delete()
    # write and remove free cells
    for it in items:
        write_arrival_row([it], f"@{user.username}" if user.username else user.first_name, link)
        # if from free sheet, remove it
        if it.get('cell') in get_free_cells():
            remove_free_cell(it['cell'])
    await state.finish()

async def arrival_back(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Возврат")
    await callback.message.delete()
    await state.finish()

async def arrival_exit(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Отмена")
    await callback.message.delete()
    await bot.send_message(callback.from_user.id, "Процесс отменён")
    await state.finish()


def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_arrival, commands=["arrival"], state="*")
    dp.register_callback_query_handler(arrival_page_prev, lambda c: c.data.startswith("arrival_page_prev:"), state=ArrivalStates.WAIT_MATERIAL_SELECTION)
    dp.register_callback_query_handler(arrival_page_next, lambda c: c.data.startswith("arrival_page_next:"), state=ArrivalStates.WAIT_MATERIAL_SELECTION)
    dp.register_callback_query_handler(process_arrival_selection, lambda c: c.data.startswith("arrival_select:"), state=ArrivalStates.WAIT_MATERIAL_SELECTION)
    dp.register_message_handler(process_manual_material, state=ArrivalStates.WAIT_MANUAL_MATERIAL)
    dp.register_callback_query_handler(process_arrival_cell, lambda c: c.data.startswith("arrival_cell:"), state=ArrivalStates.WAIT_CELL_SELECTION)
    dp.register_callback_query_handler(process_free_cell_selection, lambda c: c.data.startswith("arrival_free:"),state=ArrivalStates.WAIT_FREE_CELL_SELECTION)
    dp.register_message_handler(process_arrival_quantity, state=ArrivalStates.WAIT_QUANTITY)
    dp.register_callback_query_handler(process_arrival_more, lambda c: c.data in ["YES","NO"], state=ArrivalStates.WAIT_MORE)
    dp.register_callback_query_handler(process_arrival_confirm, lambda c: c.data.startswith("arrival_confirm:"), state=ArrivalStates.WAIT_CONFIRM)
    dp.register_callback_query_handler(process_arrival_delete, lambda c: c.data.startswith("arrival_delete:"), state=ArrivalStates.WAIT_CONFIRM)
    dp.register_callback_query_handler(process_arrival_fix, lambda c: c.data.startswith("arrival_fix:"), state=ArrivalStates.WAIT_CONFIRM)
    dp.register_message_handler(process_arrival_photo, content_types=types.ContentType.PHOTO, state=ArrivalStates.WAIT_PHOTO)
    dp.register_callback_query_handler(arrival_done_photos, lambda c: c.data.startswith("arrival_done:"), state=ArrivalStates.WAIT_PHOTO)
    dp.register_callback_query_handler(arrival_back, lambda c: c.data=="arrival_back", state="*")
    dp.register_callback_query_handler(arrival_exit, lambda c: c.data=="arrival_exit", state="*")
