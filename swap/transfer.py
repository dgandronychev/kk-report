# app/handlers/transfer.py

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Union, List, Dict, Any

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import (
    ReplyKeyboardRemove,
    InputMediaPhoto,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from app.utils.script import resolve_event
from app.config import (
    TOKEN_BOT,
    TELEGRAM_CHAT_ID_ARRIVAL,
    TELEGRAM_THREAD_MESSAGE_ID,
    PAGE_SIZE,
)
from app.states import TransferStates
from app.keyboards import (
    build_transfer_fix_keyboard,
    inline_kb_exit_and_list,
    inline_kb_yes_no_exit,
    inline_kb_paginated,
)
from app.utils.gsheets import (
    get_material_names,
    get_material_quantity,
    get_recipient_names,
    get_material_cells,
    write_transfer_row,
    return_cell_to_free,
    get_shm_locations_by_company,
    get_open_request_numbers,
    get_request_items,
    update_request_status,
)

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.HTML)

# локи на приём фото по chat.id, чтобы не было гонок
_photo_locks: Dict[int, asyncio.Lock] = {}


async def _get_photo_lock(chat_id: int) -> asyncio.Lock:
    lock = _photo_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _photo_locks[chat_id] = lock
    return lock


# =====================================================================
#              ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ВЫДАЧИ ПО ЗАЯВКЕ
# =====================================================================

def auto_fill_transfer_items_from_request(
    request_items: List[Dict[str, Any]],
) -> (List[Dict[str, str]], List[str]):
    """
    По списку позиций заявки строит список transfer_items с автоматически
    подобранными ячейками.

    request_items: [{"name": "...", "quantity": "3"}, ...]

    Возвращает:
      transfer_items: [{"name", "cell", "quantity"}, ...]
      warnings: список строк-предупреждений (недостаток на складе и т.п.)
    """
    transfer_items: List[Dict[str, str]] = []
    warnings: List[str] = []

    for req in request_items:
        name = (req.get("name") or "").strip()
        if not name:
            continue

        qty_raw = str(req.get("quantity") or "").strip()
        try:
            need = int(qty_raw or "0")
        except ValueError:
            warnings.append(f"{name}: некорректное количество в заявке ({qty_raw})")
            continue

        if need <= 0:
            continue

        cells = get_material_cells(name)  # [{"cell": "...", "quantity": "..."}, ...]
        if not cells:
            warnings.append(f"{name}: нет доступных ячеек на складе")
            continue

        remain = need
        taken_total = 0

        for c in cells:
            if remain <= 0:
                break

            cell_name = (c.get("cell") or "").strip()
            if not cell_name:
                continue

            cell_q_raw = str(c.get("quantity") or "").strip()
            try:
                cell_qty = int(cell_q_raw or "0")
            except ValueError:
                cell_qty = 0

            if cell_qty <= 0:
                continue

            take = min(remain, cell_qty)
            if take <= 0:
                continue

            transfer_items.append(
                {
                    "name": name,
                    "cell": cell_name,
                    "quantity": str(take),
                }
            )
            remain -= take
            taken_total += take

        if remain > 0:
            warnings.append(
                f"{name}: в заявке {need} шт., а на складе удалось набрать только "
                f"{taken_total} шт. (не хватает {remain})"
            )

    return transfer_items, warnings


async def build_summary(
    items: List[Dict[str, Any]],
    recipient: str,
    warnings: List[str] | None = None,
) -> str:
    """
    Формирование сводки перед подтверждением выдачи.
    """
    summary = f"Проверка данных:\nКому выдаём: {recipient}\n\n"

    for it in items:
        name = it["name"]
        cell = it.get("cell") or ""
        qty = int(it["quantity"])

        total_str = get_material_quantity(name)
        try:
            total = int(total_str or "0")
        except ValueError:
            total = 0

        issued_for_name_cell = sum(
            int(x["quantity"])
            for x in items
            if x["name"] == name and (x.get("cell") or "") == cell
        )
        remainder = max(total - issued_for_name_cell, 0)

        summary += f"🔻 {name}"
        if cell:
            summary += f" 🗂 {cell}"
        summary += (
            f"\nКоличество: {qty}\n"
            f"Остаток после выдачи: {remainder} (из {total})\n\n"
        )

    if warnings:
        if items:
            summary += "\n"
        summary += "⚠️ Предупреждения:\n"
        for w in warnings:
            summary += f"• {w}\n"

    return summary


async def _ask_recipient(message: types.Message, state: FSMContext):
    """
    Спрашивает 'Кому выдаёте?' и переводит в WAIT_RECIPIENT_SELECTION.
    Добавлено сообщение об ожидании загрузки.
    """
    loading_msg = await message.answer("⏳ Загружаю список получателей...")
    recipients = get_recipient_names()
    recipients_sorted = sorted(recipients, key=lambda s: s.lower())
    try:
        await loading_msg.delete()
    except Exception:
        pass

    kb = inline_kb_exit_and_list(recipients_sorted, "transfer_recipient:", "transfer_exit")
    await message.answer("Кому выдаёте?", reply_markup=kb)
    await TransferStates.WAIT_RECIPIENT_SELECTION.set()


# =====================================================================
#                           СТАРТ КОМАНДЫ
# =====================================================================

async def cmd_transfer(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    """
    /transfer — выдача ТМЦ со склада.

    1) Вопрос: выдача по заявке? (Да/Нет)
    2) Если Нет — обычный сценарий.
       Если Да — выбор заявки со статусом 'Новая' и автоподстановка позиций.
    """
    message, actor = resolve_event(event)

    if getattr(message.chat, "type", None) != "private":
        await message.answer("Команда доступна только в личных сообщениях с ботом.")
        return

    await state.finish()

    username = getattr(actor, "username", None) or getattr(message.chat, "username", None) or ""
    tag = f"@{username}" if username else ""
    logger.info("[transfer] start: %s chat_id=%s", tag or "<no_username>", message.chat.id)

    await state.update_data(
        transfer_items=[],
        photos=[],
        request_number=None,
        request_items=[],
        request_warnings=[],
    )

    kb = inline_kb_exit_and_list(
        ["Да", "Нет"],
        base_callback="transfer_byreq:",
        cb_exit="transfer_exit",
    )
    await message.answer("Выдача по заявке?", reply_markup=kb)
    await TransferStates.WAIT_BY_REQUEST.set()


# =====================================================================
#                     ВЫДАЧА ПО ЗАЯВКЕ?  (Да / Нет)
# =====================================================================

async def process_transfer_by_request(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    try:
        await callback.message.delete()
    except Exception:
        pass

    # 0 — Да, 1 — Нет
    if idx == 0:
        # загрузка списка заявок может быть долгой
        loading_msg = await bot.send_message(
            callback.from_user.id,
            "⏳ Загружаю список заявок. Пожалуйста, подождите...",
        )
        numbers = get_open_request_numbers()
        try:
            await loading_msg.delete()
        except Exception:
            pass

        if not numbers:
            await callback.message.answer(
                "Нет заявок со статусом <b>«Новая»</b>.\n"
                "Продолжаем обычную выдачу без привязки к заявке.",
                parse_mode=types.ParseMode.HTML,
            )
            await _ask_recipient(callback.message, state)
            return

        await state.update_data(request_numbers=numbers)

        kb = inline_kb_exit_and_list(
            numbers,
            base_callback="transfer_reqnum:",
            cb_exit="transfer_exit",
        )
        await callback.message.answer("Выберите номер заявки:", reply_markup=kb)
        await TransferStates.WAIT_REQUEST_SELECT.set()
        return

    # Нет — обычная логика
    await _ask_recipient(callback.message, state)


# =====================================================================
#                ВЫБОР НОМЕРА ЗАЯВКИ И ДЕЙСТВИЕ ПО НЕЙ
# =====================================================================

async def process_transfer_request_select(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    data = await state.get_data()
    numbers: List[str] = data.get("request_numbers") or []
    if not (0 <= idx < len(numbers)):
        await callback.message.answer("Ошибка выбора заявки. Попробуйте снова.")
        return

    request_number = numbers[idx]

    # тут чтение позиций заявки — тоже может тормозить
    try:
        await callback.message.delete()
    except Exception:
        pass
    loading_msg = await bot.send_message(
        callback.from_user.id,
        f"⏳ Загружаю позиции заявки {request_number}...",
    )
    items = get_request_items(request_number)
    try:
        await loading_msg.delete()
    except Exception:
        pass

    if not items:
        await bot.send_message(
            callback.from_user.id,
            f"Для заявки {request_number} не найдено позиций.\n"
            f"Продолжаем обычную выдачу без привязки к заявке.",
        )
        await _ask_recipient(callback.message, state)
        return

    await state.update_data(
        request_number=request_number,
        request_items=items,
    )

    text = f"Заявка: <b>{request_number}</b>\n\nПозиции:\n"
    for it in items:
        text += f"🔻 {it.get('name', '')} — <b>{it.get('quantity', '')}</b> шт.\n"

    text += "\nЧто сделать с заявкой?"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Отменить", callback_data="transfer_req_action:cancel"),
        InlineKeyboardButton("Выдать", callback_data="transfer_req_action:issue"),
    )
    kb.row(InlineKeyboardButton("Выход", callback_data="transfer_exit"))

    await bot.send_message(
        callback.from_user.id,
        text,
        reply_markup=kb,
        parse_mode=types.ParseMode.HTML,
    )
    await TransferStates.WAIT_REQUEST_ACTION.set()


async def process_transfer_request_action(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, action = callback.data.split(":", 1)
    except Exception:
        return

    data = await state.get_data()
    request_number: str = data.get("request_number") or ""
    request_items: List[Dict[str, Any]] = data.get("request_items") or []

    try:
        await callback.message.delete()
    except Exception:
        pass

    # --- Отмена заявки ---
    if action == "cancel":
        if request_number:
            update_request_status(request_number, "Отменено", message_link=None)
        await bot.send_message(
            callback.from_user.id,
            f"Заявка <b>{request_number}</b> помечена как <b>«Отменено»</b>.",
            parse_mode=types.ParseMode.HTML,
        )
        await state.finish()
        return

    # --- Выдать по заявке ---
    if action == "issue":
        # подбор ТМЦ по заявке + чтение ячеек может быть долгим
        loading_msg = await bot.send_message(
            callback.from_user.id,
            "⏳ Подбираю ТМЦ по заявке и ячейки на складе...",
        )
        transfer_items, warnings = auto_fill_transfer_items_from_request(request_items)
        try:
            await loading_msg.delete()
        except Exception:
            pass

        if not transfer_items:
            await bot.send_message(
                callback.from_user.id,
                f"Не удалось подобрать ТМЦ по заявке {request_number}. "
                f"Проверьте остатки на складе.",
            )
            await state.finish()
            return

        await state.update_data(
            transfer_items=transfer_items,
            request_warnings=warnings,
        )

        # теперь спрашиваем 'Кому выдаёте?'
        await _ask_recipient(callback.message, state)
        return


# =====================================================================
#                   ВЫБОР ПОЛУЧАТЕЛЯ (ОБЩИЙ ДЛЯ ВСЕХ)
# =====================================================================

async def process_transfer_recipient(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, idx_str = callback.data.split(":", 1)
        index = int(idx_str)
    except Exception:
        logger.exception("Ошибка выбора получателя")
        return

    recipients = get_recipient_names()
    recipients_sorted = sorted(recipients, key=lambda s: s.lower())

    if not (0 <= index < len(recipients_sorted)):
        await callback.message.answer("Ошибка выбора получателя, попробуйте снова")
        return

    chosen = recipients_sorted[index]

    try:
        await callback.message.delete()
    except Exception:
        pass

    await state.update_data(recipient_base=chosen, recipient=chosen)

    data = await state.get_data()
    request_number = data.get("request_number")
    transfer_items: List[Dict[str, Any]] = data.get("transfer_items") or []
    warnings: List[str] = data.get("request_warnings") or []

    # Если выдача по заявке и список позиций уже сформирован —
    # сразу к подтверждению (без выбора ТМЦ).
    if request_number and transfer_items:
        summary = await build_summary(transfer_items, chosen, warnings=warnings)
        kb = inline_kb_exit_and_list(
            ["Всё ОК", "Есть ошибка"],
            base_callback="transfer_confirm:",
            cb_exit="transfer_exit",
        )
        await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
        await TransferStates.WAIT_CONFIRM.set()
        return

    # --- Обычная логика (выдача не по заявке) ---
    if chosen.upper() == "ЛОКАЦИИ СШМ":
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("Ситидрайв", callback_data="transfer_shm_company:Ситидрайв"),
            InlineKeyboardButton("Яндекс", callback_data="transfer_shm_company:Яндекс"),
            InlineKeyboardButton("Белка", callback_data="transfer_shm_company:Белка"),
        )
        await bot.send_message(callback.from_user.id, "Выберите компанию:", reply_markup=kb)
        await TransferStates.WAIT_SHM_COMPANY.set()
        return

    # загрузка списка ТМЦ
    loading_msg = await bot.send_message(
        callback.from_user.id,
        "⏳ Загружаю список ТМЦ...",
    )
    names = get_material_names()
    names_sorted = sorted(names, key=lambda s: s.lower())
    try:
        await loading_msg.delete()
    except Exception:
        pass

    await state.update_data(options=names_sorted, page=0)

    kb = inline_kb_paginated(
        options=names_sorted,
        base_callback="transfer",
        cb_prev="transfer_page_prev",
        cb_next="transfer_page_next",
        cb_item="transfer_select",
        cb_exit="transfer_exit",
        page=0,
    )
    await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
    await TransferStates.WAIT_MATERIAL_SELECTION.set()


# =====================================================================
#             ВЕТКА ДЛЯ ЛОКАЦИЙ СШМ (СИТИ / ЯНДЕКС / БЕЛКА)
# =====================================================================

async def process_transfer_shm_company(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    _, company = callback.data.split(":", 1)
    await state.update_data(shm_company=company)

    try:
        await callback.message.delete()
    except Exception:
        pass

    loading_msg = await bot.send_message(
        callback.from_user.id,
        "⏳ Загружаю список локаций...",
    )
    addrs = get_shm_locations_by_company(company)
    try:
        await loading_msg.delete()
    except Exception:
        pass

    if not addrs:
        await bot.send_message(
            callback.from_user.id,
            f"Для компании {company} нет локаций",
        )
        await state.finish()
        return

    kb = inline_kb_exit_and_list(addrs, "transfer_shm_loc:", "transfer_exit")
    await bot.send_message(callback.from_user.id, "Выберите адрес локации:", reply_markup=kb)
    await TransferStates.WAIT_SHM_LOCATION.set()


async def process_transfer_shm_location(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    company = data.get("shm_company", "")

    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    addrs = get_shm_locations_by_company(company)
    if not (0 <= idx < len(addrs)):
        return

    addr = addrs[idx]
    recipient_text = f"ЛОКАЦИИ СШМ\n{company} {addr}"

    await state.update_data(recipient_base=recipient_text, recipient=recipient_text)

    try:
        await callback.message.delete()
    except Exception:
        pass

    # после выбора локации — загрузка списка ТМЦ
    loading_msg = await bot.send_message(
        callback.from_user.id,
        "⏳ Загружаю список ТМЦ...",
    )
    names = get_material_names()
    names_sorted = sorted(names, key=lambda s: s.lower())
    try:
        await loading_msg.delete()
    except Exception:
        pass

    await state.update_data(options=names_sorted, page=0)

    kb = inline_kb_paginated(
        options=names_sorted,
        base_callback="transfer",
        cb_prev="transfer_page_prev",
        cb_next="transfer_page_next",
        cb_item="transfer_select",
        cb_exit="transfer_exit",
        page=0,
    )
    await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
    await TransferStates.WAIT_MATERIAL_SELECTION.set()


# =====================================================================
#                       ПАГИНАЦИЯ СПИСКА ТМЦ
# =====================================================================

async def transfer_page_prev(callback: types.CallbackQuery, state: FSMContext):
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
        base_callback="transfer",
        cb_prev="transfer_page_prev",
        cb_next="transfer_page_next",
        cb_item="transfer_select",
        cb_exit="transfer_exit",
        page=page,
    )
    await callback.message.edit_reply_markup(reply_markup=kb)


async def transfer_page_next(callback: types.CallbackQuery, state: FSMContext):
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
        base_callback="transfer",
        cb_prev="transfer_page_prev",
        cb_next="transfer_page_next",
        cb_item="transfer_select",
        cb_exit="transfer_exit",
        page=page,
    )
    await callback.message.edit_reply_markup(reply_markup=kb)


# =====================================================================
#                      ВЫБОР ТМЦ И ЯЧЕЙКИ (обычный путь)
# =====================================================================

async def process_transfer_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, page_str, idx_str = callback.data.split(":", 2)
        page = int(page_str)
        idx = int(idx_str)
    except Exception:
        logger.exception("Неверный формат callback_data для transfer_select")
        return

    data = await state.get_data()
    options = data.get("options", [])
    global_idx = page * PAGE_SIZE + idx

    if not (0 <= global_idx < len(options)):
        await bot.send_message(callback.from_user.id, "Ошибка выбора ТМЦ")
        return

    material = options[global_idx]
    await state.update_data(current_material=material)

    try:
        await callback.message.delete()
    except Exception:
        pass

    # загрузка ячеек для выбранного ТМЦ
    loading_msg = await bot.send_message(
        callback.from_user.id,
        "⏳ Загружаю ячейки на складе для выбранного ТМЦ...",
    )
    cells = get_material_cells(material)
    await state.update_data(cells=cells)
    try:
        await loading_msg.delete()
    except Exception:
        pass

    if len(cells) > 1:
        kb = InlineKeyboardMarkup(row_width=1)
        for i, c in enumerate(cells):
            cell = c.get("cell", "")
            qty = c.get("quantity", "0")
            kb.insert(
                InlineKeyboardButton(
                    f"🗂 {cell} — {qty} шт.",
                    callback_data=f"transfer_cell:{i}",
                )
            )
        await bot.send_message(callback.from_user.id, "Выберите ячейку:", reply_markup=kb)
        await TransferStates.WAIT_CELL_SELECTION.set()
    else:
        cell = cells[0].get("cell", "")
        qty_cell_raw = cells[0].get("quantity", "0")
        try:
            qty_cell = int(qty_cell_raw or "0")
        except ValueError:
            qty_cell = 0

        await state.update_data(current_cell=cell)

        await bot.send_message(
            callback.from_user.id,
            f"Вы выбрали: <b>{material}</b> 🗂 {cell}\n"
            f"Доступно в ячейке: <b>{qty_cell}</b> шт.\n\n"
            "Введите количество для выдачи:",
            parse_mode=types.ParseMode.HTML,
        )
        await TransferStates.WAIT_QUANTITY.set()


async def process_transfer_cell(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    cells = data.get("cells", [])

    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    if not (0 <= idx < len(cells)):
        return

    cell_info = cells[idx]
    cell = cell_info.get("cell", "")
    qty_cell_raw = cell_info.get("quantity", "0")
    try:
        qty_cell = int(qty_cell_raw or "0")
    except ValueError:
        qty_cell = 0

    material = data.get("current_material", "")

    await state.update_data(current_cell=cell)

    try:
        await callback.message.delete()
    except Exception:
        pass

    await bot.send_message(
        callback.from_user.id,
        f"Вы выбрали: <b>{material}</b> 🗂 {cell}\n"
        f"Доступно в ячейке: <b>{qty_cell}</b> шт.\n\n"
        "Введите количество для выдачи:",
        parse_mode=types.ParseMode.HTML,
    )
    await TransferStates.WAIT_QUANTITY.set()


async def process_transfer_manual_material(message: types.Message, state: FSMContext):
    material = message.text.strip()
    if not material:
        await message.answer("Наименование не может быть пустым, введите ещё раз.")
        return

    await state.update_data(current_material=material, current_cell="")

    await message.answer(
        f"Вы ввели: <b>{material}</b>\n"
        "Введите количество для выдачи:",
        parse_mode=types.ParseMode.HTML,
    )
    await TransferStates.WAIT_QUANTITY.set()


async def process_transfer_quantity(message: types.Message, state: FSMContext):
    qty_raw = message.text.strip()
    if not qty_raw.isdigit():
        await message.answer("Количество должно быть целым числом. Введите ещё раз.")
        return

    qty = int(qty_raw)
    if qty <= 0:
        await message.answer("Количество должно быть больше нуля. Введите ещё раз.")
        return

    data = await state.get_data()
    material = data.get("current_material")
    cell = data.get("current_cell", "")
    if not material:
        await message.answer("Ошибка состояния: не выбрано наименование ТМЦ.")
        await state.finish()
        return

    # проверяем доступность в ячейке
    cells = get_material_cells(material)
    cell_info = None
    for c in cells:
        if (c.get("cell") or "") == cell:
            cell_info = c
            break

    if cell_info is None and cells:
        cell_info = cells[0]
        cell = cell_info.get("cell", "")
        await state.update_data(current_cell=cell)

    total_in_cell = 0
    if cell_info:
        try:
            total_in_cell = int(str(cell_info.get("quantity") or "0"))
        except ValueError:
            total_in_cell = 0

    items: List[Dict[str, Any]] = data.get("transfer_items") or []
    already_planned = sum(
        int(it["quantity"])
        for it in items
        if it["name"] == material and (it.get("cell") or "") == cell
    )

    available = max(total_in_cell - already_planned, 0)

    if qty > available:
        await message.answer(
            "⚠️ Вы запрашиваете больше, чем доступно в выбранной ячейке.\n"
            f"Доступно: {available} шт. (из {total_in_cell}).\n"
            "Если всё равно нужно именно это количество — отправьте число ещё раз."
        )
        return  # остаёмся в WAIT_QUANTITY

    items.append({"name": material, "quantity": str(qty), "cell": cell})
    await state.update_data(transfer_items=items, current_material=None, current_cell=None)

    kb = inline_kb_yes_no_exit("transfer_exit")
    await message.answer("Требуется ещё ТМЦ?", reply_markup=kb)
    await TransferStates.WAIT_MORE.set()


# =====================================================================
#                 ЕЩЁ ТМЦ?  (YES / NO)  + ПОДТВЕРЖДЕНИЕ
# =====================================================================

async def process_transfer_more(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()

    if callback.data == "YES":
        # загрузка списка ТМЦ
        loading_msg = await bot.send_message(
            callback.from_user.id,
            "⏳ Загружаю список ТМЦ...",
        )
        names = get_material_names()
        names_sorted = sorted(names, key=lambda s: s.lower())
        try:
            await loading_msg.delete()
        except Exception:
            pass

        await state.update_data(options=names_sorted, page=0)

        kb = inline_kb_paginated(
            options=names_sorted,
            base_callback="transfer",
            cb_prev="transfer_page_prev",
            cb_next="transfer_page_next",
            cb_item="transfer_select",
            cb_exit="transfer_exit",
            page=0,
        )

        try:
            await callback.message.delete()
        except Exception:
            pass

        await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
        await TransferStates.WAIT_MATERIAL_SELECTION.set()
        return

    # NO — сводка
    recipient = data.get("recipient", "—")
    items: List[Dict[str, Any]] = data.get("transfer_items") or []
    warnings: List[str] = data.get("request_warnings") or []

    summary = await build_summary(items, recipient, warnings=warnings)

    kb = inline_kb_exit_and_list(
        ["Всё ОК", "Есть ошибка"],
        base_callback="transfer_confirm:",
        cb_exit="transfer_exit",
    )

    try:
        await callback.message.delete()
    except Exception:
        pass

    await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
    await TransferStates.WAIT_CONFIRM.set()


async def process_transfer_confirm(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, choice = callback.data.split(":", 1)
    except Exception:
        return

    if choice == "0":
        # Всё ОК — переходим к фото
        try:
            await callback.message.delete()
        except Exception:
            pass
        await bot.send_message(
            callback.from_user.id,
            "Прикрепите фото (от 1 до нескольких). "
            "После загрузки всех фотографий нажмите «Готово».",
            reply_markup=ReplyKeyboardRemove(),
        )
        await TransferStates.WAIT_PHOTO.set()
        return

    # Есть ошибка — показываем клавиатуру редактирования
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("transfer_items") or []

    kb = build_transfer_fix_keyboard(items)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(
        callback.from_user.id,
        "Выберите позицию для удаления или добавьте новую:",
        reply_markup=kb,
    )
    await TransferStates.WAIT_CONFIRM.set()


async def show_transfer_fix_options(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("transfer_items") or []
    kb = build_transfer_fix_keyboard(items)
    await bot.send_message(
        callback.from_user.id,
        "Редактирование списка:",
        reply_markup=kb,
    )


async def process_transfer_delete(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("transfer_items") or []

    try:
        _, idx_str = callback.data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        return

    if 0 <= idx < len(items):
        items.pop(idx)
        await state.update_data(transfer_items=items)

    try:
        await callback.message.delete()
    except Exception:
        pass

    await show_transfer_fix_options(callback, state)


async def process_transfer_fix(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        _, action = callback.data.split(":", 1)
    except Exception:
        return

    try:
        await callback.message.delete()
    except Exception:
        pass

    if action == "add":
        loading_msg = await bot.send_message(
            callback.from_user.id,
            "⏳ Загружаю список ТМЦ...",
        )
        names = get_material_names()
        names_sorted = sorted(names, key=lambda s: s.lower())
        try:
            await loading_msg.delete()
        except Exception:
            pass

        await state.update_data(options=names_sorted, page=0)

        kb = inline_kb_paginated(
            options=names_sorted,
            base_callback="transfer",
            cb_prev="transfer_page_prev",
            cb_next="transfer_page_next",
            cb_item="transfer_select",
            cb_exit="transfer_exit",
            page=0,
        )
        await bot.send_message(callback.from_user.id, "Выберите наименование ТМЦ:", reply_markup=kb)
        await TransferStates.WAIT_MATERIAL_SELECTION.set()
    elif action == "finish":
        data = await state.get_data()
        recipient = data.get("recipient", "—")
        items: List[Dict[str, Any]] = data.get("transfer_items") or []
        warnings: List[str] = data.get("request_warnings") or []

        summary = await build_summary(items, recipient, warnings=warnings)
        kb = inline_kb_exit_and_list(
            ["Всё ОК", "Есть ошибка"],
            base_callback="transfer_confirm:",
            cb_exit="transfer_exit",
        )
        await bot.send_message(callback.from_user.id, summary, reply_markup=kb)
        await TransferStates.WAIT_CONFIRM.set()


# =====================================================================
#                      ПРИЁМ ФОТО И ОКОНЧАТЕЛЬНЫЙ ОТЧЁТ
# =====================================================================

async def process_transfer_photo(message: types.Message, state: FSMContext):
    lock = await _get_photo_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        photos: List[str] = data.get("photos") or []

        if not message.photo:
            await message.answer("Не удалось получить фото, отправьте ещё раз.")
            return

        photos.append(message.photo[-1].file_id)
        await state.update_data(photos=photos)

        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(InlineKeyboardButton("Готово", callback_data="transfer_done:ok"))
        kb.row(InlineKeyboardButton("Выход", callback_data="transfer_exit"))

        await message.answer(
            f"Фото добавлено. Сейчас прикреплено: {len(photos)}",
            reply_markup=kb,
        )


async def transfer_done_photos(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    # сразу удаляем сообщение с кнопкой "Готово"
    try:
        await callback.message.delete()
    except Exception:
        pass

    data = await state.get_data()
    photos: List[str] = data.get("photos") or []
    items: List[Dict[str, Any]] = data.get("transfer_items") or []
    recipient: str = data.get("recipient", "—")
    request_number: str | None = data.get("request_number")
    warnings: List[str] = data.get("request_warnings") or []

    if not photos:
        await callback.message.answer("Нужно прикрепить хотя бы одно фото")
        return

    dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    caption_lines = [f"{dt}", "#Выдача_ТМЦ"]
    if request_number:
        caption_lines.append(f"Номер заявки: {request_number}")
    caption_lines.append(f"Кому: {recipient}")
    caption_lines.append("")

    # Список позиций
    for it in items:
        name = it["name"]
        cell = it.get("cell") or ""
        qty = int(it["quantity"])

        total_str = get_material_quantity(name)
        try:
            total = int(total_str or "0")
        except ValueError:
            total = 0

        issued_for_name_cell = sum(
            int(x["quantity"])
            for x in items
            if x["name"] == name and (x.get("cell") or "") == cell
        )
        remainder = max(total - issued_for_name_cell, 0)

        line = f"🔻 {name}"
        if cell:
            line += f" 🗂 {cell}"
        line += f"\nКоличество: {qty}\nОстаток после выдачи: {remainder} (из {total})\n"
        caption_lines.append(line)

    if warnings:
        caption_lines.append("⚠️ Предупреждения:")
        caption_lines.extend([f"• {w}" for w in warnings])

    user = callback.from_user
    tag = f"@{user.username}" if user.username else user.first_name
    caption_lines.append(f"\nОтправил: {tag}")

    caption = "\n".join(caption_lines)

    # отправка в чат склада
    if len(photos) == 1:
        sent = await bot.send_photo(
            TELEGRAM_CHAT_ID_ARRIVAL,
            photo=photos[0],
            caption=caption,
            message_thread_id=TELEGRAM_THREAD_MESSAGE_ID,
        )
    else:
        media = [InputMediaPhoto(media=photos[0], caption=caption)]
        for pid in photos[1:]:
            media.append(InputMediaPhoto(media=pid))
        msgs = await bot.send_media_group(
            TELEGRAM_CHAT_ID_ARRIVAL,
            media=media,
            message_thread_id=TELEGRAM_THREAD_MESSAGE_ID,
        )
        sent = msgs[0] if msgs else None

    # формируем ссылку
    message_link = ""
    if sent:
        cid_str = str(TELEGRAM_CHAT_ID_ARRIVAL)
        chat_link_id = cid_str[4:] if cid_str.startswith("-100") else cid_str
        message_link = f"https://t.me/c/{chat_link_id}/{sent.message_id}"

    # если выдача по заявке — обновляем статус
    if request_number:
        update_request_status(request_number, "Выдано", message_link=message_link)

    # запись в "Передача/поступление"
    write_transfer_row(
        transfer_items=items,
        recipient=recipient,
        user_tag=tag,
        message_link=message_link,
    )

    # возврат ячеек с нулевым остатком в пул свободных
    for it in items:
        name = it["name"]
        cell = it.get("cell") or ""
        total_str = get_material_quantity(name)
        try:
            total = int(total_str or "0")
        except ValueError:
            total = 0

        if total == 0 and cell:
            return_cell_to_free(cell)

    # короткий итог для пользователя
    summary_user = "Запрос на выдачу отправлен\n\n"
    for it in items:
        summary_user += f"🔻 {it['name']} — {it['quantity']} шт.\n"
    if request_number:
        summary_user += f"\nНомер заявки: {request_number}\n"

    await bot.send_message(callback.from_user.id, summary_user)

    await state.finish()



# =====================================================================
#                             ВЫХОД / ОТМЕНА
# =====================================================================

async def transfer_exit(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Отмена")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(callback.from_user.id, "Процесс выдачи ТМЦ отменён")
    await state.finish()


async def transfer_back(callback: types.CallbackQuery, state: FSMContext):
    # на всякий случай, если где-то используешь "Назад"
    await callback.answer("Шаг назад пока не реализован.")
    # здесь можно реализовать стек состояний, если понадобится


# =====================================================================
#                     РЕГИСТРАЦИЯ ХЭНДЛЕРОВ
# =====================================================================

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_transfer, commands=["transfer"], state="*")

    # Выдача по заявке? Да / Нет
    dp.register_callback_query_handler(
        process_transfer_by_request,
        lambda c: c.data.startswith("transfer_byreq:"),
        state=TransferStates.WAIT_BY_REQUEST,
    )

    # Выбор заявки и действие по ней
    dp.register_callback_query_handler(
        process_transfer_request_select,
        lambda c: c.data.startswith("transfer_reqnum:"),
        state=TransferStates.WAIT_REQUEST_SELECT,
    )
    dp.register_callback_query_handler(
        process_transfer_request_action,
        lambda c: c.data.startswith("transfer_req_action:"),
        state=TransferStates.WAIT_REQUEST_ACTION,
    )

    # Получатель
    dp.register_callback_query_handler(
        process_transfer_recipient,
        lambda c: c.data.startswith("transfer_recipient:"),
        state=TransferStates.WAIT_RECIPIENT_SELECTION,
    )

    # Локации СШМ
    dp.register_callback_query_handler(
        process_transfer_shm_company,
        lambda c: c.data.startswith("transfer_shm_company:"),
        state=TransferStates.WAIT_SHM_COMPANY,
    )
    dp.register_callback_query_handler(
        process_transfer_shm_location,
        lambda c: c.data.startswith("transfer_shm_loc:"),
        state=TransferStates.WAIT_SHM_LOCATION,
    )

    # Пагинация и выбор ТМЦ
    dp.register_callback_query_handler(
        transfer_page_prev,
        lambda c: c.data.startswith("transfer_page_prev:"),
        state=TransferStates.WAIT_MATERIAL_SELECTION,
    )
    dp.register_callback_query_handler(
        transfer_page_next,
        lambda c: c.data.startswith("transfer_page_next:"),
        state=TransferStates.WAIT_MATERIAL_SELECTION,
    )
    dp.register_callback_query_handler(
        process_transfer_selection,
        lambda c: c.data.startswith("transfer_select:"),
        state=TransferStates.WAIT_MATERIAL_SELECTION,
    )

    # Ввод наименования вручную (если используешь)
    dp.register_message_handler(
        process_transfer_manual_material,
        state=TransferStates.WAIT_MANUAL_MATERIAL,
    )

    # Выбор ячейки и количества
    dp.register_callback_query_handler(
        process_transfer_cell,
        lambda c: c.data.startswith("transfer_cell:"),
        state=TransferStates.WAIT_CELL_SELECTION,
    )
    dp.register_message_handler(
        process_transfer_quantity,
        state=TransferStates.WAIT_QUANTITY,
    )

    # Ещё ТМЦ? + подтверждение
    dp.register_callback_query_handler(
        process_transfer_more,
        lambda c: c.data in ["YES", "NO"],
        state=TransferStates.WAIT_MORE,
    )
    dp.register_callback_query_handler(
        process_transfer_confirm,
        lambda c: c.data.startswith("transfer_confirm:"),
        state=TransferStates.WAIT_CONFIRM,
    )
    dp.register_callback_query_handler(
        process_transfer_delete,
        lambda c: c.data.startswith("transfer_delete:"),
        state=TransferStates.WAIT_CONFIRM,
    )
    dp.register_callback_query_handler(
        process_transfer_fix,
        lambda c: c.data.startswith("transfer_fix:"),
        state=TransferStates.WAIT_CONFIRM,
    )

    # Фото
    dp.register_message_handler(
        process_transfer_photo,
        content_types=types.ContentType.PHOTO,
        state=TransferStates.WAIT_PHOTO,
    )
    dp.register_callback_query_handler(
        transfer_done_photos,
        lambda c: c.data.startswith("transfer_done:"),
        state=TransferStates.WAIT_PHOTO,
    )

    # Выход / Назад — глобально
    dp.register_callback_query_handler(
        transfer_exit,
        lambda c: c.data == "transfer_exit",
        state="*",
    )
    dp.register_callback_query_handler(
        transfer_back,
        lambda c: c.data == "transfer_back",
        state="*",
    )
