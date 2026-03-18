from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from max_warehouse_common import (
    WarehouseFlow,
    WarehouseState,
    extract_photo_ids,
    handle_pagination,
    pop_step,
    push_step,
    reset_warehouse_progress,
    safe_int,
    send_info,
    send_paginated_prompt,
    send_prompt,
    sender_tag,
)
from swap.gsheets import (
    get_material_cells,
    get_material_names,
    get_open_request_numbers,
    get_recipient_names,
    get_request_items,
    get_shm_locations_by_company,
    return_cell_to_free,
    update_request_status,
    write_transfer_row,
)

logger = logging.getLogger(__name__)


def auto_fill_transfer_items_from_request(request_items: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
    transfer_items: list[dict[str, str]] = []
    warnings: list[str] = []
    for req in request_items:
        name = str(req.get("name") or "").strip()
        need = safe_int(req.get("quantity"), 0)
        if not name or need <= 0:
            continue
        cells = get_material_cells(name)
        if not cells:
            warnings.append(f"{name}: нет доступных ячеек")
            continue
        remain = need
        for cell in cells:
            cell_name = str(cell.get("cell") or "").strip()
            qty = safe_int(cell.get("quantity"), 0)
            if not cell_name or qty <= 0:
                continue
            take = min(remain, qty)
            if take > 0:
                transfer_items.append({"name": name, "cell": cell_name, "quantity": str(take)})
                remain -= take
            if remain <= 0:
                break
        if remain > 0:
            warnings.append(f"{name}: не хватает {remain} шт")
    return transfer_items, warnings


async def cmd_transfer_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    flow = WarehouseFlow(mode="transfer", step="transfer_by_request", data={"items": [], "photos": [], "page": 0})
    state.flows_by_user[user_id] = flow
    await send_prompt(flow, chat_id, "Выдача ТМЦ: работать по заявке?", ["Да", "Нет"], include_back=False)


async def _ask_recipient(flow: WarehouseFlow, chat_id: int) -> None:
    recipients = sorted(set(get_recipient_names()), key=lambda s: s.lower())
    recipients.append("Ввести вручную")
    flow.step = "transfer_recipient"
    await send_paginated_prompt(flow, chat_id, "Кому выдаёте?", recipients, page_key="recipient_page")


async def _ask_material(flow: WarehouseFlow, chat_id: int) -> None:
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    names.append("Ввести вручную")
    flow.step = "transfer_material"
    await send_paginated_prompt(flow, chat_id, "Выберите ТМЦ для выдачи.", names, page_key="material_page")


async def _show_material_cells(flow: WarehouseFlow, chat_id: int, material: str) -> None:
    cells = get_material_cells(material)
    flow.data["current_name"] = material
    flow.data["current_cells"] = cells
    if not cells:
        push_step(flow, "transfer_qty")
        flow.data["current_cell"] = ""
        await send_info(chat_id, f"{material}: ячейки не найдены. Введите количество вручную.")
        return
    options = [f"{c.get('cell', '')} | {c.get('quantity', '')} шт" for c in cells if c.get("cell")]
    push_step(flow, "transfer_cell")
    await send_prompt(flow, chat_id, f"{material}: выберите ячейку.", options)


async def _show_summary(flow: WarehouseFlow, chat_id: int) -> None:
    items = flow.data.get("items") or []
    warnings = flow.data.get("warnings") or []
    lines = ["Проверьте выдачу:"]
    lines.append(f"Получатель: {flow.data.get('recipient', '')}")
    if flow.data.get("location"):
        lines.append(f"Адрес: {flow.data.get('location')}")
    lines.append("")
    for idx, item in enumerate(items, start=1):
        cell = item.get("cell") or "—"
        lines.append(f"{idx}. {item['name']} | {item['quantity']} шт | ячейка {cell}")
    if warnings:
        lines.append("")
        lines.append("Предупреждения:")
        lines.extend(f"• {w}" for w in warnings)
    lines.append("")
    lines.append("Далее потребуется минимум 1 фото.")
    push_step(flow, "transfer_confirm")
    await send_prompt(flow, chat_id, "\n".join(lines), ["Всё ОК", "Есть ошибка"])


async def handle_transfer_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "transfer":
        return False

    if text in {"warehouse_prev", "warehouse_next"}:
        control = "prev" if text == "warehouse_prev" else "next"
        if flow.step == "transfer_request_number":
            return await handle_pagination(flow, chat_id, control, "Выберите номер заявки.", page_key="request_page")
        if flow.step == "transfer_recipient":
            return await handle_pagination(flow, chat_id, control, "Кому выдаёте?", page_key="recipient_page")
        if flow.step == "transfer_material":
            return await handle_pagination(flow, chat_id, control, "Выберите ТМЦ для выдачи.", page_key="material_page")
        if flow.step == "transfer_location":
            return await handle_pagination(flow, chat_id, control, "Выберите адрес СШМ.", page_key="location_page")
        return True

    if text == "warehouse_back":
        prev_step = pop_step(flow)
        if prev_step == "transfer_by_request":
            flow.step = "transfer_by_request"
            await send_prompt(flow, chat_id, "Выдача ТМЦ: работать по заявке?", ["Да", "Нет"], include_back=False)
            return True
        if prev_step == "transfer_request_number":
            nums = sorted(set(get_open_request_numbers()))
            flow.step = "transfer_request_number"
            await send_paginated_prompt(flow, chat_id, "Выберите номер заявки.", nums, page_key="request_page")
            return True
        if prev_step == "transfer_recipient":
            await _ask_recipient(flow, chat_id)
            return True
        if prev_step == "transfer_company":
            flow.step = "transfer_company"
            await send_prompt(flow, chat_id, "Выберите каршеринг для адреса СШМ.", ["Ситидрайв", "Яндекс", "Белка"])
            return True
        if prev_step == "transfer_location":
            company = flow.data.get("company_ui", "")
            locations = get_shm_locations_by_company(company)
            flow.step = "transfer_location"
            await send_paginated_prompt(flow, chat_id, "Выберите адрес СШМ.", locations, page_key="location_page")
            return True
        if prev_step in {"transfer_material", "transfer_cell", "transfer_qty"}:
            await _ask_material(flow, chat_id)
            return True
        if prev_step in {"transfer_more", "transfer_confirm", "transfer_photo"}:
            await _show_summary(flow, chat_id)
            return True
        await send_info(chat_id, "Назад недоступно на этом шаге.")
        return True

    if text == "warehouse_exit":
        await reset_warehouse_progress(state, user_id)
        await send_info(chat_id, "Сценарий выдачи завершён.")
        return True

    step = flow.step
    items = flow.data.setdefault("items", [])

    if step == "transfer_by_request":
        if text == "Да":
            nums = sorted(set(get_open_request_numbers()))
            if not nums:
                await send_info(chat_id, "Открытых заявок нет. Переходим к обычной выдаче.")
                await _ask_recipient(flow, chat_id)
                return True
            push_step(flow, "transfer_request_number")
            await send_paginated_prompt(flow, chat_id, "Выберите номер заявки.", nums, page_key="request_page")
            return True
        if text == "Нет":
            push_step(flow, "transfer_recipient")
            await _ask_recipient(flow, chat_id)
            return True
        await send_info(chat_id, "Выберите Да или Нет.")
        return True

    if step == "transfer_request_number":
        request_number = text.strip()
        if request_number not in set(get_open_request_numbers()):
            await send_info(chat_id, "Выберите номер заявки кнопкой из списка.")
            return True
        flow.data["request_number"] = request_number
        req_items = get_request_items(request_number)
        auto_items, warnings = auto_fill_transfer_items_from_request(req_items)
        flow.data["items"] = auto_items
        flow.data["warnings"] = warnings
        push_step(flow, "transfer_recipient")
        await _ask_recipient(flow, chat_id)
        return True

    if step == "transfer_recipient":
        if text == "Ввести вручную":
            push_step(flow, "transfer_recipient_manual")
            await send_info(chat_id, "Введите получателя текстом.")
            return True
        flow.data["recipient"] = text.strip()
        if text.strip() == "ЛОКАЦИИ СШМ":
            push_step(flow, "transfer_company")
            await send_prompt(flow, chat_id, "Выберите каршеринг для адреса СШМ.", ["Ситидрайв", "Яндекс", "Белка"])
            return True
        push_step(flow, "transfer_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "transfer_recipient_manual":
        if not text.strip():
            await send_info(chat_id, "Получатель не может быть пустым.")
            return True
        flow.data["recipient"] = text.strip()
        push_step(flow, "transfer_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "transfer_company":
        if text not in {"Ситидрайв", "Яндекс", "Белка"}:
            await send_info(chat_id, "Выберите компанию кнопкой.")
            return True
        flow.data["company_ui"] = text
        locations = get_shm_locations_by_company(text)
        if not locations:
            await send_info(chat_id, "Для этой компании нет адресов СШМ. Введите получателя заново.")
            await _ask_recipient(flow, chat_id)
            return True
        push_step(flow, "transfer_location")
        await send_paginated_prompt(flow, chat_id, "Выберите адрес СШМ.", locations, page_key="location_page")
        return True

    if step == "transfer_location":
        flow.data["location"] = text.strip()
        flow.data["recipient"] = f"ЛОКАЦИИ СШМ | {text.strip()}"
        push_step(flow, "transfer_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "transfer_material":
        if text == "Ввести вручную":
            push_step(flow, "transfer_material_manual")
            await send_info(chat_id, "Введите наименование ТМЦ текстом.")
            return True
        await _show_material_cells(flow, chat_id, text.strip())
        return True

    if step == "transfer_material_manual":
        material = text.strip()
        if not material:
            await send_info(chat_id, "Наименование не может быть пустым.")
            return True
        flow.data["current_name"] = material
        flow.data["current_cell"] = ""
        push_step(flow, "transfer_qty")
        await send_info(chat_id, "Введите количество для выдачи.")
        return True

    if step == "transfer_cell":
        selected = None
        for item in flow.data.get("current_cells") or []:
            option = f"{item.get('cell', '')} | {item.get('quantity', '')} шт"
            if option == text:
                selected = item.get("cell", "").strip()
                break
        if not selected:
            await send_info(chat_id, "Выберите ячейку кнопкой из списка.")
            return True
        flow.data["current_cell"] = selected
        push_step(flow, "transfer_qty")
        await send_info(chat_id, f"Ячейка: {selected}. Введите количество.")
        return True

    if step == "transfer_qty":
        qty = safe_int(text, -1)
        if qty <= 0:
            await send_info(chat_id, "Количество должно быть целым числом больше нуля.")
            return True
        material = flow.data.get("current_name", "")
        cell = flow.data.get("current_cell", "")
        available = 0
        if material and cell:
            for item in get_material_cells(material):
                if str(item.get("cell") or "").strip() == cell:
                    available = safe_int(item.get("quantity"), 0)
                    break
            already = sum(
                safe_int(x.get("quantity"), 0)
                for x in items
                if x.get("name") == material and x.get("cell") == cell
            )
            available = max(available - already, 0)
            if qty > available:
                await send_info(chat_id, f"В выбранной ячейке доступно только {available} шт. Введите число заново.")
                return True
        items.append({"name": material, "cell": cell, "quantity": str(qty)})
        for key in ("current_name", "current_cell", "current_cells"):
            flow.data.pop(key, None)
        push_step(flow, "transfer_more")
        await send_prompt(flow, chat_id, "Позиция добавлена. Добавить ещё?", ["Да", "Нет"])
        return True

    if step == "transfer_more":
        if text == "Да":
            await _ask_material(flow, chat_id)
            return True
        if text == "Нет":
            await _show_summary(flow, chat_id)
            return True
        await send_info(chat_id, "Выберите Да или Нет.")
        return True

    if step == "transfer_confirm":
        if text == "Есть ошибка":
            if items:
                deleted = items.pop()
                await send_info(chat_id, f"Удалена последняя позиция: {deleted['name']} | {deleted['quantity']}.")
            if not items:
                await _ask_material(flow, chat_id)
                return True
            await _show_summary(flow, chat_id)
            return True
        if text == "Всё ОК":
            push_step(flow, "transfer_photo")
            await send_info(chat_id, "Отправьте минимум 1 фото. Когда закончите, нажмите «Готово».")
            await send_prompt(flow, chat_id, "Приём фото для выдачи.", ["Готово"])
            return True
        await send_info(chat_id, "Выберите «Всё ОК» или «Есть ошибка».")
        return True

    if step == "transfer_photo":
        photo_ids = extract_photo_ids(msg)
        photos = flow.data.setdefault("photos", [])
        if photo_ids:
            photos.extend(photo_ids)
            await send_info(chat_id, f"Фото получено: {len(photos)}")
            return True
        if text != "Готово":
            await send_info(chat_id, "Отправьте фото или нажмите «Готово».")
            return True
        if not photos:
            await send_info(chat_id, "Нужно минимум 1 фото.")
            return True
        tag = sender_tag(msg, user_id)
        message_link = f"max://warehouse/transfer/{datetime.now().strftime('%Y%m%d%H%M%S')}"
        write_transfer_row(items, flow.data.get("recipient", ""), tag, message_link)
        for item in items:
            if item.get("cell"):
                if safe_int(item.get("quantity"), 0) == 0:
                    continue
                # возврат пустой ячейки зависит от gsheets-логики; вызываем только при нулевом остатке в таблице после выдачи.
                # Здесь условие вычислить нельзя без доп. функции, поэтому оставляем только ручной вызов в gsheets после записи.
                pass
        request_number = flow.data.get("request_number")
        if request_number:
            update_request_status(request_number, "Выдано", message_link)
        await send_info(chat_id, "✅ Выдача ТМЦ сохранена.")
        await reset_warehouse_progress(state, user_id)
        return True

    return False
