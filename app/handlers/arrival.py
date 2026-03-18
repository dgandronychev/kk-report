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
    get_free_cells,
    get_material_cells,
    get_material_names,
    remove_free_cell,
    write_arrival_row,
)

logger = logging.getLogger(__name__)


async def cmd_arrival_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    flow = WarehouseFlow(mode="arrival", step="arrival_name", data={"items": [], "photos": [], "page": 0})
    state.flows_by_user[user_id] = flow
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    names.append("Ввести вручную")
    await send_paginated_prompt(flow, chat_id, "Поступление ТМЦ: выберите наименование.", names, page_key="page", include_back=False)


async def _show_cells(flow: WarehouseFlow, chat_id: int, material: str) -> None:
    cells = get_material_cells(material)
    flow.data["current_name"] = material
    flow.data["current_cells"] = cells
    if cells:
        options = [f"{c.get('cell', '')} | {c.get('quantity', '')} шт" for c in cells if c.get("cell")]
        if not options:
            options = ["Ввести вручную"]
            push_step(flow, "arrival_name_manual")
            await send_prompt(flow, chat_id, "У выбранного ТМЦ нет валидных ячеек. Введите наименование заново.", options=["Ввести вручную"])
            return
        push_step(flow, "arrival_cell_existing")
        await send_prompt(flow, chat_id, f"{material}: выберите ячейку хранения.", options)
        return

    free_cells = sorted(set(get_free_cells()), key=lambda s: s.lower())
    flow.data["current_free_cells"] = free_cells
    if free_cells:
        push_step(flow, "arrival_cell_free")
        await send_paginated_prompt(flow, chat_id, f"{material}: выберите свободную ячейку.", free_cells, page_key="free_page")
        return

    push_step(flow, "arrival_cell_manual")
    await send_info(chat_id, "Свободные ячейки не найдены. Введите ячейку вручную.")


async def _show_summary(flow: WarehouseFlow, chat_id: int) -> None:
    items = flow.data.get("items") or []
    lines = ["Проверьте поступление:", ""]
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {item['name']} | {item['quantity']} шт | ячейка {item['cell']}")
    lines.append("")
    lines.append("Далее потребуется минимум 1 фото.")
    push_step(flow, "arrival_confirm")
    await send_prompt(flow, chat_id, "\n".join(lines), ["Всё ОК", "Есть ошибка"])


async def handle_arrival_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "arrival":
        return False

    if text == "warehouse_prev" or text == "warehouse_next":
        if flow.step == "arrival_name":
            return await handle_pagination(flow, chat_id, "prev" if text == "warehouse_prev" else "next", "Поступление ТМЦ: выберите наименование.")
        if flow.step == "arrival_cell_free":
            return await handle_pagination(flow, chat_id, "prev" if text == "warehouse_prev" else "next", f"{flow.data.get('current_name', '')}: выберите свободную ячейку.", page_key="free_page")
        return True

    if text == "warehouse_back":
        prev_step = pop_step(flow)
        if prev_step == "arrival_name":
            flow.step = "arrival_name"
            names = sorted(set(get_material_names()), key=lambda s: s.lower())
            names.append("Ввести вручную")
            await send_paginated_prompt(flow, chat_id, "Поступление ТМЦ: выберите наименование.", names, page_key="page")
            return True
        if prev_step in {"arrival_cell_existing", "arrival_cell_free"}:
            await _show_cells(flow, chat_id, flow.data.get("current_name", ""))
            return True
        if prev_step in {"arrival_qty", "arrival_cell_manual"}:
            flow.step = prev_step
            await send_info(chat_id, "Введите количество ещё раз." if prev_step == "arrival_qty" else "Введите ячейку вручную.")
            return True
        if prev_step in {"arrival_more", "arrival_confirm", "arrival_photo"}:
            await _show_summary(flow, chat_id)
            return True
        await send_info(chat_id, "Назад недоступно на этом шаге.")
        return True

    if text == "warehouse_exit":
        await reset_warehouse_progress(state, user_id)
        await send_info(chat_id, "Сценарий поступления завершён.")
        return True

    step = flow.step
    items = flow.data.setdefault("items", [])

    if step == "arrival_name":
        if text == "Ввести вручную":
            push_step(flow, "arrival_name_manual")
            await send_info(chat_id, "Введите наименование ТМЦ текстом.")
            return True
        await _show_cells(flow, chat_id, text.strip())
        return True

    if step == "arrival_name_manual":
        material = text.strip()
        if not material:
            await send_info(chat_id, "Наименование не может быть пустым.")
            return True
        await _show_cells(flow, chat_id, material)
        return True

    if step == "arrival_cell_existing":
        cells = flow.data.get("current_cells") or []
        selected = None
        for item in cells:
            option = f"{item.get('cell', '')} | {item.get('quantity', '')} шт"
            if option == text:
                selected = item.get("cell", "").strip()
                break
        if not selected:
            await send_info(chat_id, "Выберите ячейку кнопкой из списка.")
            return True
        flow.data["current_cell"] = selected
        flow.data["current_cell_from_free"] = False
        push_step(flow, "arrival_qty")
        await send_info(chat_id, f"Ячейка: {selected}. Введите количество.")
        return True

    if step == "arrival_cell_free":
        if text not in (flow.data.get("current_free_cells") or []):
            await send_info(chat_id, "Выберите свободную ячейку кнопкой из списка.")
            return True
        flow.data["current_cell"] = text.strip()
        flow.data["current_cell_from_free"] = True
        push_step(flow, "arrival_qty")
        await send_info(chat_id, f"Свободная ячейка: {text}. Введите количество.")
        return True

    if step == "arrival_cell_manual":
        if not text.strip():
            await send_info(chat_id, "Ячейка не может быть пустой.")
            return True
        flow.data["current_cell"] = text.strip()
        flow.data["current_cell_from_free"] = False
        push_step(flow, "arrival_qty")
        await send_info(chat_id, "Введите количество.")
        return True

    if step == "arrival_qty":
        qty = safe_int(text, -1)
        if qty <= 0:
            await send_info(chat_id, "Количество должно быть целым числом больше нуля.")
            return True
        items.append(
            {
                "name": flow.data.get("current_name", ""),
                "cell": flow.data.get("current_cell", ""),
                "quantity": str(qty),
                "from_free": bool(flow.data.get("current_cell_from_free")),
            }
        )
        for key in ("current_name", "current_cell", "current_cells", "current_free_cells", "current_cell_from_free"):
            flow.data.pop(key, None)
        push_step(flow, "arrival_more")
        await send_prompt(flow, chat_id, "Позиция добавлена. Добавить ещё?", ["Да", "Нет"])
        return True

    if step == "arrival_more":
        if text == "Да":
            flow.step = "arrival_name"
            names = sorted(set(get_material_names()), key=lambda s: s.lower())
            names.append("Ввести вручную")
            await send_paginated_prompt(flow, chat_id, "Поступление ТМЦ: выберите наименование.", names, page_key="page")
            return True
        if text == "Нет":
            await _show_summary(flow, chat_id)
            return True
        await send_info(chat_id, "Выберите Да или Нет.")
        return True

    if step == "arrival_confirm":
        if text == "Есть ошибка":
            if items:
                deleted = items.pop()
                await send_info(chat_id, f"Удалена последняя позиция: {deleted['name']} | {deleted['quantity']}.")
            if not items:
                flow.step = "arrival_name"
                names = sorted(set(get_material_names()), key=lambda s: s.lower())
                names.append("Ввести вручную")
                await send_paginated_prompt(flow, chat_id, "Поступление ТМЦ: выберите наименование.", names, page_key="page")
                return True
            await _show_summary(flow, chat_id)
            return True
        if text == "Всё ОК":
            push_step(flow, "arrival_photo")
            await send_info(chat_id, "Отправьте минимум 1 фото. Когда закончите, нажмите «Готово».")
            await send_prompt(flow, chat_id, "Приём фото для поступления.", ["Готово"])
            return True
        await send_info(chat_id, "Выберите «Всё ОК» или «Есть ошибка».")
        return True

    if step == "arrival_photo":
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
        message_link = f"max://warehouse/arrival/{datetime.now().strftime('%Y%m%d%H%M%S')}"
        write_arrival_row(items, tag, message_link)
        for item in items:
            if item.get("from_free") and item.get("cell"):
                try:
                    remove_free_cell(item["cell"])
                except Exception:
                    logger.exception("failed to remove free cell %s", item.get("cell"))
        await send_info(chat_id, "✅ Поступление ТМЦ сохранено.")
        await reset_warehouse_progress(state, user_id)
        return True

    return False
