from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import requests

from warehouse_common import (
    WarehouseFlow,
    WarehouseState,
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

from app.config import TELEGRAM_CHAT_ID_ARRIVAL, TELEGRAM_THREAD_MESSAGE_ID, URL_TASK_FIO

from app.utils.telegram_api import send_text as send_telegram_text
from app.utils.gsheets import (
    get_material_names,
    get_material_quantity,
    get_next_request_number,
    write_request_tmc_rows,
)

logger = logging.getLogger(__name__)

DEPARTMENTS = [
    "Склад",
    "Ст техник",
    "Зона ШМ",
    "Локации СШМ",
    "Запчасти техничка",
]


def build_request_summary(items: list[dict[str, str]], department: str) -> str:
    lines = ["Проверка данных:", f"Отдел: {department}", ""]
    for it in items:
        lines.append(f"🔻 {it['name']}")
        lines.append(f"Количество: {it['quantity']}")
        lines.append("")
    return "\n".join(lines).strip()


def build_request_fix_options(items: list[dict[str, str]]) -> list[str]:
    options = [f"❌ {it['name']} × {it['quantity']}" for it in items]
    options.extend(["➕ Добавить позицию", "✅ Закончить"])
    return options


async def _resolve_fullname(chat_id: int) -> str:
    def _request() -> str:
        resp = requests.get(
            URL_TASK_FIO,
            params={"max_chat_id": str(chat_id), "tg_chat_id": str(chat_id)},
            timeout=5,
            verify=False,
        )
        resp.raise_for_status()
        user = (resp.json() or {}).get("user", {})
        return str(user.get("fullname") or "").strip()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _request)


async def cmd_request_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    fullname = ""
    try:
        fullname = await _resolve_fullname(chat_id)
    except Exception:
        logger.exception("failed to resolve fullname from URL_TASK_FIO")
        await send_info(chat_id, "⚠️ Невозможно получить данные из КЛИНКа. Обратитесь к администратору.")
        return

    flow = WarehouseFlow(
        mode="request",
        step="req_department",
        data={
            "items": [],
            "fullname": fullname,
            "page": 0,
        },
    )
    state.flows_by_user[user_id] = flow
    await send_prompt(flow, chat_id, "Выберите отдел, который запрашивает выдачу:", DEPARTMENTS, include_back=False)


async def _ask_material(flow: WarehouseFlow, chat_id: int) -> None:
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    flow.step = "req_material"
    await send_paginated_prompt(flow, chat_id, "Выберите наименование ТМЦ:", names, page_key="material_page")


async def _show_fix_menu(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "req_fix"
    await send_prompt(flow, chat_id, "Выберите позицию для удаления или следующее действие:", build_request_fix_options(flow.data.get("items") or []))


async def handle_request_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "request":
        return False

    if text in {"warehouse_prev", "warehouse_next"}:
        control = "prev" if text == "warehouse_prev" else "next"
        if flow.step == "req_material":
            return await handle_pagination(flow, chat_id, control, "Выберите наименование ТМЦ:", page_key="material_page")
        return True

    if text == "warehouse_exit":
        await reset_warehouse_progress(state, user_id)
        await send_info(chat_id, "Процесс отменён")
        return True

    if text == "warehouse_back":
        prev_step = pop_step(flow)
        if prev_step == "req_department":
            flow.step = "req_department"
            await send_prompt(flow, chat_id, "Выберите отдел, который запрашивает выдачу:", DEPARTMENTS, include_back=False)
            return True
        if prev_step in {"req_material", "req_quantity", "req_overflow", "req_more"}:
            await _ask_material(flow, chat_id)
            return True
        if prev_step in {"req_confirm", "req_fix"}:
            flow.step = "req_confirm"
            summary = build_request_summary(flow.data.get("items") or [], flow.data.get("department", "—"))
            await send_prompt(flow, chat_id, summary, ["Всё ОК", "Есть ошибка"])
            return True
        await send_info(chat_id, "Назад недоступно на этом шаге.")
        return True

    items = flow.data.setdefault("items", [])
    step = flow.step

    if step == "req_department":
        if text not in DEPARTMENTS:
            await send_info(chat_id, "Выберите отдел кнопкой.")
            return True
        flow.data["department"] = text
        push_step(flow, "req_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "req_material":
        material = text.strip()
        if material not in set(get_material_names()):
            await send_info(chat_id, "Выберите ТМЦ кнопкой из списка.")
            return True
        current_qty = safe_int(get_material_quantity(material), 0)
        flow.data["current_material"] = material
        flow.data["current_qty"] = current_qty
        push_step(flow, "req_quantity")
        await send_info(
            chat_id,
            f"Вы выбрали: {material}\nТекущее количество на складе: {current_qty}\n\nВведите требуемое количество:",
        )
        return True

    if step == "req_quantity":
        qty = safe_int(text, -1)
        if qty <= 0:
            await send_info(chat_id, "Количество должно быть целым числом больше нуля. Введите ещё раз")
            return True
        current_qty = safe_int(flow.data.get("current_qty"), 0)
        if qty > current_qty:
            flow.data["pending_qty"] = qty
            diff = qty - current_qty
            push_step(flow, "req_overflow")
            await send_prompt(
                flow,
                chat_id,
                f"⚠️ Вы запрашиваете {qty} шт., но на складе только {current_qty} шт.\nНедостача: {diff} шт.\n\nВыберите действие:",
                ["Запросить всё равно", "Изменить количество"],
            )
            return True
        items.append({"name": flow.data.get("current_material", ""), "quantity": str(qty)})
        for key in ("current_material", "current_qty", "pending_qty"):
            flow.data.pop(key, None)
        push_step(flow, "req_more")
        await send_prompt(flow, chat_id, "Требуется ещё ТМЦ?", ["Да", "Нет"])
        return True

    if step == "req_overflow":
        if text == "Запросить всё равно":
            items.append({"name": flow.data.get("current_material", ""), "quantity": str(flow.data.get("pending_qty", 0))})
            for key in ("current_material", "current_qty", "pending_qty"):
                flow.data.pop(key, None)
            push_step(flow, "req_more")
            await send_prompt(flow, chat_id, "Требуется ещё ТМЦ?", ["Да", "Нет"])
            return True
        if text == "Изменить количество":
            flow.step = "req_quantity"
            await send_info(chat_id, f"На складе доступно только {flow.data.get('current_qty', 0)} шт. Введите требуемое количество ещё раз:")
            return True
        await send_info(chat_id, "Выберите действие кнопкой.")
        return True

    if step == "req_more":
        if text == "Да":
            await _ask_material(flow, chat_id)
            return True
        if text == "Нет":
            summary = build_request_summary(items, flow.data.get("department", "—"))
            push_step(flow, "req_confirm")
            await send_prompt(flow, chat_id, summary, ["Всё ОК", "Есть ошибка"])
            return True
        await send_info(chat_id, "Выберите Да или Нет.")
        return True

    if step == "req_confirm":
        if text == "Всё ОК":
            request_number = get_next_request_number()
            dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
            tag = sender_tag(msg, user_id)
            fullname = str(flow.data.get("fullname") or "-")
            department = str(flow.data.get("department") or "—")
            caption = [
                "#Запрос_ТМЦ",
                dt,
                f"Номер заявки: {request_number}",
                f"Отдел: {department}",
                "",
            ]
            for it in items:
                caption.append(f"🔻 {it['name']}")
                caption.append(f"Количество: {it['quantity']}")
                caption.append("")
            caption.append(f"Запросил: {fullname} {tag}".strip())
            text_out = "\n".join(caption).strip()
            await send_telegram_text(chat_id=TELEGRAM_CHAT_ID_ARRIVAL, text=text_out, thread_id=TELEGRAM_THREAD_MESSAGE_ID)
            message_link = f"max://warehouse/request/{request_number}"
            write_request_tmc_rows(
                request_number=request_number,
                fio=fullname,
                tag=tag,
                department=department,
                items=items,
                message_link=message_link,
            )
            lines = ["Запрос отправлен ✅", "", "📦 Список позиций:"]
            for it in items:
                lines.append(f"• {it['name']} — {it['quantity']} шт.")
            lines.append("")
            lines.append(f"Номер заявки: {request_number}")
            await send_info(chat_id, "\n".join(lines))
            await reset_warehouse_progress(state, user_id)
            return True
        if text == "Есть ошибка":
            await _show_fix_menu(flow, chat_id)
            return True
        await send_info(chat_id, "Выберите «Всё ОК» или «Есть ошибка».")
        return True

    if step == "req_fix":
        if text == "➕ Добавить позицию":
            await _ask_material(flow, chat_id)
            return True
        if text == "✅ Закончить":
            summary = build_request_summary(items, flow.data.get("department", "—"))
            flow.step = "req_confirm"
            await send_prompt(flow, chat_id, summary, ["Всё ОК", "Есть ошибка"])
            return True
        for idx, option in enumerate(build_request_fix_options(items)[: len(items)]):
            if text == option:
                items.pop(idx)
                await _show_fix_menu(flow, chat_id)
                return True
        await send_info(chat_id, "Выберите действие кнопкой.")
        return True

    return False
