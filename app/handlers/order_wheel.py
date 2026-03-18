from __future__ import annotations

import logging
from datetime import datetime

import app.config as cfg
from app.utils.telegram_api import send_text as send_telegram_text
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
)
from app.config import CHAT_ID_ARRIVAL, TELEGRAM_THREAD_MESSAGE_SKLAD_SITY, TELEGRAM_THREAD_MESSAGE_SKLAD_YNDX
from app.utils.gsheets import load_data_rez_disk

logger = logging.getLogger(__name__)


def _catalog(company: str, order_type: str) -> list[dict]:
    if company == "СитиДрайв" and order_type == "Диск":
        return cfg.BAZA_DISK_SITY
    if company == "СитиДрайв" and order_type == "Резина":
        return cfg.BAZA_REZN_SITY
    if company == "Яндекс" and order_type == "Диск":
        return cfg.BAZA_DISK_YNDX
    return cfg.BAZA_REZN_YNDX


def _size_key(order_type: str) -> str:
    return "Размер резины" if order_type == "Диск" else "Размерность"


def _name_key(order_type: str) -> str:
    return "наименование" if order_type == "Диск" else "Наименование"


def _model_key(records: list[dict]) -> str:
    return "Модель авто" if any("Модель авто" in it for it in records) else "Модель"


def _unique(records: list[dict], key: str) -> list[str]:
    out = {str(r.get(key) or "").strip() for r in records if str(r.get(key) or "").strip()}
    return sorted(out, key=lambda s: s.lower())


async def cmd_update_orders_db(chat_id: int) -> None:
    await send_info(chat_id, "🔄 Обновляю базу заказов…")
    try:
        load_data_rez_disk()
        await send_info(chat_id, "✅ База заказов обновлена")
    except Exception:
        logger.exception("failed to update order db")
        await send_info(chat_id, "❌ Не удалось обновить базу заказов")


async def cmd_order_wheels(state: WarehouseState, user_id: int, chat_id: int) -> None:
    flow = WarehouseFlow(mode="order", step="order_company", data={})
    state.flows_by_user[user_id] = flow
    await send_prompt(flow, chat_id, "Заказ резины/дисков: выберите компанию.", ["СитиДрайв", "Яндекс"], include_back=False)


async def handle_order_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "order":
        return False

    if text in {"warehouse_prev", "warehouse_next"}:
        control = "prev" if text == "warehouse_prev" else "next"
        if flow.step == "order_size":
            return await handle_pagination(flow, chat_id, control, "Выберите размерность:", page_key="size_page")
        if flow.step == "order_name":
            return await handle_pagination(flow, chat_id, control, "Выберите наименование:", page_key="name_page")
        if flow.step == "order_model":
            return await handle_pagination(flow, chat_id, control, "Выберите модель:", page_key="model_page")
        return True

    if text == "warehouse_exit":
        await reset_warehouse_progress(state, user_id)
        await send_info(chat_id, "❌ Процесс заказа отменён.")
        return True

    if text == "warehouse_back":
        prev_step = pop_step(flow)
        if prev_step == "order_company":
            flow.step = "order_company"
            await send_prompt(flow, chat_id, "Заказ резины/дисков: выберите компанию.", ["СитиДрайв", "Яндекс"], include_back=False)
            return True
        if prev_step == "order_type":
            flow.step = "order_type"
            await send_prompt(flow, chat_id, "Выберите тип товара.", ["Резина", "Диск"])
            return True
        if prev_step == "order_size":
            flow.step = "order_size"
            await send_paginated_prompt(flow, chat_id, "Выберите размерность:", _unique(flow.data.get("catalog") or [], flow.data.get("size_key")), page_key="size_page")
            return True
        if prev_step == "order_name":
            records = [r for r in flow.data.get("catalog") or [] if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data.get("size", "")]
            flow.step = "order_name"
            await send_paginated_prompt(flow, chat_id, "Выберите наименование:", _unique(records, flow.data.get("name_key")), page_key="name_page")
            return True
        if prev_step == "order_model":
            records = [
                r for r in flow.data.get("catalog") or []
                if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data.get("size", "")
                and str(r.get(flow.data.get("name_key")) or "").strip() == flow.data.get("name", "")
            ]
            flow.step = "order_model"
            await send_paginated_prompt(flow, chat_id, "Выберите модель:", _unique(records, flow.data.get("model_key")), page_key="model_page")
            return True
        if prev_step in {"order_qty", "order_assembled", "order_confirm"}:
            flow.step = prev_step
            await send_info(chat_id, "Вернулись на предыдущий шаг.")
            return True
        return True

    step = flow.step

    if step == "order_company":
        if text not in {"СитиДрайв", "Яндекс"}:
            await send_info(chat_id, "Выберите компанию кнопкой.")
            return True
        flow.data["company"] = text
        push_step(flow, "order_type")
        await send_prompt(flow, chat_id, "Выберите тип товара.", ["Резина", "Диск"])
        return True

    if step == "order_type":
        if text not in {"Резина", "Диск"}:
            await send_info(chat_id, "Выберите тип товара кнопкой.")
            return True
        catalog = _catalog(flow.data["company"], text)
        size_key = _size_key(text)
        name_key = _name_key(text)
        model_key = _model_key(catalog) if catalog else "Модель"
        sizes = _unique(catalog, size_key)
        flow.data.update({"type": text, "catalog": catalog, "size_key": size_key, "name_key": name_key, "model_key": model_key})
        push_step(flow, "order_size")
        await send_paginated_prompt(flow, chat_id, "Выберите размерность:", sizes, page_key="size_page")
        return True

    if step == "order_size":
        flow.data["size"] = text.strip()
        records = [r for r in flow.data.get("catalog") or [] if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data["size"]]
        names = _unique(records, flow.data.get("name_key"))
        push_step(flow, "order_name")
        await send_paginated_prompt(flow, chat_id, "Выберите наименование:", names, page_key="name_page")
        return True

    if step == "order_name":
        flow.data["name"] = text.strip()
        records = [
            r for r in flow.data.get("catalog") or []
            if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data.get("size", "")
            and str(r.get(flow.data.get("name_key")) or "").strip() == flow.data["name"]
        ]
        push_step(flow, "order_model")
        await send_paginated_prompt(flow, chat_id, "Выберите модель:", _unique(records, flow.data.get("model_key")), page_key="model_page")
        return True

    if step == "order_model":
        flow.data["model"] = text.strip()
        record = next(
            (
                r for r in flow.data.get("catalog") or []
                if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data.get("size", "")
                and str(r.get(flow.data.get("name_key")) or "").strip() == flow.data.get("name", "")
                and str(r.get(flow.data.get("model_key")) or "").strip() == flow.data["model"]
            ),
            {},
        )
        available = safe_int(record.get("Текущий остаток", record.get("остаток", 0)), 0)
        flow.data["available"] = available
        push_step(flow, "order_qty")
        await send_info(chat_id, f"Введите количество (доступно: {available}):")
        return True

    if step == "order_qty":
        qty = safe_int(text, -1)
        available = safe_int(flow.data.get("available"), 0)
        if qty <= 0:
            await send_info(chat_id, "Введите целое число больше нуля.")
            return True
        if qty > available:
            await send_info(chat_id, f"Нельзя больше {available}")
            return True
        flow.data["quantity"] = qty
        push_step(flow, "order_assembled")
        await send_prompt(flow, chat_id, "Собрано по заявке?", ["Да", "Нет, замена..."])
        return True

    if step == "order_assembled":
        if text not in {"Да", "Нет, замена..."}:
            await send_info(chat_id, "Выберите вариант кнопкой.")
            return True
        flow.data["assembled"] = text
        summary = (
            f"Проверьте заказ:\n"
            f"Компания: {flow.data.get('company', '')}\n"
            f"Тип: {flow.data.get('type', '')}\n"
            f"Размерность: {flow.data.get('size', '')}\n"
            f"Наименование: {flow.data.get('name', '')}\n"
            f"Модель: {flow.data.get('model', '')}\n"
            f"Количество: {flow.data.get('quantity', '')}\n"
            f"Собрано: {flow.data.get('assembled', '')}"
        )
        push_step(flow, "order_confirm")
        await send_prompt(flow, chat_id, summary, ["Подтвердить", "Отмена"])
        return True

    if step == "order_confirm":
        if text == "Отмена":
            await reset_warehouse_progress(state, user_id)
            await send_info(chat_id, "Операция отменена.")
            return True
        if text != "Подтвердить":
            await send_info(chat_id, "Выберите «Подтвердить» или «Отмена».")
            return True
        company = flow.data.get("company", "")
        thread_id = TELEGRAM_THREAD_MESSAGE_SKLAD_SITY if company == "СитиДрайв" else TELEGRAM_THREAD_MESSAGE_SKLAD_YNDX
        report_text = (
            f"#Заказ_{'диска' if flow.data.get('type') == 'Диск' else 'резины'}\n\n"
            f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"Размерность: {flow.data.get('size', '')}\n"
            f"Наименование: {flow.data.get('name', '')}\n"
            f"Модель: {flow.data.get('model', '')}\n"
            f"Количество: {flow.data.get('quantity', '')}\n"
            f"Собрано: {'Да' if flow.data.get('assembled') == 'Да' else 'Нет, нужна замена'}"
        )
        await send_telegram_text(chat_id=CHAT_ID_ARRIVAL, text=report_text, thread_id=thread_id)
        await send_info(chat_id, "✅ Заказ отправлен.")
        await reset_warehouse_progress(state, user_id)
        return True

    return False
