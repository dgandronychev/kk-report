from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import app.config as cfg
from app.handlers.warehouse_common import (
    WarehouseFlow,
    WarehouseState,
    pop_step,
    push_step,
    reset_warehouse_progress,
    safe_int,
)
from app.utils.gsheets import load_data_rez_disk
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)
from app.utils.telegram_api import send_text as send_telegram_text

logger = logging.getLogger(__name__)

_PAGINATION_PAGE_SIZE = 20
_PAGINATION_PREV_TEXT = "<<"
_PAGINATION_PREV_PAYLOAD = "order_prev_page"
_PAGINATION_MORE_TEXT = ">>"
_PAGINATION_MORE_PAYLOAD = "order_more_page"


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


def _cfg_int(*names: str) -> Optional[int]:
    for name in names:
        value = getattr(cfg, name, None)
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            logger.warning("invalid int config for %s: %r", name, value)
    return None


def _normalize(text: str) -> str:
    return str(text or "").strip().strip("«»\"'").lower()


def _control_candidates(text: str, msg: dict) -> set[str]:
    candidates = [text]
    callback = msg.get("callback")
    if isinstance(callback, dict):
        for node in (callback, callback.get("payload") if isinstance(callback.get("payload"), dict) else None):
            if not isinstance(node, dict):
                continue
            for key in ("payload", "data", "value", "command", "action", "text"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value)
    return {_normalize(v) for v in candidates if isinstance(v, str) and v.strip()}


def _control(text: str, msg: dict) -> str:
    norms = _control_candidates(text, msg)
    if "warehouse_exit" in norms or "выход" in norms:
        return "exit"
    if "warehouse_back" in norms or "назад" in norms:
        return "back"
    if _normalize(_PAGINATION_PREV_TEXT) in norms or _normalize(_PAGINATION_PREV_PAYLOAD) in norms:
        return "prev_page"
    if _normalize(_PAGINATION_MORE_TEXT) in norms or _normalize(_PAGINATION_MORE_PAYLOAD) in norms:
        return "more"
    return ""


def _paginate_options(options: list[str], page: int, page_size: int = _PAGINATION_PAGE_SIZE) -> tuple[list[str], bool, bool, int]:
    if page_size <= 0:
        page_size = _PAGINATION_PAGE_SIZE
    total = len(options)
    if total <= 0:
        return [], False, False, 0
    max_page = max(0, (total - 1) // page_size)
    page = max(0, min(page, max_page))
    start = page * page_size
    end = min(start + page_size, total)
    return options[start:end], page > 0, end < total, page


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["warehouse_back", "warehouse_exit"]
    return ["Выход"], ["warehouse_exit"]


async def _delete_prev_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        try:
            await delete_message(chat_id, prev_msg_id)
        except Exception:
            logger.exception("failed to delete previous prompt | chat_id=%s | message_id=%s", chat_id, prev_msg_id)


async def _send_plain(flow: WarehouseFlow, chat_id: int, text: str) -> None:
    await _delete_prev_prompt(flow, chat_id)
    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask(flow: WarehouseFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls, payloads = _kb_control(include_back=include_back)
    await _delete_prev_prompt(flow, chat_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + controls,
        button_payloads=options + payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask_paginated(
    flow: WarehouseFlow,
    chat_id: int,
    text: str,
    options: list[str],
    state_key: str,
    include_back: bool = True,
    page_size: int = _PAGINATION_PAGE_SIZE,
) -> None:
    page = int(flow.data.get(f"{state_key}_page", 0) or 0)
    page_options, has_prev, has_more, page = _paginate_options(options, page, page_size)
    flow.data[f"{state_key}_options"] = list(options)
    flow.data[f"{state_key}_page"] = page
    flow.data[f"{state_key}_text"] = text

    controls, payloads = _kb_control(include_back=include_back)
    button_texts = list(page_options)
    button_payloads = list(page_options)
    if has_prev:
        button_texts.append(_PAGINATION_PREV_TEXT)
        button_payloads.append(_PAGINATION_PREV_PAYLOAD)
    if has_more:
        button_texts.append(_PAGINATION_MORE_TEXT)
        button_payloads.append(_PAGINATION_MORE_PAYLOAD)
    button_texts.extend(controls)
    button_payloads.extend(payloads)

    await _delete_prev_prompt(flow, chat_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=button_texts,
        button_payloads=button_payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


def _pagination_next_page(flow: WarehouseFlow, state_key: str) -> None:
    flow.data[f"{state_key}_page"] = int(flow.data.get(f"{state_key}_page", 0) or 0) + 1


def _pagination_prev_page(flow: WarehouseFlow, state_key: str) -> None:
    flow.data[f"{state_key}_page"] = max(0, int(flow.data.get(f"{state_key}_page", 0) or 0) - 1)


async def _reask_current_page(flow: WarehouseFlow, chat_id: int, state_key: str, include_back: bool = True) -> None:
    await _ask_paginated(
        flow,
        chat_id,
        str(flow.data.get(f"{state_key}_text") or "Выберите вариант:"),
        flow.data.get(f"{state_key}_options") or [],
        state_key=state_key,
        include_back=include_back,
    )


def _build_summary(flow: WarehouseFlow) -> str:
    return (
        f"Проверьте заказ:\n"
        f"Компания: {flow.data.get('company', '')}\n"
        f"Тип: {flow.data.get('type', '')}\n"
        f"Размерность: {flow.data.get('size', '')}\n"
        f"Наименование: {flow.data.get('name', '')}\n"
        f"Модель: {flow.data.get('model', '')}\n"
        f"Количество: {flow.data.get('quantity', '')}\n"
        f"Собрано: {flow.data.get('assembled', '')}"
    )


async def _show_company(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_company"
    await _ask(flow, chat_id, "Заказ резины/дисков: выберите компанию.", ["СитиДрайв", "Яндекс"], include_back=False)


async def _show_type(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_type"
    await _ask(flow, chat_id, "Выберите тип товара.", ["Резина", "Диск"])


async def _show_size(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_size"
    await _ask_paginated(
        flow,
        chat_id,
        "Выберите размерность:",
        _unique(flow.data.get("catalog") or [], flow.data.get("size_key")),
        state_key="size",
    )


async def _show_name(flow: WarehouseFlow, chat_id: int) -> None:
    records = [
        r for r in flow.data.get("catalog") or []
        if str(r.get(flow.data.get("size_key"), "")).strip() == str(flow.data.get("size", "")).strip()
    ]
    flow.step = "order_name"
    await _ask_paginated(
        flow,
        chat_id,
        "Выберите наименование:",
        _unique(records, flow.data.get("name_key")),
        state_key="name",
    )


async def _show_model(flow: WarehouseFlow, chat_id: int) -> None:
    records = [
        r for r in flow.data.get("catalog") or []
        if str(r.get(flow.data.get("size_key"), "")).strip() == str(flow.data.get("size", "")).strip()
        and str(r.get(flow.data.get("name_key")) or "").strip() == str(flow.data.get("name", "")).strip()
    ]
    flow.step = "order_model"
    await _ask_paginated(
        flow,
        chat_id,
        "Выберите модель:",
        _unique(records, flow.data.get("model_key")),
        state_key="model",
    )


async def _show_qty(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_qty"
    await _send_plain(flow, chat_id, f"Введите количество (доступно: {safe_int(flow.data.get('available'), 0)}):")


async def _show_assembled(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_assembled"
    await _ask(flow, chat_id, "Собрано по заявке?", ["Да", "Нет, замена..."])


async def _show_confirm(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "order_confirm"
    await _ask(flow, chat_id, _build_summary(flow), ["Подтвердить", "Отмена"])


async def _send_order_report(flow: WarehouseFlow) -> tuple[bool, bool, str]:
    company = str(flow.data.get("company") or "")

    report_text = (
        f"#Заказ_{'диска' if flow.data.get('type') == 'Диск' else 'резины'}\n\n"
        f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"Компания: {company}\n"
        f"Размерность: {flow.data.get('size', '')}\n"
        f"Наименование: {flow.data.get('name', '')}\n"
        f"Модель: {flow.data.get('model', '')}\n"
        f"Количество: {flow.data.get('quantity', '')}\n"
        f"Собрано: {'Да' if flow.data.get('assembled') == 'Да' else 'Нет, нужна замена'}"
    )

    if company == "СитиДрайв":
        max_chat_id = _cfg_int("MAX_CHAT_ID_ORDER_WHELL_ST_STORAGE")
        tg_chat_id = _cfg_int("TELEGRAM_CHAT_ID_ARRIVAL")
        tg_thread_id = _cfg_int("TELEGRAM_THREAD_MESSAGE_SKLAD_SITY")
    else:
        max_chat_id = _cfg_int("MAX_CHAT_ID_ORDER_WHELL_YND_STORAGE")
        tg_chat_id = _cfg_int("TELEGRAM_CHAT_ID_ARRIVAL")
        tg_thread_id = _cfg_int("TELEGRAM_THREAD_MESSAGE_SKLAD_YNDX")

    max_link = ""
    max_sent = False
    if max_chat_id:
        try:
            response = await send_message(chat_id=max_chat_id, text=report_text)
            message_id = extract_message_id(response)
            if message_id:
                max_link = f"max://chat/{max_chat_id}/message/{message_id}"
            max_sent = True
        except Exception:
            logger.exception("failed to send order report to MAX chat")
    else:
        logger.warning("MAX target chat for order report is not configured | company=%s", company)

    telegram_link = ""
    telegram_sent = False
    if tg_chat_id:
        try:
            telegram_link = await send_telegram_text(
                chat_id=tg_chat_id,
                text=report_text,
                thread_id=tg_thread_id,
                bot_token=getattr(cfg, "TELEGRAM_BOT_TOKEN_STORAGE", None),
            ) or ""
            telegram_sent = True
        except Exception:
            logger.exception("failed to mirror order report to Telegram")
    else:
        logger.warning("Telegram target chat for order report is not configured | company=%s", company)

    return max_sent, telegram_sent, telegram_link or max_link


async def cmd_update_orders_db(chat_id: int) -> None:
    try:
        await send_text(chat_id, "🔄 Обновляю базу заказов…")
        load_data_rez_disk()
        await send_text(chat_id, "✅ База заказов обновлена")
    except Exception:
        logger.exception("failed to update order db")
        await send_text(chat_id, "❌ Не удалось обновить базу заказов")


async def cmd_order_wheels(state: WarehouseState, user_id: int, chat_id: int) -> None:
    flow = WarehouseFlow(mode="order", step="order_company", data={})
    state.flows_by_user[user_id] = flow
    await _show_company(flow, chat_id)


async def handle_order_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "order":
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        await _delete_prev_prompt(flow, chat_id)
        await reset_warehouse_progress(state, user_id)
        await send_text(chat_id, "❌ Процесс заказа отменён.")
        return True

    if flow.step == "order_company":
        if text not in {"СитиДрайв", "Яндекс"}:
            await _show_company(flow, chat_id)
            return True
        flow.data["company"] = text
        push_step(flow, "order_type")
        await _show_type(flow, chat_id)
        return True

    if flow.step == "order_type":
        if ctrl == "back":
            await _show_company(flow, chat_id)
            return True
        if text not in {"Резина", "Диск"}:
            await _show_type(flow, chat_id)
            return True
        catalog = _catalog(flow.data["company"], text)
        flow.data.update({
            "type": text,
            "catalog": catalog,
            "size_key": _size_key(text),
            "name_key": _name_key(text),
            "model_key": _model_key(catalog) if catalog else "Модель",
        })
        push_step(flow, "order_size")
        await _show_size(flow, chat_id)
        return True

    if flow.step == "order_size":
        if ctrl == "back":
            await _show_type(flow, chat_id)
            return True
        if ctrl == "prev_page":
            _pagination_prev_page(flow, "size")
            await _reask_current_page(flow, chat_id, "size")
            return True
        if ctrl == "more":
            _pagination_next_page(flow, "size")
            await _reask_current_page(flow, chat_id, "size")
            return True
        options = set(flow.data.get("size_options") or [])
        if options and text.strip() not in options:
            await _reask_current_page(flow, chat_id, "size")
            return True
        flow.data["size"] = text.strip()
        push_step(flow, "order_name")
        await _show_name(flow, chat_id)
        return True

    if flow.step == "order_name":
        if ctrl == "back":
            await _show_size(flow, chat_id)
            return True
        if ctrl == "prev_page":
            _pagination_prev_page(flow, "name")
            await _reask_current_page(flow, chat_id, "name")
            return True
        if ctrl == "more":
            _pagination_next_page(flow, "name")
            await _reask_current_page(flow, chat_id, "name")
            return True
        options = set(flow.data.get("name_options") or [])
        if options and text.strip() not in options:
            await _reask_current_page(flow, chat_id, "name")
            return True
        flow.data["name"] = text.strip()
        push_step(flow, "order_model")
        await _show_model(flow, chat_id)
        return True

    if flow.step == "order_model":
        if ctrl == "back":
            await _show_name(flow, chat_id)
            return True
        if ctrl == "prev_page":
            _pagination_prev_page(flow, "model")
            await _reask_current_page(flow, chat_id, "model")
            return True
        if ctrl == "more":
            _pagination_next_page(flow, "model")
            await _reask_current_page(flow, chat_id, "model")
            return True
        options = set(flow.data.get("model_options") or [])
        if options and text.strip() not in options:
            await _reask_current_page(flow, chat_id, "model")
            return True
        flow.data["model"] = text.strip()
        record = next(
            (
                r
                for r in flow.data.get("catalog") or []
                if str(r.get(flow.data.get("size_key"), "")).strip() == flow.data.get("size", "")
                and str(r.get(flow.data.get("name_key")) or "").strip() == flow.data.get("name", "")
                and str(r.get(flow.data.get("model_key")) or "").strip() == flow.data["model"]
            ),
            {},
        )
        flow.data["available"] = safe_int(record.get("Текущий остаток", record.get("остаток", 0)), 0)
        push_step(flow, "order_qty")
        await _show_qty(flow, chat_id)
        return True

    if flow.step == "order_qty":
        if ctrl == "back":
            await _show_model(flow, chat_id)
            return True
        qty = safe_int(text, -1)
        available = safe_int(flow.data.get("available"), 0)
        if qty <= 0:
            await _send_plain(flow, chat_id, "Введите целое число больше нуля.")
            return True
        if qty > available:
            await _send_plain(flow, chat_id, f"Нельзя больше {available}")
            return True
        flow.data["quantity"] = qty
        push_step(flow, "order_assembled")
        await _show_assembled(flow, chat_id)
        return True

    if flow.step == "order_assembled":
        if ctrl == "back":
            await _show_qty(flow, chat_id)
            return True
        if text not in {"Да", "Нет, замена..."}:
            await _show_assembled(flow, chat_id)
            return True
        flow.data["assembled"] = text
        push_step(flow, "order_confirm")
        await _show_confirm(flow, chat_id)
        return True

    if flow.step == "order_confirm":
        if ctrl == "back":
            await _show_assembled(flow, chat_id)
            return True
        if text == "Отмена":
            await _delete_prev_prompt(flow, chat_id)
            await reset_warehouse_progress(state, user_id)
            await send_text(chat_id, "Операция отменена.")
            return True
        if text != "Подтвердить":
            await _show_confirm(flow, chat_id)
            return True

        max_sent, telegram_sent, report_link = await _send_order_report(flow)
        if not max_sent and not telegram_sent:
            await _send_plain(flow, chat_id, "❌ Не удалось отправить заказ ни в MAX, ни в Telegram.")
            return True

        if report_link:
            await _send_plain(flow, chat_id, f"✅ Заказ отправлен\n{report_link}")
        else:
            await _send_plain(flow, chat_id, "✅ Заказ отправлен")
        await reset_warehouse_progress(state, user_id)
        return True

    return False
