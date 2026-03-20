from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import requests

import app.config as cfg
from app.handlers.warehouse_common import (
    WarehouseFlow,
    WarehouseState,
    reset_warehouse_progress,
    safe_int,
    sender_tag,
)
from app.utils.gsheets import (
    get_material_names,
    get_material_quantity,
    get_next_request_number,
    write_request_tmc_rows,
)
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)
from app.utils.telegram_api import send_text as send_telegram_text

logger = logging.getLogger(__name__)

DEPARTMENTS = [
    "Склад",
    "Ст техник",
    "Зона ШМ",
    "Локации СШМ",
    "Запчасти техничка",
]

_PAGINATION_PAGE_SIZE = 20
_PAGINATION_PREV_TEXT = "<<"
_PAGINATION_PREV_PAYLOAD = "req_prev_page"
_PAGINATION_MORE_TEXT = ">>"
_PAGINATION_MORE_PAYLOAD = "req_more_page"


def _cfg_first(*names: str, default=None):
    for name in names:
        if hasattr(cfg, name):
            value = getattr(cfg, name)
            if value not in (None, ""):
                return value
    return default


REQUEST_MAX_CHAT_ID = _cfg_first(
    "REQUEST_TMC_MAX_CHAT_ID",
    "MAX_CHAT_ID_REQUEST_TMC",
    "MAX_CHAT_ID_ARRIVAL",
    "CHAT_ID_ARRIVAL",
)
TELEGRAM_CHAT_ID_REQUEST_TMC = _cfg_first(
    "TELEGRAM_CHAT_ID_REQUEST_TMC",
    "TG_CHAT_ID_REQUEST_TMC",
    "TELEGRAM_CHAT_ID_ARRIVAL",
    "CHAT_ID_ARRIVAL",
)
TELEGRAM_THREAD_ID_REQUEST_TMC = _cfg_first(
    "TELEGRAM_THREAD_ID_REQUEST_TMC",
    "TELEGRAM_THREAD_MESSAGE_ID_REQUEST_TMC",
    "TELEGRAM_THREAD_MESSAGE_ID",
    "THREAD_MESSAGE_ID",
)
TELEGRAM_BOT_TOKEN_REQUEST_TMC = _cfg_first(
    "TELEGRAM_BOT_TOKEN_TECHNIK",
    "TELEGRAM_BOT_TOKEN",
    "TOKEN_BOT",
)
URL_FIO = _cfg_first("URL_GET_FIO", "URL_TASK_FIO")


def _normalize(text: str) -> str:
    return str(text or "").strip().strip("«»\"'").lower()


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["warehouse_back", "warehouse_exit"]
    return ["Выход"], ["warehouse_exit"]


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
    has_prev = page > 0
    has_more = end < total
    return options[start:end], has_prev, has_more, page


async def _delete_prev_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        try:
            await delete_message(chat_id, prev_msg_id)
        except Exception:
            logger.exception("failed to delete previous request_tmc prompt")


async def _ask(flow: WarehouseFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls, payloads = _kb_control(include_back=include_back)
    await _delete_prev_prompt(flow, chat_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=list(options) + controls,
        button_payloads=list(options) + payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_plain(flow: WarehouseFlow, chat_id: int, text: str) -> None:
    await _delete_prev_prompt(flow, chat_id)
    response = await send_message(chat_id=chat_id, text=text)
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
    page_options, has_prev, has_more, page = _paginate_options(options, page, page_size=page_size)
    flow.data[f"{state_key}_options"] = list(options)
    flow.data[f"{state_key}_page"] = page

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



def _pagination_reset(flow: WarehouseFlow, state_key: str) -> None:
    flow.data.pop(f"{state_key}_page", None)
    flow.data.pop(f"{state_key}_options", None)



def _control_candidates(text: str, msg: dict) -> set[str]:
    candidates = [text]
    callback = msg.get("callback")
    if isinstance(callback, dict):
        payload = callback.get("payload")
        nodes = [callback]
        if isinstance(payload, dict):
            nodes.append(payload)
        for node in nodes:
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
    if not URL_FIO:
        raise RuntimeError("Не настроен URL получения ФИО")

    def _request() -> str:
        resp = requests.get(
            URL_FIO,
            params={"max_chat_id": str(chat_id), "tg_chat_id": str(chat_id)},
            timeout=5,
            verify=False,
        )
        resp.raise_for_status()
        user = (resp.json() or {}).get("user", {})
        return str(user.get("fullname") or "").strip()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _request)



def _render_request_report(request_number: str, fullname: str, tag: str, department: str, items: list[dict[str, str]]) -> str:
    dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    lines = [
        "#Запрос_ТМЦ",
        dt,
        f"Номер заявки: {request_number}",
        f"Отдел: {department}",
        "",
    ]
    for it in items:
        lines.append(f"🔻 {it['name']}")
        lines.append(f"Количество: {it['quantity']}")
        lines.append("")
    lines.append(f"Запросил: {fullname} {tag}".strip())
    return "\n".join(lines).strip()


async def _ask_department(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "req_department"
    await _ask(flow, chat_id, "Выберите отдел, который запрашивает выдачу:", DEPARTMENTS, include_back=False)


async def _ask_material(flow: WarehouseFlow, chat_id: int) -> None:
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    flow.step = "req_material"
    flow.data["material_prompt_text"] = "Выберите наименование ТМЦ:"
    flow.data["material_options"] = names
    await _ask_paginated(flow, chat_id, "Выберите наименование ТМЦ:", names, state_key="material", include_back=True)


async def _show_fix_menu(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "req_fix"
    flow.data["fix_prompt_text"] = "Выберите позицию для удаления или следующее действие:"
    flow.data["fix_options"] = build_request_fix_options(flow.data.get("items") or [])
    flow.data["fix_page"] = 0
    await _send_fix_prompt(flow, chat_id)


async def _send_more_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "req_more"
    await _ask(flow, chat_id, "Требуется ещё ТМЦ?", ["Да", "Нет"])


async def _send_overflow_prompt(flow: WarehouseFlow, chat_id: int, qty: int, current_qty: int) -> None:
    flow.step = "req_overflow"
    diff = qty - current_qty
    text = (
        f"⚠️ Вы запрашиваете {qty} шт., но на складе только {current_qty} шт.\n"
        f"Недостача: {diff} шт.\n\n"
        f"Выберите действие:"
    )
    await _ask(flow, chat_id, text, ["Запросить всё равно", "Изменить количество"])


async def _send_review_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "req_confirm"
    summary = build_request_summary(flow.data.get("items") or [], flow.data.get("department", "—"))
    await _ask(flow, chat_id, summary, ["Всё ОК", "Есть ошибка"])


async def _send_fix_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    options = build_request_fix_options(flow.data.get("items") or [])
    flow.data["fix_options"] = options
    await _ask_paginated(
        flow,
        chat_id,
        "Выберите позицию для удаления или следующее действие:",
        options,
        state_key="fix",
        include_back=False,
    )


async def cmd_request_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    fullname = ""
    try:
        fullname = await _resolve_fullname(chat_id)
    except Exception:
        logger.exception("failed to resolve fullname from clinck")
        await send_text(chat_id, "⚠️ Невозможно получить данные из КЛИНКа. Обратитесь к администратору.")
        return

    logger.info("[request_tmc] start chat_id=%s user_id=%s fullname=%s", chat_id, user_id, fullname)
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
    await _ask_department(flow, chat_id)


async def handle_request_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "request":
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        await reset_warehouse_progress(state, user_id)
        await send_text(chat_id, "Процесс отменён")
        return True

    items = flow.data.setdefault("items", [])
    step = flow.step

    if step == "req_department":
        if text not in DEPARTMENTS:
            await _ask_department(flow, chat_id)
            return True
        flow.data["department"] = text
        await _ask_material(flow, chat_id)
        return True

    if step == "req_material":
        if ctrl == "back":
            await _ask_department(flow, chat_id)
            return True
        if ctrl == "prev_page":
            _pagination_prev_page(flow, "material")
            await _ask_paginated(
                flow,
                chat_id,
                str(flow.data.get("material_prompt_text") or "Выберите наименование ТМЦ:"),
                flow.data.get("material_options") or [],
                state_key="material",
                include_back=True,
            )
            return True
        if ctrl == "more":
            _pagination_next_page(flow, "material")
            await _ask_paginated(
                flow,
                chat_id,
                str(flow.data.get("material_prompt_text") or "Выберите наименование ТМЦ:"),
                flow.data.get("material_options") or [],
                state_key="material",
                include_back=True,
            )
            return True

        material = text.strip()
        options = set(flow.data.get("material_options") or get_material_names())
        if material not in options:
            await _ask_paginated(
                flow,
                chat_id,
                str(flow.data.get("material_prompt_text") or "Выберите наименование ТМЦ:"),
                flow.data.get("material_options") or [],
                state_key="material",
                include_back=True,
            )
            return True
        current_qty = safe_int(get_material_quantity(material), 0)
        flow.data["current_material"] = material
        flow.data["current_qty"] = current_qty
        flow.step = "req_quantity"
        await _send_plain(flow, chat_id, f"Вы выбрали: {material}\nТекущее количество на складе: {current_qty}\n\nВведите требуемое количество:")
        return True

    if step == "req_quantity":
        if ctrl == "back":
            await _ask_material(flow, chat_id)
            return True
        qty = safe_int(text, -1)
        if qty <= 0:
            await _send_plain(flow, chat_id, "Количество должно быть целым числом больше нуля. Введите ещё раз")
            return True
        current_qty = safe_int(flow.data.get("current_qty"), 0)
        if qty > current_qty:
            flow.data["pending_qty"] = qty
            await _send_overflow_prompt(flow, chat_id, qty, current_qty)
            return True
        item = {"name": flow.data.get("current_material", ""), "quantity": str(qty)}
        items.append(item)
        logger.info("[request_tmc] add item chat_id=%s item=%s", chat_id, item)
        for key in ("current_material", "current_qty", "pending_qty"):
            flow.data.pop(key, None)
        await _send_more_prompt(flow, chat_id)
        return True

    if step == "req_overflow":
        if ctrl == "back":
            flow.step = "req_quantity"
            await _send_plain(flow, chat_id, f"На складе доступно только {flow.data.get('current_qty', 0)} шт. Введите требуемое количество ещё раз:")
            return True
        if text == "Запросить всё равно":
            item = {"name": flow.data.get("current_material", ""), "quantity": str(flow.data.get("pending_qty", 0))}
            items.append(item)
            logger.info("[request_tmc] add overflow item chat_id=%s item=%s", chat_id, item)
            for key in ("current_material", "current_qty", "pending_qty"):
                flow.data.pop(key, None)
            await _send_more_prompt(flow, chat_id)
            return True
        if text == "Изменить количество":
            flow.step = "req_quantity"
            await _send_plain(flow, chat_id, f"На складе доступно только {flow.data.get('current_qty', 0)} шт. Введите требуемое количество ещё раз:")
            return True
        await _send_overflow_prompt(flow, chat_id, int(flow.data.get("pending_qty", 0) or 0), int(flow.data.get("current_qty", 0) or 0))
        return True

    if step == "req_more":
        if ctrl == "back":
            if items:
                removed = items.pop()
                logger.info("[request_tmc] remove last item by back chat_id=%s item=%s", chat_id, removed)
            await _ask_material(flow, chat_id)
            return True
        if text == "Да":
            await _ask_material(flow, chat_id)
            return True
        if text == "Нет":
            await _send_review_prompt(flow, chat_id)
            return True
        await _send_more_prompt(flow, chat_id)
        return True

    if step == "req_confirm":
        if ctrl == "back":
            await _send_more_prompt(flow, chat_id)
            return True
        if text == "Всё ОК":
            request_number = get_next_request_number()
            tag = sender_tag(msg, user_id)
            fullname = str(flow.data.get("fullname") or "-")
            department = str(flow.data.get("department") or "—")
            text_out = _render_request_report(request_number, fullname, tag, department, items)

            max_link = ""
            max_sent = False
            if REQUEST_MAX_CHAT_ID:
                try:
                    response = await send_message(chat_id=int(REQUEST_MAX_CHAT_ID), text=text_out)
                    message_id = extract_message_id(response)
                    if message_id:
                        max_link = f"max://chat/{REQUEST_MAX_CHAT_ID}/message/{message_id}"
                    max_sent = True
                except Exception:
                    logger.exception("failed to send request_tmc report to MAX")

            telegram_link = ""
            telegram_sent = False
            if TELEGRAM_CHAT_ID_REQUEST_TMC and TELEGRAM_BOT_TOKEN_REQUEST_TMC:
                try:
                    telegram_link = await send_telegram_text(
                        chat_id=int(TELEGRAM_CHAT_ID_REQUEST_TMC),
                        text=text_out,
                        thread_id=int(TELEGRAM_THREAD_ID_REQUEST_TMC) if TELEGRAM_THREAD_ID_REQUEST_TMC else None,
                        bot_token=str(TELEGRAM_BOT_TOKEN_REQUEST_TMC),
                    ) or ""
                    telegram_sent = True
                except Exception:
                    logger.exception("failed to mirror request_tmc report to Telegram")

            if not max_sent and not telegram_sent:
                await _send_plain(flow, chat_id, "Не удалось отправить заявку в чаты. Запись в таблицу не выполнена. Обратитесь к разработчикам.")
                return True

            report_link = telegram_link or max_link
            try:
                write_request_tmc_rows(
                    request_number=request_number,
                    fio=fullname,
                    tag=tag,
                    department=department,
                    items=items,
                    message_link=report_link,
                )
                logger.info("[request_tmc] saved request_number=%s report_link=%s items=%s", request_number, report_link, items)
            except Exception:
                logger.exception("failed to write request_tmc rows")
                await _send_plain(flow, chat_id, "Заявка отправлена в чат, но при записи в таблицу произошла ошибка. Обратитесь к разработчикам.")
                return True

            lines = ["Запрос отправлен ✅", "", "📦 Список позиций:"]
            for it in items:
                lines.append(f"• {it['name']} — {it['quantity']} шт.")
            lines.append("")
            lines.append(f"Номер заявки: {request_number}")
            if report_link:
                lines.append(f"Ссылка на отчёт: {report_link}")
            await _send_plain(flow, chat_id, "\n".join(lines))
            await reset_warehouse_progress(state, user_id)
            return True
        if text == "Есть ошибка":
            await _show_fix_menu(flow, chat_id)
            return True
        await _send_review_prompt(flow, chat_id)
        return True

    if step == "req_fix":
        if ctrl == "prev_page":
            _pagination_prev_page(flow, "fix")
            await _send_fix_prompt(flow, chat_id)
            return True
        if ctrl == "more":
            _pagination_next_page(flow, "fix")
            await _send_fix_prompt(flow, chat_id)
            return True
        if ctrl == "back":
            await _send_review_prompt(flow, chat_id)
            return True
        if text == "➕ Добавить позицию":
            await _ask_material(flow, chat_id)
            return True
        if text == "✅ Закончить":
            await _send_review_prompt(flow, chat_id)
            return True
        options = build_request_fix_options(items)
        if text not in options:
            await _send_fix_prompt(flow, chat_id)
            return True
        idx = options.index(text)
        if 0 <= idx < len(items):
            removed = items.pop(idx)
            logger.info("[request_tmc] remove item chat_id=%s item=%s", chat_id, removed)
        if not items:
            await _ask_material(flow, chat_id)
            return True
        await _send_fix_prompt(flow, chat_id)
        return True

    return False
