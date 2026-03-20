from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import app.config as cfg
from app.handlers.warehouse_common import (
    WarehouseFlow,
    WarehouseState,
    pop_step,
    push_step,
    reset_warehouse_progress,
    safe_int,
    sender_tag,
)
from app.utils.gsheets import (
    get_material_cells,
    get_material_names,
    get_material_quantity,
    get_open_request_numbers,
    get_recipient_names,
    get_request_items,
    get_shm_locations_by_company,
    return_cell_to_free,
    update_request_status,
    write_transfer_row,
)
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)
from app.utils.telegram_api import send_report as send_telegram_report

logger = logging.getLogger(__name__)

_PAGINATION_PAGE_SIZE = 20
_PAGINATION_PREV_TEXT = "<<"
_PAGINATION_PREV_PAYLOAD = "transfer_prev_page"
_PAGINATION_MORE_TEXT = ">>"
_PAGINATION_MORE_PAYLOAD = "transfer_more_page"


def _normalize(text: str) -> str:
    return str(text or "").strip().strip("«»\"'").lower()


def _cfg_int(*names: str) -> int | None:
    for name in names:
        value = getattr(cfg, name, None)
        if value in (None, ""):
            continue
        try:
            return int(str(value).strip())
        except Exception:
            continue
    return None


def _cfg_str(*names: str) -> str | None:
    for name in names:
        value = getattr(cfg, name, None)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    texts: list[str] = []
    payloads: list[str] = []
    if include_back:
        texts.append("Назад")
        payloads.append("transfer_back")
    texts.append("Выход")
    payloads.append("transfer_exit")
    return texts, payloads


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


async def _ask(
    flow: WarehouseFlow,
    chat_id: int,
    text: str,
    options: list[str],
    *,
    include_back: bool = True,
) -> None:
    controls, payloads = _kb_control(include_back=include_back)
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=list(options) + controls,
        button_payloads=list(options) + payloads,
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
    *,
    include_back: bool = True,
    page_size: int = _PAGINATION_PAGE_SIZE,
) -> None:
    page = int(flow.data.get(f"{state_key}_page", 0) or 0)
    page_options, has_prev, has_more, page = _paginate_options(options, page, page_size=page_size)
    flow.data[f"{state_key}_page"] = page
    flow.data[f"{state_key}_options"] = list(options)

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

    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=button_texts,
        button_payloads=button_payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_plain(flow: WarehouseFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_files_prompt(flow: WarehouseFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=["Готово", "Назад", "Выход"],
        button_payloads=["transfer_done", "transfer_back", "transfer_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_need_more_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text="Позиция добавлена. Добавить ещё?",
        button_texts=["Да", "Нет", "Назад", "Выход"],
        button_payloads=["Да", "Нет", "transfer_back", "transfer_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_review_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    text = await build_summary(
        flow.data.get("items") or [],
        flow.data.get("recipient", ""),
        warnings=flow.data.get("warnings") or [],
    )
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=["Всё ОК", "Есть ошибка", "Назад", "Выход"],
        button_payloads=["Всё ОК", "Есть ошибка", "transfer_back", "transfer_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_delete_prompt(flow: WarehouseFlow, chat_id: int) -> None:
    items = flow.data.get("items") or []
    lines = build_transfer_fix_options(items)
    flow.step = "transfer_fix"
    flow.data["delete_lines"] = list(lines)
    page = int(flow.data.get("delete_page", 0) or 0)
    page_lines, has_prev, has_more, page = _paginate_options(lines, page)
    flow.data["delete_page"] = page

    button_texts = list(page_lines)
    button_payloads = list(page_lines)
    if has_prev:
        button_texts.append(_PAGINATION_PREV_TEXT)
        button_payloads.append(_PAGINATION_PREV_PAYLOAD)
    if has_more:
        button_texts.append(_PAGINATION_MORE_TEXT)
        button_payloads.append(_PAGINATION_MORE_PAYLOAD)
    button_texts.extend(["Назад", "Выход"])
    button_payloads.extend(["transfer_back", "transfer_exit"])

    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text="Выберите позицию для удаления или следующее действие:",
        button_texts=button_texts,
        button_payloads=button_payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


def _pagination_next_page(flow: WarehouseFlow, state_key: str) -> None:
    current = int(flow.data.get(f"{state_key}_page", 0) or 0)
    flow.data[f"{state_key}_page"] = current + 1


def _pagination_prev_page(flow: WarehouseFlow, state_key: str) -> None:
    current = int(flow.data.get(f"{state_key}_page", 0) or 0)
    flow.data[f"{state_key}_page"] = max(0, current - 1)


def _extract_attachments(msg: dict, include_nested: bool = True) -> list[dict]:
    attachments = msg.get("attachments")
    if include_nested and not isinstance(attachments, list):
        body = msg.get("body")
        if isinstance(body, dict):
            attachments = body.get("attachments")
    if include_nested and not isinstance(attachments, list):
        payload = msg.get("payload")
        if isinstance(payload, dict):
            attachments = payload.get("attachments")
    if not isinstance(attachments, list):
        return []
    out: list[dict] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "")
        if item_type == "image":
            out.append({"type": item_type, "payload": item.get("payload")})
    return out


def _add_files(flow: WarehouseFlow, attachments: list[dict], max_files: int = 10) -> int:
    files = flow.data.setdefault("files", [])
    file_keys = flow.data.setdefault("file_keys", [])
    existing = set(file_keys)
    added = 0
    for item in attachments:
        if len(files) >= max_files:
            break
        key = f"{item.get('type')}::{item.get('payload')}"
        if key in existing:
            continue
        files.append(item)
        file_keys.append(key)
        existing.add(key)
        added += 1
    return added


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
    if "transfer_exit" in norms or "выход" in norms:
        return "exit"
    if "transfer_back" in norms or "назад" in norms:
        return "back"
    if "transfer_done" in norms or "готово" in norms:
        return "done"
    if _normalize(_PAGINATION_PREV_TEXT) in norms or _normalize(_PAGINATION_PREV_PAYLOAD) in norms:
        return "prev_page"
    if _normalize(_PAGINATION_MORE_TEXT) in norms or _normalize(_PAGINATION_MORE_PAYLOAD) in norms:
        return "more_page"
    return ""


def build_transfer_fix_options(items: list[dict[str, str]]) -> list[str]:
    options: list[str] = []
    for it in items:
        cell = f" | {it.get('cell')}" if it.get("cell") else ""
        options.append(f"❌ {it['name']} × {it['quantity']}{cell}")
    options.extend(["➕ Добавить позицию", "✅ Закончить"])
    return options


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
            warnings.append(f"{name}: нет доступных ячеек на складе")
            continue
        remain = need
        taken_total = 0
        for cell in cells:
            cell_name = str(cell.get("cell") or "").strip()
            qty = safe_int(cell.get("quantity"), 0)
            if not cell_name or qty <= 0:
                continue
            take = min(remain, qty)
            if take > 0:
                transfer_items.append({"name": name, "cell": cell_name, "quantity": str(take)})
                remain -= take
                taken_total += take
            if remain <= 0:
                break
        if remain > 0:
            warnings.append(
                f"{name}: в заявке {need} шт., а на складе удалось набрать только {taken_total} шт. (не хватает {remain})"
            )
    return transfer_items, warnings


async def build_summary(items: list[dict[str, Any]], recipient: str, warnings: list[str] | None = None) -> str:
    lines = ["Проверка данных:", f"Кому выдаём: {recipient}", ""]
    for it in items:
        name = it["name"]
        cell = it.get("cell") or ""
        qty = safe_int(it.get("quantity"), 0)
        total = safe_int(get_material_quantity(name), 0)
        issued_for_name_cell = sum(
            safe_int(x.get("quantity"), 0)
            for x in items
            if x.get("name") == name and (x.get("cell") or "") == cell
        )
        remainder = max(total - issued_for_name_cell, 0)
        line = f"🔻 {name}"
        if cell:
            line += f" 🗂 {cell}"
        lines.append(line)
        lines.append(f"Количество: {qty}")
        lines.append(f"Остаток после выдачи: {remainder} (из {total})")
        lines.append("")
    if warnings:
        lines.append("⚠️ Предупреждения:")
        lines.extend([f"• {w}" for w in warnings])
        lines.append("")
    lines.append("Далее потребуется минимум 1 фото.")
    return "\n".join(lines).strip()


def _telegram_target_for_transfer() -> tuple[int | None, int | None, str | None]:
    tg_chat_id = _cfg_int(
        "TELEGRAM_CHAT_ID_TRANSFER",
        "TG_CHAT_ID_TRANSFER",
        "TELEGRAM_CHAT_ID_ARRIVAL",
        "CHAT_ID_ARRIVAL",
    )
    tg_thread_id = _cfg_int(
        "TELEGRAM_THREAD_ID_TRANSFER",
        "TG_THREAD_ID_TRANSFER",
        "TELEGRAM_THREAD_MESSAGE_TRANSFER",
        "TELEGRAM_THREAD_ID_ARRIVAL",
        "THREAD_MESSAGE_ID",
    )
    bot_token = _cfg_str(
        "TELEGRAM_BOT_TOKEN_STORAGE",
        "TELEGRAM_BOT_TOKEN_TECHNIK",
        "TELEGRAM_BOT_TOKEN",
        "TOKEN_BOT",
    )
    return tg_chat_id, tg_thread_id, bot_token


async def _send_transfer_report(flow: WarehouseFlow, chat_id: int, tag: str) -> tuple[bool, bool, str]:
    items = flow.data.get("items") or []
    recipient = str(flow.data.get("recipient") or "")
    request_number = str(flow.data.get("request_number") or "")
    warnings = flow.data.get("warnings") or []
    files = flow.data.get("files") or []

    dt = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    lines = [dt, "#Выдача_ТМЦ"]
    if request_number:
        lines.append(f"Номер заявки: {request_number}")
    lines.append(f"Кому: {recipient}")
    lines.append("")
    for it in items:
        name = it["name"]
        cell = it.get("cell") or ""
        qty = safe_int(it.get("quantity"), 0)
        total = safe_int(get_material_quantity(name), 0)
        issued_for_name_cell = sum(
            safe_int(x.get("quantity"), 0)
            for x in items
            if x.get("name") == name and (x.get("cell") or "") == cell
        )
        remainder = max(total - issued_for_name_cell, 0)
        line = f"🔻 {name}"
        if cell:
            line += f" 🗂 {cell}"
        line += f"\nКоличество: {qty}\nОстаток после выдачи: {remainder} (из {total})\n"
        lines.append(line)
    if warnings:
        lines.append("⚠️ Предупреждения:")
        lines.extend([f"• {w}" for w in warnings])
    lines.append("")
    lines.append(f"Отправил: {tag}")
    report_text = "\n".join(lines).strip()

    max_chat_id = _cfg_int(
        "TRANSFER_MAX_CHAT_ID",
        "MAX_CHAT_ID_TRANSFER",
        "MAX_CHAT_ID_ARRIVAL",
        "CHAT_ID_ARRIVAL",
    )
    max_sent = False
    max_link = ""
    if max_chat_id:
        try:
            response = await send_message(chat_id=max_chat_id, text=report_text, attachments=files)
            message_id = extract_message_id(response)
            if message_id:
                max_link = f"max://chat/{max_chat_id}/message/{message_id}"
            max_sent = True
        except Exception:
            logger.exception("failed to send transfer report to MAX")
    else:
        logger.warning("MAX target chat for transfer report is not configured")

    tg_chat_id, tg_thread_id, bot_token = _telegram_target_for_transfer()
    telegram_sent = False
    telegram_link = ""
    if tg_chat_id:
        try:
            telegram_link = await send_telegram_report(
                chat_id=tg_chat_id,
                thread_id=tg_thread_id,
                text=report_text,
                attachments=files,
                bot_token=bot_token,
            ) or ""
            telegram_sent = True
        except Exception:
            logger.exception("failed to mirror transfer report to Telegram")
    else:
        logger.warning("Telegram target chat for transfer report is not configured")

    return max_sent, telegram_sent, telegram_link or max_link


async def _finish_flow(state: WarehouseState, user_id: int, chat_id: int, flow: WarehouseFlow, msg: dict) -> None:
    tag = sender_tag(msg, user_id)
    max_sent, telegram_sent, report_link = await _send_transfer_report(flow, chat_id, tag)
    if not max_sent and not telegram_sent:
        await _send_plain(flow, chat_id, "❌ Не удалось отправить отчёт в чаты. Запись в таблицу не выполнена.")
        return

    items = flow.data.get("items") or []
    request_number = flow.data.get("request_number")

    write_transfer_row(items, flow.data.get("recipient", ""), tag, report_link)
    if request_number:
        update_request_status(str(request_number), "Выдано", report_link)

    touched = {(it.get("name", ""), it.get("cell", "")) for it in items if it.get("name") and it.get("cell")}
    for name, cell in touched:
        cells = get_material_cells(name)
        remaining = None
        for c in cells:
            if str(c.get("cell") or "").strip() == cell:
                remaining = safe_int(c.get("quantity"), 0)
                break
        if remaining == 0:
            try:
                return_cell_to_free(cell)
            except Exception:
                logger.exception("failed to return cell to free: %s", cell)

    text = "✅ Выдача ТМЦ сохранена."
    if report_link:
        text += f"\nСсылка на отчёт: {report_link}"
    await _send_plain(flow, chat_id, text)
    await reset_warehouse_progress(state, user_id)


async def cmd_transfer_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    flow = WarehouseFlow(
        mode="transfer",
        step="transfer_by_request",
        data={"items": [], "warnings": [], "files": [], "file_keys": []},
    )
    state.flows_by_user[user_id] = flow
    await _ask(flow, chat_id, "Выдача ТМЦ: работать по заявке?", ["Да", "Нет"], include_back=False)


async def _ask_recipient(flow: WarehouseFlow, chat_id: int) -> None:
    recipients = sorted(set(get_recipient_names()), key=lambda s: s.lower())
    flow.step = "transfer_recipient"
    await _ask_paginated(flow, chat_id, "Кому выдаёте?", recipients, "recipient", include_back=True)


async def _ask_material(flow: WarehouseFlow, chat_id: int) -> None:
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    names.append("Ввести вручную")
    flow.step = "transfer_material"
    await _ask_paginated(flow, chat_id, "Выберите ТМЦ для выдачи.", names, "material", include_back=True)


async def _show_material_cells(flow: WarehouseFlow, chat_id: int, material: str) -> None:
    cells = get_material_cells(material)
    flow.data["current_name"] = material
    flow.data["current_cells"] = cells
    if len(cells) == 1:
        selected = str(cells[0].get("cell") or "").strip()
        flow.data["current_cell"] = selected
        push_step(flow, "transfer_qty")
        flow.step = "transfer_qty"
        await _send_plain(flow, chat_id, f"Ячейка: {selected}. Введите количество.")
        return
    if not cells:
        push_step(flow, "transfer_qty")
        flow.step = "transfer_qty"
        flow.data["current_cell"] = ""
        await _send_plain(flow, chat_id, f"{material}: ячейки не найдены. Введите количество вручную.")
        return
    options = [f"{c.get('cell', '')} | {c.get('quantity', '')} шт" for c in cells if c.get("cell")]
    push_step(flow, "transfer_cell")
    flow.step = "transfer_cell"
    await _ask(flow, chat_id, f"{material}: выберите ячейку.", options)


async def _show_summary(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "transfer_confirm"
    push_step(flow, "transfer_confirm")
    await _send_review_prompt(flow, chat_id)


async def handle_transfer_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "transfer":
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        await reset_warehouse_progress(state, user_id)
        await send_text(chat_id, "Сценарий выдачи завершён.")
        return True

    if ctrl == "prev_page":
        if flow.step == "transfer_request_number":
            _pagination_prev_page(flow, "request")
            await _ask_paginated(flow, chat_id, "Выберите номер заявки.", sorted(set(get_open_request_numbers())), "request")
            return True
        if flow.step == "transfer_recipient":
            _pagination_prev_page(flow, "recipient")
            await _ask_recipient(flow, chat_id)
            return True
        if flow.step == "transfer_material":
            _pagination_prev_page(flow, "material")
            await _ask_material(flow, chat_id)
            return True
        if flow.step == "transfer_location":
            _pagination_prev_page(flow, "location")
            await _ask_paginated(flow, chat_id, "Выберите адрес СШМ.", get_shm_locations_by_company(flow.data.get("company_ui", "")), "location")
            return True
        if flow.step == "transfer_fix":
            _pagination_prev_page(flow, "delete")
            await _send_delete_prompt(flow, chat_id)
            return True
        return True

    if ctrl == "more_page":
        if flow.step == "transfer_request_number":
            _pagination_next_page(flow, "request")
            await _ask_paginated(flow, chat_id, "Выберите номер заявки.", sorted(set(get_open_request_numbers())), "request")
            return True
        if flow.step == "transfer_recipient":
            _pagination_next_page(flow, "recipient")
            await _ask_recipient(flow, chat_id)
            return True
        if flow.step == "transfer_material":
            _pagination_next_page(flow, "material")
            await _ask_material(flow, chat_id)
            return True
        if flow.step == "transfer_location":
            _pagination_next_page(flow, "location")
            await _ask_paginated(flow, chat_id, "Выберите адрес СШМ.", get_shm_locations_by_company(flow.data.get("company_ui", "")), "location")
            return True
        if flow.step == "transfer_fix":
            _pagination_next_page(flow, "delete")
            await _send_delete_prompt(flow, chat_id)
            return True
        return True

    if ctrl == "back":
        prev_step = pop_step(flow)
        if prev_step == "transfer_by_request":
            flow.step = "transfer_by_request"
            await _ask(flow, chat_id, "Выдача ТМЦ: работать по заявке?", ["Да", "Нет"], include_back=False)
            return True
        if prev_step == "transfer_request_number":
            flow.step = "transfer_request_number"
            await _ask_paginated(flow, chat_id, "Выберите номер заявки.", sorted(set(get_open_request_numbers())), "request")
            return True
        if prev_step == "transfer_recipient":
            await _ask_recipient(flow, chat_id)
            return True
        if prev_step == "transfer_company":
            flow.step = "transfer_company"
            await _ask(flow, chat_id, "Выберите каршеринг для адреса СШМ.", ["Ситидрайв", "Яндекс", "Белка"])
            return True
        if prev_step == "transfer_location":
            flow.step = "transfer_location"
            await _ask_paginated(flow, chat_id, "Выберите адрес СШМ.", get_shm_locations_by_company(flow.data.get("company_ui", "")), "location")
            return True
        if prev_step in {"transfer_material", "transfer_cell", "transfer_qty", "transfer_fix"}:
            await _ask_material(flow, chat_id)
            return True
        if prev_step in {"transfer_more", "transfer_confirm", "transfer_photo"}:
            await _send_review_prompt(flow, chat_id)
            return True
        await _send_plain(flow, chat_id, "Назад недоступно на этом шаге.")
        return True

    items = flow.data.setdefault("items", [])
    step = flow.step

    if step == "transfer_by_request":
        if text == "Да":
            nums = sorted(set(get_open_request_numbers()))
            if not nums:
                await _send_plain(flow, chat_id, "Открытых заявок нет. Переходим к обычной выдаче.")
                push_step(flow, "transfer_recipient")
                await _ask_recipient(flow, chat_id)
                return True
            push_step(flow, "transfer_request_number")
            flow.step = "transfer_request_number"
            await _ask_paginated(flow, chat_id, "Выберите номер заявки.", nums, "request")
            return True
        if text == "Нет":
            push_step(flow, "transfer_recipient")
            await _ask_recipient(flow, chat_id)
            return True
        await _send_plain(flow, chat_id, "Выберите Да или Нет.")
        return True

    if step == "transfer_request_number":
        request_number = text.strip()
        if request_number not in set(get_open_request_numbers()):
            await _send_plain(flow, chat_id, "Выберите номер заявки кнопкой из списка.")
            return True
        flow.data["request_number"] = request_number
        auto_items, warnings = auto_fill_transfer_items_from_request(get_request_items(request_number))
        flow.data["items"] = auto_items
        flow.data["warnings"] = warnings
        push_step(flow, "transfer_recipient")
        await _ask_recipient(flow, chat_id)
        return True

    if step == "transfer_recipient":
        recipient = text.strip()
        allowed_recipients = set(get_recipient_names())
        if recipient == "ЛОКАЦИИ СШМ":
            flow.data["recipient"] = recipient
            push_step(flow, "transfer_company")
            flow.step = "transfer_company"
            await _ask(flow, chat_id, "Выберите каршеринг для адреса СШМ.", ["Ситидрайв", "Яндекс", "Белка"])
            return True
        if recipient not in allowed_recipients:
            await _send_plain(flow, chat_id, "Выберите получателя кнопкой из списка.")
            return True
        flow.data["recipient"] = recipient
        if flow.data.get("request_number") and items:
            await _show_summary(flow, chat_id)
            return True
        push_step(flow, "transfer_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "transfer_company":
        if text not in {"Ситидрайв", "Яндекс", "Белка"}:
            await _send_plain(flow, chat_id, "Выберите компанию кнопкой.")
            return True
        flow.data["company_ui"] = text
        locations = get_shm_locations_by_company(text)
        if not locations:
            await _send_plain(flow, chat_id, "Для этой компании нет адресов СШМ.")
            await _ask_recipient(flow, chat_id)
            return True
        push_step(flow, "transfer_location")
        flow.step = "transfer_location"
        await _ask_paginated(flow, chat_id, "Выберите адрес СШМ.", locations, "location")
        return True

    if step == "transfer_location":
        locations = set(get_shm_locations_by_company(flow.data.get("company_ui", "")))
        if text.strip() not in locations:
            await _send_plain(flow, chat_id, "Выберите адрес кнопкой из списка.")
            return True
        flow.data["location"] = text.strip()
        flow.data["recipient"] = f"ЛОКАЦИИ СШМ | {text.strip()}"
        push_step(flow, "transfer_material")
        await _ask_material(flow, chat_id)
        return True

    if step == "transfer_material":
        if text == "Ввести вручную":
            push_step(flow, "transfer_material_manual")
            flow.step = "transfer_material_manual"
            await _send_plain(flow, chat_id, "Введите наименование ТМЦ текстом.")
            return True
        names = sorted(set(get_material_names()), key=lambda s: s.lower())
        if text not in set(names):
            await _send_plain(flow, chat_id, "Выберите ТМЦ кнопкой из списка.")
            return True
        await _show_material_cells(flow, chat_id, text.strip())
        return True

    if step == "transfer_material_manual":
        material = text.strip()
        if not material:
            await _send_plain(flow, chat_id, "Наименование не может быть пустым.")
            return True
        flow.data["current_name"] = material
        flow.data["current_cell"] = ""
        push_step(flow, "transfer_qty")
        flow.step = "transfer_qty"
        await _send_plain(flow, chat_id, "Введите количество для выдачи.")
        return True

    if step == "transfer_cell":
        selected = None
        for item in flow.data.get("current_cells") or []:
            option = f"{item.get('cell', '')} | {item.get('quantity', '')} шт"
            if option == text:
                selected = str(item.get("cell") or "").strip()
                break
        if not selected:
            await _send_plain(flow, chat_id, "Выберите ячейку кнопкой из списка.")
            return True
        flow.data["current_cell"] = selected
        push_step(flow, "transfer_qty")
        flow.step = "transfer_qty"
        await _send_plain(flow, chat_id, f"Ячейка: {selected}. Введите количество.")
        return True

    if step == "transfer_qty":
        qty = safe_int(text, -1)
        if qty <= 0:
            await _send_plain(flow, chat_id, "Количество должно быть целым числом больше нуля.")
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
                await _send_plain(flow, chat_id, f"В выбранной ячейке доступно только {available} шт. Введите число заново.")
                return True
        items.append({"name": material, "cell": cell, "quantity": str(qty)})
        logger.info("[transfer] item added | user_id=%s | item=%s", user_id, items[-1])
        for key in ("current_name", "current_cell", "current_cells"):
            flow.data.pop(key, None)
        push_step(flow, "transfer_more")
        flow.step = "transfer_more"
        await _send_need_more_prompt(flow, chat_id)
        return True

    if step == "transfer_more":
        if text == "Да":
            await _ask_material(flow, chat_id)
            return True
        if text == "Нет":
            await _show_summary(flow, chat_id)
            return True
        await _send_plain(flow, chat_id, "Выберите Да или Нет.")
        return True

    if step == "transfer_confirm":
        if text == "Есть ошибка":
            await _send_delete_prompt(flow, chat_id)
            return True
        if text == "Всё ОК":
            push_step(flow, "transfer_photo")
            flow.step = "transfer_photo"
            await _send_files_prompt(flow, chat_id, "Отправьте минимум 1 фото. Когда закончите, нажмите «Готово».")
            return True
        await _send_plain(flow, chat_id, "Выберите «Всё ОК» или «Есть ошибка».")
        return True

    if step == "transfer_fix":
        if text == "➕ Добавить позицию":
            await _ask_material(flow, chat_id)
            return True
        if text == "✅ Закончить":
            await _show_summary(flow, chat_id)
            return True
        delete_options = build_transfer_fix_options(items)[: len(items)]
        for idx, option in enumerate(delete_options):
            if option == text:
                removed = items.pop(idx)
                logger.info("[transfer] item removed | user_id=%s | item=%s", user_id, removed)
                await _send_delete_prompt(flow, chat_id)
                return True
        await _send_plain(flow, chat_id, "Выберите действие кнопкой.")
        return True

    if step == "transfer_photo":
        attachments = _extract_attachments(msg, include_nested=not isinstance(msg.get("callback"), dict))
        if attachments:
            added = _add_files(flow, attachments, max_files=10)
            if added > 0:
                await _send_files_prompt(flow, chat_id, f"Фото получено: {len(flow.data.get('files') or [])}")
            else:
                await _send_files_prompt(flow, chat_id, "Эти фото уже были добавлены или превышен лимит.")
            return True
        if ctrl == "done":
            if not (flow.data.get("files") or []):
                await _send_files_prompt(flow, chat_id, "Нужно минимум 1 фото.")
                return True
            await _finish_flow(state, user_id, chat_id, flow, msg)
            return True
        await _send_files_prompt(flow, chat_id, "Отправьте фото или нажмите «Готово».")
        return True

    return False
