from __future__ import annotations

import logging
from datetime import datetime
from importlib import import_module
from typing import Any, Optional

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
    get_free_cells,
    get_material_cells,
    get_material_names,
    remove_free_cell,
    write_arrival_row,
)
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message as send_max_message,
    send_text_with_reply_buttons,
)
from app.utils.telegram_api import send_report as send_telegram_report

logger = logging.getLogger(__name__)

MAX_FILES_COUNT = 10
_PAGINATION_PAGE_SIZE = 20
_PAGINATION_PREV_TEXT = "<<"
_PAGINATION_PREV_PAYLOAD = "arrival_prev_page"
_PAGINATION_MORE_TEXT = ">>"
_PAGINATION_MORE_PAYLOAD = "arrival_more"


def build_arrival_fix_options(items: list[dict[str, str]]) -> list[str]:
    options = [f"❌ {it['name']} × {it['quantity']} | {it['cell']}" for it in items]
    options.extend(["➕ Добавить позицию", "✅ Закончить"])
    return options


def _normalize(text: str) -> str:
    return str(text or "").strip().strip('«»"\'').lower()


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["arrival_back", "arrival_exit"]
    return ["Выход"], ["arrival_exit"]


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
            logger.exception("[arrival] failed to delete prompt chat_id=%s message_id=%s", chat_id, prev_msg_id)
    flow.data.pop("prompt_msg_id", None)


async def _send_buttons(
    flow: WarehouseFlow,
    chat_id: int,
    text: str,
    button_texts: list[str],
    button_payloads: list[str],
) -> None:
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


async def _ask(flow: WarehouseFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls, payloads = _kb_control(include_back=include_back)
    await _send_buttons(flow, chat_id, text, options + controls, options + payloads)


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
    await _send_buttons(flow, chat_id, text, button_texts, button_payloads)


async def _send_plain(flow: WarehouseFlow, chat_id: int, text: str, include_back: bool = True) -> None:
    controls, payloads = _kb_control(include_back=include_back)
    await _send_buttons(flow, chat_id, text, controls, payloads)


async def _send_files_prompt(flow: WarehouseFlow, chat_id: int, text: str) -> None:
    await _send_buttons(
        flow,
        chat_id,
        text,
        ["Готово", "Назад", "Выход"],
        ["arrival_done", "arrival_back", "arrival_exit"],
    )


def _pagination_next_page(flow: WarehouseFlow, state_key: str) -> None:
    current = int(flow.data.get(f"{state_key}_page", 0) or 0)
    flow.data[f"{state_key}_page"] = current + 1


def _pagination_prev_page(flow: WarehouseFlow, state_key: str) -> None:
    current = int(flow.data.get(f"{state_key}_page", 0) or 0)
    flow.data[f"{state_key}_page"] = max(0, current - 1)


def _control_candidates(text: str, msg: dict) -> set[str]:
    candidates = [text]
    callback = msg.get("callback")
    if isinstance(callback, dict):
        nodes = [callback]
        payload_node = callback.get("payload")
        if isinstance(payload_node, dict):
            nodes.append(payload_node)
        for node in nodes:
            for key in ("payload", "data", "value", "command", "action", "text"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value)
    for key in ("text", "payload", "data", "value", "command", "action"):
        value = msg.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append(value)
    return {_normalize(v) for v in candidates if isinstance(v, str) and v.strip()}


def _control(text: str, msg: dict) -> str:
    norms = _control_candidates(text, msg)
    if "arrival_exit" in norms or "выход" in norms:
        return "exit"
    if "arrival_back" in norms or "назад" in norms:
        return "back"
    if "arrival_done" in norms or "готово" in norms:
        return "done"
    if _normalize(_PAGINATION_PREV_TEXT) in norms or _normalize(_PAGINATION_PREV_PAYLOAD) in norms:
        return "prev_page"
    if _normalize(_PAGINATION_MORE_TEXT) in norms or _normalize(_PAGINATION_MORE_PAYLOAD) in norms:
        return "more"
    return ""


def _extract_choice(text: str, msg: dict) -> str:
    candidates: list[str] = []
    if isinstance(text, str) and text.strip():
        candidates.append(text.strip())
    callback = msg.get("callback")
    if isinstance(callback, dict):
        nodes = [callback]
        payload_node = callback.get("payload")
        if isinstance(payload_node, dict):
            nodes.append(payload_node)
        for node in nodes:
            for key in ("payload", "data", "value", "text"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
    for key in ("payload", "data", "value"):
        value = msg.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())
    return candidates[0] if candidates else ""


def _extract_attachments(msg: dict, include_nested: bool = True) -> list[dict[str, Any]]:
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

    out: list[dict[str, Any]] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "") != "image":
            continue
        out.append({"type": "image", "payload": item.get("payload")})
    return out


def _add_files(flow: WarehouseFlow, attachments: list[dict[str, Any]], max_files: int = MAX_FILES_COUNT) -> int:
    files = flow.data.setdefault("photos", [])
    file_keys = flow.data.setdefault("photo_keys", set())
    added = 0
    for item in attachments:
        if len(files) >= max_files:
            break
        key = f"{item.get('type')}::{item.get('payload')}"
        if key in file_keys:
            continue
        file_keys.add(key)
        files.append(item)
        added += 1
    return added


def _build_arrival_caption(items: list[dict[str, str]], tag: str) -> str:
    lines = [
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        "#Поступление_ТМЦ",
        "📥 Поступление",
        "",
    ]
    for it in items:
        lines.append(f"🔻 {it['name']} 🗂 {it['cell']}")
        lines.append(f"Количество: {it['quantity']}")
        lines.append("")
    if tag:
        lines.append(tag)
    return "\n".join(lines).strip()


def _build_max_link(chat_id: Any, message_id: Any) -> str:
    if chat_id is None or message_id is None:
        return ""
    return f"max://chat/{chat_id}/message/{message_id}"


def _load_arrival_targets() -> dict[str, Any]:
    try:
        cfg = import_module("app.config")
    except Exception:
        logger.exception("failed to import app.config for arrival targets")
        return {}

    def pick(*names: str) -> Any:
        for name in names:
            if hasattr(cfg, name):
                value = getattr(cfg, name)
                if value not in (None, ""):
                    return value
        return None

    return {
        "max_chat_id": pick("MAX_CHAT_ID_ARRIVAL_STORAGE"),
        "telegram_chat_id": pick("TELEGRAM_CHAT_ID_ARRIVAL"),
        "telegram_thread_id": pick("TELEGRAM_THREAD_MESSAGE_ID"),
        "telegram_bot_token": pick("TELEGRAM_BOT_TOKEN_STORAGE"),
    }


async def _send_arrival_report(items: list[dict[str, str]], files: list[dict[str, Any]], tag: str) -> str:
    text = _build_arrival_caption(items, tag)
    targets = _load_arrival_targets()

    max_link = ""
    max_sent = False
    max_chat_id = targets.get("max_chat_id")
    if max_chat_id not in (None, ""):
        try:
            response = await send_max_message(chat_id=int(max_chat_id), text=text, attachments=files)
            max_message_id = extract_message_id(response)
            if max_message_id:
                max_link = _build_max_link(max_chat_id, max_message_id)
            max_sent = True
        except Exception:
            logger.exception("failed to send arrival report to MAX")

    telegram_link = ""
    telegram_chat_id = targets.get("telegram_chat_id")
    telegram_bot_token = targets.get("telegram_bot_token")
    if telegram_chat_id not in (None, "") and telegram_bot_token not in (None, ""):
        try:
            telegram_link = await send_telegram_report(
                chat_id=int(telegram_chat_id),
                text=text,
                attachments=files,
                thread_id=targets.get("telegram_thread_id") or None,
                bot_token=str(telegram_bot_token),
            ) or ""
        except Exception:
            logger.exception("failed to mirror arrival report to Telegram")

    if not max_sent and not telegram_link:
        raise RuntimeError("Не удалось отправить отчёт о поступлении ни в MAX, ни в Telegram")

    return telegram_link or max_link


async def _ask_material(flow: WarehouseFlow, chat_id: int, include_back: bool = False) -> None:
    names = sorted(set(get_material_names()), key=lambda s: s.lower())
    names.append("Ввести вручную")
    flow.step = "arrival_name"
    await _ask_paginated(flow, chat_id, "Поступление ТМЦ: выберите наименование.", names, state_key="arrival_name", include_back=include_back)


async def cmd_arrival_tmc(state: WarehouseState, user_id: int, chat_id: int) -> None:
    logger.info("[arrival] start chat_id=%s user_id=%s", chat_id, user_id)
    flow = WarehouseFlow(
        mode="arrival",
        step="arrival_name",
        data={"items": [], "photos": [], "photo_keys": set(), "prompt_msg_id": None},
    )
    state.flows_by_user[user_id] = flow
    await _ask_material(flow, chat_id, include_back=False)


async def _show_cells(flow: WarehouseFlow, chat_id: int, material: str) -> None:
    cells = get_material_cells(material)
    flow.data["current_name"] = material
    flow.data["current_cells"] = cells
    if cells:
        if len(cells) == 1 and cells[0].get("cell"):
            selected = str(cells[0].get("cell") or "").strip()
            flow.data["current_cell"] = selected
            flow.data["current_cell_from_free"] = False
            push_step(flow, "arrival_qty")
            flow.step = "arrival_qty"
            await _send_plain(flow, chat_id, f"{material}: найдена ячейка {selected}. Введите количество.")
            return
        options = [f"{c.get('cell', '')} | {c.get('quantity', '')} шт" for c in cells if c.get("cell")]
        push_step(flow, "arrival_cell_existing")
        flow.step = "arrival_cell_existing"
        await _ask(flow, chat_id, f"{material}: выберите ячейку хранения.", options)
        return

    free_cells = sorted(set(get_free_cells()), key=lambda s: s.lower())
    flow.data["current_free_cells"] = free_cells
    if free_cells:
        if len(free_cells) == 1:
            flow.data["current_cell"] = free_cells[0]
            flow.data["current_cell_from_free"] = True
            push_step(flow, "arrival_qty")
            flow.step = "arrival_qty"
            await _send_plain(flow, chat_id, f"{material}: выбрана свободная ячейка {free_cells[0]}. Введите количество.")
            return
        push_step(flow, "arrival_cell_free")
        flow.step = "arrival_cell_free"
        await _ask_paginated(flow, chat_id, f"{material}: выберите свободную ячейку.", free_cells, state_key="arrival_cell_free")
        return

    push_step(flow, "arrival_cell_manual")
    flow.step = "arrival_cell_manual"
    await _send_plain(flow, chat_id, "Свободные ячейки не найдены. Введите ячейку вручную.")


async def _show_summary(flow: WarehouseFlow, chat_id: int) -> None:
    items = flow.data.get("items") or []
    lines = ["Проверка данных:", ""]
    for it in items:
        lines.append(f"🔻 {it['name']} 🗂 {it['cell']}")
        lines.append(f"Количество: {it['quantity']}")
        lines.append("")
    lines.append("Далее потребуется минимум 1 фото.")
    push_step(flow, "arrival_confirm")
    flow.step = "arrival_confirm"
    await _ask(flow, chat_id, "\n".join(lines).strip(), ["Всё ОК", "Есть ошибка"])


async def _show_fix_menu(flow: WarehouseFlow, chat_id: int) -> None:
    flow.step = "arrival_fix"
    options = build_arrival_fix_options(flow.data.get("items") or [])
    await _ask_paginated(flow, chat_id, "Выберите позицию для удаления или следующее действие:", options, state_key="arrival_fix")


async def handle_arrival_input(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = state.flows_by_user.get(user_id)
    if flow is None or flow.mode != "arrival":
        return False

    control = _control(text, msg)
    choice = _extract_choice(text, msg)
    items = flow.data.setdefault("items", [])
    step = flow.step

    if control == "exit":
        logger.info("[arrival] cancelled chat_id=%s user_id=%s", chat_id, user_id)
        await reset_warehouse_progress(state, user_id)
        await _delete_prev_prompt(flow, chat_id)
        await send_max_message(chat_id=chat_id, text="Сценарий поступления завершён.")
        return True

    if control == "prev_page":
        if flow.step == "arrival_name":
            _pagination_prev_page(flow, "arrival_name")
            await _ask_material(flow, chat_id, include_back=False)
            return True
        if flow.step == "arrival_cell_free":
            _pagination_prev_page(flow, "arrival_cell_free")
            await _ask_paginated(flow, chat_id, f"{flow.data.get('current_name', '')}: выберите свободную ячейку.", flow.data.get("current_free_cells") or [], state_key="arrival_cell_free")
            return True
        if flow.step == "arrival_fix":
            _pagination_prev_page(flow, "arrival_fix")
            await _show_fix_menu(flow, chat_id)
            return True
        return True

    if control == "more":
        if flow.step == "arrival_name":
            _pagination_next_page(flow, "arrival_name")
            await _ask_material(flow, chat_id, include_back=False)
            return True
        if flow.step == "arrival_cell_free":
            _pagination_next_page(flow, "arrival_cell_free")
            await _ask_paginated(flow, chat_id, f"{flow.data.get('current_name', '')}: выберите свободную ячейку.", flow.data.get("current_free_cells") or [], state_key="arrival_cell_free")
            return True
        if flow.step == "arrival_fix":
            _pagination_next_page(flow, "arrival_fix")
            await _show_fix_menu(flow, chat_id)
            return True
        return True

    if control == "back":
        prev_step = pop_step(flow)
        if prev_step == "arrival_name":
            await _ask_material(flow, chat_id, include_back=False)
            return True
        if prev_step in {"arrival_cell_existing", "arrival_cell_free", "arrival_cell_manual"}:
            await _show_cells(flow, chat_id, flow.data.get("current_name", ""))
            return True
        if prev_step == "arrival_qty":
            flow.step = "arrival_qty"
            await _send_plain(flow, chat_id, "Введите количество ещё раз.")
            return True
        if prev_step == "arrival_more":
            flow.step = "arrival_more"
            await _ask(flow, chat_id, "Позиция добавлена. Добавить ещё?", ["Да", "Нет"])
            return True
        if prev_step in {"arrival_confirm", "arrival_fix", "arrival_photo"}:
            await _show_summary(flow, chat_id)
            return True
        await _send_plain(flow, chat_id, "Назад недоступно на этом шаге.")
        return True

    if step == "arrival_name":
        if choice == "Ввести вручную":
            push_step(flow, "arrival_name_manual")
            flow.step = "arrival_name_manual"
            await _send_plain(flow, chat_id, "Введите наименование ТМЦ текстом.")
            return True
        if choice not in set(flow.data.get("arrival_name_options") or sorted(set(get_material_names()), key=lambda s: s.lower()) + ["Ввести вручную"]):
            await _ask_material(flow, chat_id, include_back=False)
            return True
        await _show_cells(flow, chat_id, choice.strip())
        return True

    if step == "arrival_name_manual":
        material = choice.strip()
        if not material:
            await _send_plain(flow, chat_id, "Наименование не может быть пустым.")
            return True
        await _show_cells(flow, chat_id, material)
        return True

    if step == "arrival_cell_existing":
        selected = None
        for item in flow.data.get("current_cells") or []:
            option = f"{item.get('cell', '')} | {item.get('quantity', '')} шт"
            if option == choice:
                selected = str(item.get("cell") or "").strip()
                break
        if not selected:
            await _ask(flow, chat_id, f"{flow.data.get('current_name', '')}: выберите ячейку хранения.", [f"{c.get('cell', '')} | {c.get('quantity', '')} шт" for c in flow.data.get('current_cells') or [] if c.get('cell')])
            return True
        flow.data["current_cell"] = selected
        flow.data["current_cell_from_free"] = False
        push_step(flow, "arrival_qty")
        flow.step = "arrival_qty"
        await _send_plain(flow, chat_id, f"Ячейка: {selected}. Введите количество.")
        return True

    if step == "arrival_cell_free":
        if choice not in set(flow.data.get("current_free_cells") or []):
            await _ask_paginated(flow, chat_id, f"{flow.data.get('current_name', '')}: выберите свободную ячейку.", flow.data.get("current_free_cells") or [], state_key="arrival_cell_free")
            return True
        flow.data["current_cell"] = choice.strip()
        flow.data["current_cell_from_free"] = True
        push_step(flow, "arrival_qty")
        flow.step = "arrival_qty"
        await _send_plain(flow, chat_id, f"Свободная ячейка: {choice}. Введите количество.")
        return True

    if step == "arrival_cell_manual":
        if not choice.strip():
            await _send_plain(flow, chat_id, "Ячейка не может быть пустой.")
            return True
        flow.data["current_cell"] = choice.strip()
        flow.data["current_cell_from_free"] = False
        push_step(flow, "arrival_qty")
        flow.step = "arrival_qty"
        await _send_plain(flow, chat_id, "Введите количество.")
        return True

    if step == "arrival_qty":
        qty = safe_int(choice, -1)
        if qty <= 0:
            await _send_plain(flow, chat_id, "Количество должно быть целым числом больше нуля.")
            return True
        item = {
            "name": flow.data.get("current_name", ""),
            "cell": flow.data.get("current_cell", ""),
            "quantity": str(qty),
            "from_free": bool(flow.data.get("current_cell_from_free")),
        }
        items.append(item)
        logger.info("[arrival] item added chat_id=%s user_id=%s item=%s", chat_id, user_id, item)
        for key in ("current_name", "current_cell", "current_cells", "current_free_cells", "current_cell_from_free"):
            flow.data.pop(key, None)
        push_step(flow, "arrival_more")
        flow.step = "arrival_more"
        await _ask(flow, chat_id, "Позиция добавлена. Добавить ещё?", ["Да", "Нет"])
        return True

    if step == "arrival_more":
        if choice == "Да":
            await _ask_material(flow, chat_id)
            return True
        if choice == "Нет":
            await _show_summary(flow, chat_id)
            return True
        await _ask(flow, chat_id, "Позиция добавлена. Добавить ещё?", ["Да", "Нет"])
        return True

    if step == "arrival_confirm":
        if choice == "Есть ошибка":
            await _show_fix_menu(flow, chat_id)
            return True
        if choice == "Всё ОК":
            push_step(flow, "arrival_photo")
            flow.step = "arrival_photo"
            await _send_files_prompt(flow, chat_id, "Отправьте минимум 1 фото. Когда закончите, нажмите «Готово».")
            return True
        await _show_summary(flow, chat_id)
        return True

    if step == "arrival_fix":
        if choice == "➕ Добавить позицию":
            await _ask_material(flow, chat_id)
            return True
        if choice == "✅ Закончить":
            await _show_summary(flow, chat_id)
            return True
        for idx, option in enumerate(build_arrival_fix_options(items)[: len(items)]):
            if option == choice:
                removed = items.pop(idx)
                logger.info("[arrival] item removed chat_id=%s user_id=%s item=%s", chat_id, user_id, removed)
                flow.data["arrival_fix_page"] = 0
                await _show_fix_menu(flow, chat_id)
                return True
        await _show_fix_menu(flow, chat_id)
        return True

    if step == "arrival_photo":
        attachments = _extract_attachments(msg, include_nested=not isinstance(msg.get("callback"), dict))
        if attachments:
            added = _add_files(flow, attachments)
            total = len(flow.data.get("photos") or [])
            logger.info("[arrival] photos received chat_id=%s user_id=%s added=%s total=%s", chat_id, user_id, added, total)
            await _send_files_prompt(flow, chat_id, f"Фото получено: {total}/{MAX_FILES_COUNT}")
            return True
        if control != "done":
            await _send_files_prompt(flow, chat_id, "Отправьте фото или нажмите «Готово».")
            return True

        files = flow.data.get("photos") or []
        if not files:
            await _send_files_prompt(flow, chat_id, "Нужно минимум 1 фото.")
            return True

        tag = sender_tag(msg, user_id)
        try:
            message_link = await _send_arrival_report(items, files, tag)
        except Exception:
            logger.exception("failed to send arrival report")
            await _send_plain(flow, chat_id, "Не удалось отправить отчёт. Данные в таблицу не записаны.")
            return True

        try:
            for item in items:
                write_arrival_row([item], tag, message_link)
                if item.get("from_free") and item.get("cell"):
                    remove_free_cell(item["cell"])
        except Exception:
            logger.exception("failed to save arrival rows")
            await _send_plain(flow, chat_id, "Отчёт отправлен, но при записи в Google Sheets произошла ошибка.")
            return True

        logger.info(
            "[arrival] saved chat_id=%s user_id=%s items=%s photos=%s message_link=%s",
            chat_id,
            user_id,
            len(items),
            len(files),
            message_link,
        )
        await _delete_prev_prompt(flow, chat_id)
        await send_max_message(chat_id=chat_id, text=f"✅ Поступление ТМЦ сохранено.\nОтчёт: {message_link}")
        await reset_warehouse_progress(state, user_id)
        return True

    return False
