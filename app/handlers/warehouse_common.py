from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.utils.max_api import delete_message, extract_message_id, send_text, send_text_with_reply_buttons

from __future__ import annotations

from warehouse_common import WarehouseState, extract_text, is_control
from arrival import cmd_arrival_tmc, handle_arrival_input
from order_wheel import cmd_order_wheels, cmd_update_orders_db, handle_order_input
from request_tmc import cmd_request_tmc, handle_request_input
from transfer import cmd_transfer_tmc, handle_transfer_input

__all__ = [
    "WarehouseState",
    "cmd_arrival_tmc",
    "cmd_transfer_tmc",
    "cmd_request_tmc",
    "cmd_order_wheels",
    "cmd_update_orders_db",
    "try_handle_warehouse_step",
]

logger = logging.getLogger(__name__)

PAGINATION_SIZE = 15


@dataclass
class WarehouseFlow:
    mode: str = ""
    step: str = ""
    data: dict = field(default_factory=dict)
    history: list[str] = field(default_factory=list)


@dataclass
class WarehouseState:
    flows_by_user: Dict[int, WarehouseFlow] = field(default_factory=dict)


async def reset_warehouse_progress(state: WarehouseState, user_id: int) -> None:
    flow = state.flows_by_user.pop(user_id, None)
    if not flow:
        return
    prev_msg_id = flow.data.get("prompt_msg_id")
    chat_id = flow.data.get("chat_id")
    if prev_msg_id and chat_id:
        try:
            await delete_message(chat_id, prev_msg_id)
        except Exception:
            logger.debug("failed to delete prompt message during reset", exc_info=True)



def push_step(flow: WarehouseFlow, next_step: str) -> None:
    if flow.step and (not flow.history or flow.history[-1] != flow.step):
        flow.history.append(flow.step)
    flow.step = next_step



def pop_step(flow: WarehouseFlow) -> Optional[str]:
    if not flow.history:
        return None
    flow.step = flow.history.pop()
    return flow.step



def controls(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["warehouse_back", "warehouse_exit"]
    return ["Выход"], ["warehouse_exit"]


async def send_prompt(
    flow: WarehouseFlow,
    chat_id: int,
    text: str,
    options: list[str],
    include_back: bool = True,
) -> None:
    flow.data["chat_id"] = chat_id
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        try:
            await delete_message(chat_id, prev_msg_id)
        except Exception:
            logger.debug("failed to delete previous prompt", exc_info=True)

    ctl_texts, ctl_payloads = controls(include_back=include_back)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + ctl_texts,
        button_payloads=options + ctl_payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def send_paginated_prompt(
    flow: WarehouseFlow,
    chat_id: int,
    text: str,
    options: list[str],
    page_key: str,
    include_back: bool = True,
) -> None:
    flow.data["chat_id"] = chat_id
    page = int(flow.data.get(page_key, 0) or 0)
    total = len(options)
    max_page = max(0, (total - 1) // PAGINATION_SIZE) if total else 0
    page = max(0, min(page, max_page))
    start = page * PAGINATION_SIZE
    end = min(start + PAGINATION_SIZE, total)
    page_options = list(options[start:end])
    payloads = list(page_options)

    if page > 0:
        page_options.append("<<")
        payloads.append("warehouse_prev")
    if end < total:
        page_options.append(">>")
        payloads.append("warehouse_next")

    flow.data["current_options"] = list(options)
    flow.data[page_key] = page

    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        try:
            await delete_message(chat_id, prev_msg_id)
        except Exception:
            logger.debug("failed to delete previous prompt", exc_info=True)

    ctl_texts, ctl_payloads = controls(include_back=include_back)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=page_options + ctl_texts,
        button_payloads=payloads + ctl_payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def send_info(chat_id: int, text: str) -> None:
    await send_text(chat_id, text)



def is_control(text: str) -> str:
    normalized = (text or "").strip().lower()
    if normalized in {"выход", "warehouse_exit"}:
        return "exit"
    if normalized in {"назад", "warehouse_back"}:
        return "back"
    if normalized == "warehouse_prev":
        return "prev"
    if normalized == "warehouse_next":
        return "next"
    return ""



def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default



def sender_tag(msg: dict, user_id: int) -> str:
    sender = msg.get("sender") if isinstance(msg, dict) else {}
    if not isinstance(sender, dict):
        sender = {}
    username = str(sender.get("username") or "").strip()
    first_name = str(sender.get("first_name") or sender.get("name") or "").strip()
    if username:
        return f"@{username}"
    if first_name:
        return first_name
    return str(user_id)



def extract_text(msg: dict) -> str:
    if not isinstance(msg, dict):
        return ""
    for key in ("text", "payload", "button_payload"):
        value = msg.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""



def extract_photo_ids(msg: dict) -> list[str]:
    if not isinstance(msg, dict):
        return []
    out: list[str] = []
    attachments = msg.get("attachments") or msg.get("media") or []
    if isinstance(attachments, dict):
        attachments = [attachments]
    for item in attachments:
        if not isinstance(item, dict):
            continue
        for key in ("file_id", "id", "photo_id"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                out.append(value.strip())
                break
    return out


async def handle_pagination(flow: WarehouseFlow, chat_id: int, control: str, prompt_text: str, page_key: str = "page") -> bool:
    options = flow.data.get("current_options") or []
    if not options:
        return False
    page = int(flow.data.get(page_key, 0) or 0)
    flow.data[page_key] = page - 1 if control == "prev" else page + 1
    await send_paginated_prompt(flow, chat_id, prompt_text, options, page_key=page_key)
    return True

async def try_handle_warehouse_step(state: WarehouseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    incoming = extract_text(msg) or text
    control = is_control(incoming)
    if control == "prev":
        incoming = "warehouse_prev"
    elif control == "next":
        incoming = "warehouse_next"
    elif control == "back":
        incoming = "warehouse_back"
    elif control == "exit":
        incoming = "warehouse_exit"

    if await handle_arrival_input(state, user_id, chat_id, incoming, msg):
        return True
    if await handle_transfer_input(state, user_id, chat_id, incoming, msg):
        return True
    if await handle_request_input(state, user_id, chat_id, incoming, msg):
        return True
    if await handle_order_input(state, user_id, chat_id, incoming, msg):
        return True
    return False