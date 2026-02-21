from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
from typing import Dict, List

from app.config import DAMAGE_CHAT_ID_BELKA, DAMAGE_CHAT_ID_CITY, DAMAGE_CHAT_ID_YANDEX
from app.utils.helper import get_fio_async
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)


_KEY_ACTION = ["Забираете со склада", "Сдаете бой", "Передаете в техничку"]
_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_SEASON = ["Лето", "Зима", "Шип", "Липучка", "Всесезон"]
_KEY_WHEEL_TYPE = ["Комплект", "Ось", "Правое колесо", "Левое колесо"]


@dataclass
class MoveItem:
    marka_ts: str = ""
    radius: str = ""
    razmer: str = ""
    marka_rez: str = ""
    model_rez: str = ""
    sezon: str = ""
    tip_diska: str = ""
    wheel_type: str = ""
    count_left: int = 0
    count_right: int = 0


@dataclass
class MoveFlow:
    step: str = ""
    data: dict = field(default_factory=dict)
    items: List[MoveItem] = field(default_factory=list)
    files: List[dict] = field(default_factory=list)


@dataclass
class MoveState:
    flows_by_user: Dict[int, MoveFlow] = field(default_factory=dict)


def _normalize(text: str) -> str:
    return text.strip().strip("«»\"'").lower()


def _is_plate_format(value: str) -> bool:
    cleaned = re.sub(r"\s+", "", str(value).upper())
    return bool(re.match(r"^[АВЕКМНОРСТУХABEKMHOPCTYX]\d{3}[АВЕКМНОРСТУХABEKMHOPCTYX]{2}\d{2,3}$", cleaned))


async def _ask(flow: MoveFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls = ["Выход"] if not include_back else ["Назад", "Выход"]
    payloads = ["move_exit"] if not include_back else ["move_back", "move_exit"]

    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + controls,
        button_payloads=options + payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_plain(flow: MoveFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


def _control(text: str, msg: dict) -> str:
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

    norms = {_normalize(v) for v in candidates if isinstance(v, str) and v.strip()}
    if "move_exit" in norms or "выход" in norms:
        return "exit"
    if "move_back" in norms or "назад" in norms:
        return "back"
    if "move_done" in norms or "готово" in norms:
        return "done"
    if "move_add" in norms or "добавить позицию" in norms:
        return "add"
    return ""


def _extract_attachments(msg: dict) -> List[dict]:
    for node in (
        msg.get("attachments"),
        (msg.get("body") or {}).get("attachments") if isinstance(msg.get("body"), dict) else None,
        (msg.get("payload") or {}).get("attachments") if isinstance(msg.get("payload"), dict) else None,
    ):
        if isinstance(node, list):
            out = []
            for item in node:
                if not isinstance(item, dict):
                    continue
                t = str(item.get("type") or "")
                if t in {"image", "video", "file", "audio"}:
                    out.append({"type": t, "payload": item.get("payload")})
            return out
    return []


def _company_chat_id(company: str) -> int:
    if company == "СитиДрайв":
        return int(DAMAGE_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(DAMAGE_CHAT_ID_YANDEX)
    return int(DAMAGE_CHAT_ID_BELKA)


def _item_to_line(item: MoveItem) -> str:
    return (
        f"🛞 {item.marka_ts} | {item.razmer}/{item.radius} | {item.marka_rez} {item.model_rez} | "
        f"{item.sezon} | {item.tip_diska} | левых {item.count_left}, правых {item.count_right}"
    )


def get_report_move_list(flow: MoveFlow) -> list[str]:
    return [_item_to_line(item) for item in flow.items]


def get_report_move_str(flow: MoveFlow) -> str:
    lines = get_report_move_list(flow)
    if not lines:
        return ""
    return "\n".join(f"{idx + 1}. {line}" for idx, line in enumerate(lines))


async def cmd_move(st: MoveState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    st.flows_by_user[user_id] = MoveFlow(step="grz_tech", data={"username": username, "fio": fio})
    await _send_plain(st.flows_by_user[user_id], chat_id, "ГРЗ технички:")


async def _send_files_prompt(flow: MoveFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=["Готово", "Назад", "Выход"],
        button_payloads=["move_done", "move_back", "move_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_items_prompt(flow: MoveFlow, chat_id: int) -> None:
    text = "Позиции:\n" + (get_report_move_str(flow) or "(пока пусто)")
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text + "\n\nДобавить ещё позицию или перейти к фото?",
        button_texts=["Добавить позицию", "Готово", "Назад", "Выход"],
        button_payloads=["move_add", "move_done", "move_back", "move_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _finish_flow(st: MoveState, user_id: int, chat_id: int, flow: MoveFlow) -> None:
    out_chat = _company_chat_id(str(flow.data.get("company") or ""))
    text = _render_report(flow)
    await send_text(out_chat, text)
    await send_text(chat_id, "Ваша заявка сформирована")
    st.flows_by_user.pop(user_id, None)


def _render_report(flow: MoveFlow) -> str:
    data = flow.data
    base = "⌚️ " + (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S") + "\n\n"
    base += f"🚚Техничка: {data.get('grz_tech', '—')}\n\n"
    base += f"📌{data.get('action', '—')}\n\n"
    if data.get("action") == "Передаете в техничку":
        base += f"🔀{data.get('grz_peredacha', '—')}\n\n"
    base += f"👷 @{data.get('username', '—')}\n{data.get('fio', '—')}\n\n"
    base += f"🏪{data.get('company', '—')}\n\n"
    base += get_report_move_str(flow)
    return base


async def try_handle_move_step(st: MoveState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if not flow:
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        st.flows_by_user.pop(user_id, None)
        await send_text(chat_id, "Оформление отменено")
        return True

    if flow.step == "grz_tech":
        plate = text.strip().upper()
        if not _is_plate_format(plate):
            await send_text(chat_id, "Проверьте формат ГРЗ (например А123БВ77)")
            return True
        flow.data["grz_tech"] = plate
        flow.step = "action"
        await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
        return True

    if flow.step == "action":
        if text.strip() not in _KEY_ACTION:
            await _ask(flow, chat_id, "Выберите действие из списка:", _KEY_ACTION, include_back=False)
            return True
        flow.data["action"] = text.strip()
        if flow.data["action"] == "Передаете в техничку":
            flow.step = "grz_peredacha"
            await _send_plain(flow, chat_id, "Кому передаете (ГРЗ технички):")
            return True
        flow.data["grz_peredacha"] = "-"
        flow.step = "company"
        await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        return True

    if flow.step == "grz_peredacha":
        if ctrl == "back":
            flow.step = "action"
            await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
            return True
        plate = text.strip().upper()
        if not _is_plate_format(plate):
            await send_text(chat_id, "Проверьте формат ГРЗ (например А123БВ77)")
            return True
        flow.data["grz_peredacha"] = plate
        flow.step = "company"
        await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        return True

    if flow.step == "company":
        if ctrl == "back":
            flow.step = "action"
            await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
            return True
        if text.strip() not in _KEY_COMPANY:
            await _ask(flow, chat_id, "Выберите компанию из списка:", _KEY_COMPANY)
            return True
        flow.data["company"] = text.strip()
        flow.data["item_draft"] = MoveItem()
        flow.step = "item_marka_ts"
        await _send_plain(flow, chat_id, "Марка автомобиля:")
        return True

    draft: MoveItem = flow.data.get("item_draft") or MoveItem()

    if flow.step == "item_marka_ts":
        if ctrl == "back":
            flow.step = "company"
            await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
            return True
        draft.marka_ts = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_radius"
        await _send_plain(flow, chat_id, "Радиус (пример R17):")
        return True

    if flow.step == "item_radius":
        draft.radius = text.strip().upper()
        flow.step = "item_razmer"
        await _send_plain(flow, chat_id, "Размер (пример 225/65):")
        return True

    if flow.step == "item_razmer":
        draft.razmer = text.strip()
        flow.step = "item_marka_rez"
        await _send_plain(flow, chat_id, "Марка резины:")
        return True

    if flow.step == "item_marka_rez":
        draft.marka_rez = text.strip()
        flow.step = "item_model_rez"
        await _send_plain(flow, chat_id, "Модель резины:")
        return True

    if flow.step == "item_model_rez":
        draft.model_rez = text.strip()
        flow.step = "item_sezon"
        await _ask(flow, chat_id, "Сезон:", _KEY_SEASON)
        return True

    if flow.step == "item_sezon":
        draft.sezon = text.strip()
        flow.step = "item_tip_diska"
        await _send_plain(flow, chat_id, "Тип диска:")
        return True

    if flow.step == "item_tip_diska":
        draft.tip_diska = text.strip()
        flow.step = "item_wheel_type"
        await _ask(flow, chat_id, "Тип позиции:", _KEY_WHEEL_TYPE)
        return True

    if flow.step == "item_wheel_type":
        draft.wheel_type = text.strip()
        flow.step = "item_count_left"
        await _send_plain(flow, chat_id, "Сколько левых колес:")
        return True

    if flow.step == "item_count_left":
        try:
            draft.count_left = int(text.strip())
        except Exception:
            await send_text(chat_id, "Введите целое число")
            return True
        flow.step = "item_count_right"
        await _send_plain(flow, chat_id, "Сколько правых колес:")
        return True

    if flow.step == "item_count_right":
        try:
            draft.count_right = int(text.strip())
        except Exception:
            await send_text(chat_id, "Введите целое число")
            return True
        flow.items.append(draft)
        flow.data["item_draft"] = MoveItem()
        flow.step = "items_review"
        await _send_items_prompt(flow, chat_id)
        return True

    if flow.step == "items_review":
        if ctrl == "add":
            flow.step = "item_marka_ts"
            await _send_plain(flow, chat_id, "Марка автомобиля:")
            return True
        if ctrl == "done":
            if not flow.items:
                await send_text(chat_id, "Добавьте хотя бы одну позицию")
                return True
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Прикрепите от 2 до 10 фото")
            return True
        if ctrl == "back":
            if flow.items:
                flow.items.pop()
            flow.step = "item_marka_ts"
            await _send_plain(flow, chat_id, "Марка автомобиля:")
            return True
        await _send_items_prompt(flow, chat_id)
        return True

    if flow.step == "files":
        if ctrl == "back":
            flow.step = "items_review"
            await _send_items_prompt(flow, chat_id)
            return True
        if ctrl == "done":
            if len(flow.files) < 2:
                await send_text(chat_id, "Нужно минимум 2 фото")
                return True
            await _finish_flow(st, user_id, chat_id, flow)
            return True
        atts = _extract_attachments(msg)
        if atts:
            free_slots = max(0, 10 - len(flow.files))
            flow.files.extend(atts[:free_slots])
            await send_text(chat_id, f"Файлов добавлено: {len(flow.files)}/10")
            return True
        await send_text(chat_id, "Добавьте вложения или нажмите «Готово»")
        return True

    return True


def reset_move_progress(st: MoveState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)
