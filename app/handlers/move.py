from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import time
import re
import logging
import asyncio
from typing import Dict, List, Set

from app.config import (
    DAMAGE_CHAT_ID_BELKA,
    DAMAGE_CHAT_ID_CITY,
    DAMAGE_CHAT_ID_YANDEX,
    TELEGRAM_CHAT_ID_SBORKA_BELKA,
    TELEGRAM_CHAT_ID_SBORKA_CITY,
    TELEGRAM_CHAT_ID_SBORKA_YANDEX,
    TELEGRAM_THREAD_ID_MOVE_BELKA,
    TELEGRAM_THREAD_ID_MOVE_CITY,
    TELEGRAM_THREAD_ID_MOVE_YANDEX,
    TELEGRAM_BOT_TOKEN_TECHNIK,
)
from app.utils.gsheets import load_tech_plates, write_in_answers_ras
from app.utils.helper import get_fio_async
from app.utils.telegram_api import send_report as send_telegram_report
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)

try:
    from app.utils.gsheets import load_xab_cache, get_xab_koles
except Exception:
    load_xab_cache = None
    get_xab_koles = None


_KEY_ACTION = ["Забираете со склада", "Сдаете бой", "Передаете в техничку"]
_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_SEASON = ["Лето", "Зима", "Шип", "Липучка", "Всесезон"]
_KEY_WHEEL_TYPE = ["Комплект", "Ось", "Правое", "Левое"]

logger = logging.getLogger(__name__)

_MOVE_CACHE_TTL_SECONDS = 1800
_move_tech_plates_cache: list[str] | None = None
_move_cache_usage = 0
_move_users_with_cache: dict[int, float] = {}


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
    file_keys: Set[str] = field(default_factory=set)


@dataclass
class MoveState:
    flows_by_user: Dict[int, MoveFlow] = field(default_factory=dict)


def _normalize(text: str) -> str:
    return text.strip().strip("«»\"'").lower()


_ACTION_BY_NORMALIZED = {_normalize(action): action for action in _KEY_ACTION}


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["move_back", "move_exit"]
    return ["Выход"], ["move_exit"]


def _is_plate_format(value: str) -> bool:
    cleaned = re.sub(r"\s+", "", str(value).upper())
    return bool(re.match(r"^[АВЕКМНОРСТУХABEKMHOPCTYX]\d{3}[АВЕКМНОРСТУХABEKMHOPCTYX]{2}\d{2,3}$", cleaned))


def _normalize_wheel_type(value: str) -> str:
    v = _normalize(value)
    if v in {"правое", "правое колесо"}:
        return "Правое"
    if v in {"левое", "левое колесо"}:
        return "Левое"
    if v == "ось":
        return "Ось"
    if v == "комплект":
        return "Комплект"
    return value.strip()


def _tech_plate_is_known(flow: MoveFlow, plate: str) -> bool:
    options = flow.data.get("tech_grz_options") or []
    plate_norm = re.sub(r"\s+", "", str(plate).upper())
    for item in options:
        if re.sub(r"\s+", "", str(item).upper()) == plate_norm:
            return True
    return False


def _pickup_counts_by_type(type_name: str) -> tuple[int, int]:
    if type_name == "Левое":
        return 1, 0
    if type_name == "Правое":
        return 0, 1
    if type_name == "Ось":
        return 1, 1
    if type_name == "Комплект":
        return 2, 2
    return 0, 0


def split_entry(text: str) -> list[str]:
    parts = [part.strip() for part in str(text).split("|")]
    while len(parts) < 7:
        parts.append("")
    return parts[:7]


async def _ask(flow: MoveFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls, payloads = _kb_control(include_back=include_back)

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
    if "move_exit" in norms or "выход" in norms:
        return "exit"
    if "move_back" in norms or "назад" in norms:
        return "back"
    if "move_done" in norms or "готово" in norms:
        return "done"
    if "move_add" in norms or "добавить позицию" in norms:
        return "add"
    if "move_ok" in norms or "заполнено корректно" in norms:
        return "ok"
    if "move_fix" in norms or "исправить" in norms:
        return "fix"
    if "move_delete_done" in norms or "завершить" in norms:
        return "delete_done"
    return ""


def _extract_attachments(msg: dict, include_nested: bool = True) -> List[dict]:
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
    out = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        t = str(item.get("type") or "")
        if t == "image":
            out.append({"type": t, "payload": item.get("payload")})
    return out


def _add_files(flow: MoveFlow, attachments: List[dict], max_files: int) -> int:
    added = 0
    for item in attachments:
        if len(flow.files) >= max_files:
            break
        key = f"{item.get('type')}::{item.get('payload')}"
        if key in flow.file_keys:
            continue
        flow.file_keys.add(key)
        flow.files.append(item)
        added += 1
    return added


def _load_move_tech_plates_cache() -> list[str]:
    global _move_tech_plates_cache
    if _move_tech_plates_cache is None:
        _move_tech_plates_cache = load_tech_plates()
    return _move_tech_plates_cache


def acquire_move_cache(user_id: int | None = None) -> list[str]:
    global _move_cache_usage
    options = _load_move_tech_plates_cache()

    if user_id is None:
        _move_cache_usage += 1
        return options

    if user_id not in _move_users_with_cache:
        _move_cache_usage += 1
    _move_users_with_cache[user_id] = time.time()
    return options


def cleanup_stale_move_cache_users(timeout_seconds: int = _MOVE_CACHE_TTL_SECONDS) -> None:
    now = time.time()
    stale_user_ids = [uid for uid, ts in _move_users_with_cache.items() if now - ts > timeout_seconds]
    for uid in stale_user_ids:
        release_move_cache(uid)


def release_move_cache(user_id: int) -> None:
    global _move_cache_usage
    if user_id in _move_users_with_cache:
        if _move_cache_usage > 0:
            _move_cache_usage -= 1
        _move_users_with_cache.pop(user_id, None)
    _cleanup_move_cache_if_unused()


def _cleanup_move_cache_if_unused() -> None:
    global _move_tech_plates_cache, _move_cache_usage
    if _move_cache_usage <= 0 and not _move_users_with_cache:
        _move_tech_plates_cache = None
        _move_cache_usage = 0


def reset_move_cache() -> None:
    global _move_tech_plates_cache, _move_cache_usage
    _move_tech_plates_cache = None
    _move_cache_usage = 0
    _move_users_with_cache.clear()


def get_move_cache_user_ids() -> list[int]:
    return sorted(_move_users_with_cache.keys())


def _company_chat_id(company: str) -> int:
    if company == "СитиДрайв":
        return int(DAMAGE_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(DAMAGE_CHAT_ID_YANDEX)
    return int(DAMAGE_CHAT_ID_BELKA)


def _telegram_target_for_move(company: str) -> tuple[int, int | None]:
    if company == "СитиДрайв":
        return TELEGRAM_CHAT_ID_SBORKA_CITY, (TELEGRAM_THREAD_ID_MOVE_CITY or None)
    if company == "Яндекс":
        return TELEGRAM_CHAT_ID_SBORKA_YANDEX, (TELEGRAM_THREAD_ID_MOVE_YANDEX or None)
    return TELEGRAM_CHAT_ID_SBORKA_BELKA, (TELEGRAM_THREAD_ID_MOVE_BELKA or None)


def _item_to_line(item: MoveItem) -> str:
    head = (
        f"🛞 {item.marka_ts} | {item.razmer}/{item.radius} | {item.marka_rez} {item.model_rez} | "
        f"{item.sezon} | {item.tip_diska} | "
    )
    left_count = int(item.count_left or 0)
    right_count = int(item.count_right or 0)
    details: list[str] = []

    kit_count = min(left_count // 2, right_count // 2)
    if kit_count:
        details.append(f"Комплект {kit_count}шт")
        left_count -= kit_count * 2
        right_count -= kit_count * 2

    axle_count = min(left_count, right_count)
    if axle_count:
        details.append(f"Ось {axle_count}шт")
        left_count -= axle_count
        right_count -= axle_count

    if left_count:
        details.append(f"Левый {left_count}шт")
    if right_count:
        details.append(f"Правый {right_count}шт")

    tail = " | ".join(details) + " |" if details else "|"
    return head + tail


def get_report_move_list(flow: MoveFlow) -> list[str]:
    return [_item_to_line(item) for item in flow.items]


def get_report_move_str(flow: MoveFlow) -> str:
    lines = get_report_move_list(flow)
    if not lines:
        return ""
    return "\n\n".join(lines)


def _build_sheet_rows(flow: MoveFlow, message_ref: str) -> list[list[str]]:
    now = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    rows: list[list[str]] = []

    for item in flow.items:
        for _ in range(max(0, int(item.count_left or 0))):
            rows.append([
                now,
                flow.data.get("grz_tech", ""),
                flow.data.get("action", ""),
                flow.data.get("grz_peredacha", ""),
                flow.data.get("company", ""),
                item.marka_ts,
                item.radius,
                item.razmer,
                item.marka_rez,
                item.model_rez,
                item.sezon,
                item.tip_diska,
                "Левое",
                message_ref,
                flow.data.get("username", ""),
            ])
        for _ in range(max(0, int(item.count_right or 0))):
            rows.append([
                now,
                flow.data.get("grz_tech", ""),
                flow.data.get("action", ""),
                flow.data.get("grz_peredacha", ""),
                flow.data.get("company", ""),
                item.marka_ts,
                item.radius,
                item.razmer,
                item.marka_rez,
                item.model_rez,
                item.sezon,
                item.tip_diska,
                "Правое",
                message_ref,
                flow.data.get("username", ""),
            ])
    return rows


def _is_pickup_from_stock(flow: MoveFlow) -> bool:
    action = str(flow.data.get("action") or "")
    return _normalize(action) == _normalize("Забираете со склада")


async def _ask_tech_grz(flow: MoveFlow, chat_id: int, text: str, include_back: bool = True) -> None:
    options = flow.data.get("tech_grz_options") or []
    if options:
        await _ask(flow, chat_id, text, options, include_back=include_back)
        return
    await _send_plain(flow, chat_id, text.replace(" (можно ввести вручную)", ""))


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


async def _send_need_more_prompt(flow: MoveFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text="Требуются еще колеса:",
        button_texts=["Да", "Нет", "Назад", "Выход"],
        button_payloads=["Да", "Нет", "move_back", "move_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_review_prompt(flow: MoveFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    text = get_report_move_str(flow) or "(пока пусто)"
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=["Заполнено корректно", "Исправить", "Назад", "Выход"],
        button_payloads=["move_ok", "move_fix", "move_back", "move_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _send_delete_prompt(flow: MoveFlow, chat_id: int) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    lines = get_report_move_list(flow)
    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text="Удалить:",
        button_texts=lines + ["Завершить", "Добавить запись", "Выход"],
        button_payloads=lines + ["move_delete_done", "move_add", "move_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask_pickup_type(flow: MoveFlow, chat_id: int) -> None:
    await _ask(flow, chat_id, "Выберите вариант из предложенных:", _KEY_WHEEL_TYPE)


async def _ask_pickup_option(flow: MoveFlow, chat_id: int, wheel_type: str) -> bool:
    if not callable(load_xab_cache) or not callable(get_xab_koles):
        await send_text(chat_id, "Не найдены функции Хаба load_xab_cache/get_xab_koles. Проверьте import в move.py")
        return False

    company = str(flow.data.get("company") or "")
    try:
        await asyncio.to_thread(load_xab_cache)
        options = await asyncio.to_thread(get_xab_koles, company, wheel_type, flow.data.get("user_id"))
    except Exception:
        logger.exception("failed to load xab options")
        await send_text(chat_id, "❌ Не удалось обновить остатки Хаба из Google Таблицы. Повторите попытку позже.")
        return False

    if not options:
        flow.step = "pickup_type"
        await _ask(flow, chat_id, "В хабе нет выбранного варианта, выберете другой вариант:", _KEY_WHEEL_TYPE)
        return True

    flow.data["pickup_type"] = wheel_type
    flow.data["pickup_options"] = options
    flow.step = "pickup_option"
    await _ask(flow, chat_id, "Выберите вариант из предложенных:", options)
    return True


def _append_pickup_item_from_option(flow: MoveFlow, option_text: str) -> None:
    temp = split_entry(option_text)
    wheel_type = str(flow.data.get("pickup_type") or "")
    count_left, count_right = _pickup_counts_by_type(wheel_type)
    flow.items.append(
        MoveItem(
            marka_ts=temp[0].strip(),
            radius=temp[1].strip(),
            razmer=temp[2].strip(),
            marka_rez=temp[3].strip(),
            model_rez=temp[4].strip(),
            sezon=temp[5].strip(),
            tip_diska=temp[6].strip(),
            wheel_type=wheel_type,
            count_left=count_left,
            count_right=count_right,
        )
    )


async def cmd_move(st: MoveState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    if chat_id < 0:
        await send_text(chat_id, "Эта команда доступна только в личных сообщениях с ботом")
        return

    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    cleanup_stale_move_cache_users()
    grz_options: list[str] = []
    try:
        grz_options = await asyncio.to_thread(acquire_move_cache, user_id)
    except Exception:
        logger.exception("failed to load move cache with tech plates")

    st.flows_by_user[user_id] = MoveFlow(
        step="grz_tech",
        data={
            "username": username,
            "fio": fio,
            "tech_grz_options": grz_options,
            "user_id": user_id,
        },
    )
    await _ask_tech_grz(st.flows_by_user[user_id], chat_id, "ГРЗ технички (можно ввести вручную):", include_back=False)


async def _finish_flow(st: MoveState, user_id: int, chat_id: int, flow: MoveFlow) -> None:
    out_chat = _company_chat_id(str(flow.data.get("company") or ""))
    text = _render_report(flow)
    max_link = ""
    try:
        response = await send_message(chat_id=out_chat, text=text, attachments=flow.files)
        message_id = extract_message_id(response)
        if message_id:
            max_link = f"max://chat/{out_chat}/message/{message_id}"
    except Exception:
        logger.exception("failed to send move report to company chat")

    telegram_link = ""
    try:
        tg_chat_id, tg_thread_id = _telegram_target_for_move(str(flow.data.get("company") or ""))
        telegram_link = await send_telegram_report(
            chat_id=tg_chat_id,
            thread_id=tg_thread_id,
            text=text,
            attachments=flow.files,
            bot_token=TELEGRAM_BOT_TOKEN_TECHNIK,
        ) or ""
    except Exception:
        logger.exception("failed to mirror move report to telegram")

    report_link = telegram_link or max_link

    try:
        rows = _build_sheet_rows(flow, report_link)
        for row in rows:
            logger.info("[MOVE->GSHEETS] sheet=%s row=%s", "Выгрузка передача", row)
            write_in_answers_ras(row, "Выгрузка передача")
        if str(flow.data.get("action") or "") == "Сдаете бой":
            for row in rows:
                logger.info("[MOVE->GSHEETS] sheet=%s row=%s", "Онлайн остатки Бой", row)
                write_in_answers_ras(row, "Онлайн остатки Бой")
    except Exception:
        logger.exception("failed to write move report rows")

    await send_text(chat_id, "Ваша заявка сформирована")
    st.flows_by_user.pop(user_id, None)
    release_move_cache(user_id)


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
    cleanup_stale_move_cache_users()
    flow = st.flows_by_user.get(user_id)
    if not flow:
        return False

    if user_id in _move_users_with_cache:
        _move_users_with_cache[user_id] = time.time()

    ctrl = _control(text, msg)
    if ctrl == "exit":
        st.flows_by_user.pop(user_id, None)
        release_move_cache(user_id)
        await send_text(chat_id, "Оформление отменено")
        return True

    if flow.step == "grz_tech":
        plate = text.strip().upper()
        if not (_tech_plate_is_known(flow, plate) or _is_plate_format(plate)):
            await send_text(chat_id, "Проверьте формат ГРЗ (например А123БВ77)")
            return True
        flow.data["grz_tech"] = plate
        flow.step = "action"
        await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
        return True

    if flow.step == "action":
        normalized_action = _normalize(text)
        action = _ACTION_BY_NORMALIZED.get(normalized_action)
        if not action:
            await _ask(flow, chat_id, "Выберите действие из списка:", _KEY_ACTION, include_back=False)
            return True
        flow.data["action"] = action
        if flow.data["action"] == "Передаете в техничку":
            flow.step = "grz_peredacha"
            await _ask_tech_grz(flow, chat_id, "Кому передаете:")
            return True
        flow.data["grz_peredacha"] = ""
        flow.step = "company"
        await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        return True

    if flow.step == "grz_peredacha":
        if ctrl == "back":
            flow.step = "action"
            await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
            return True
        plate = text.strip().upper()
        if not (_tech_plate_is_known(flow, plate) or _is_plate_format(plate)):
            await send_text(chat_id, "Проверьте формат ГРЗ (например А123БВ77)")
            return True
        flow.data["grz_peredacha"] = plate
        flow.step = "company"
        await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        return True

    if flow.step == "company":
        if ctrl == "back":
            if flow.data.get("action") == "Передаете в техничку":
                flow.step = "grz_peredacha"
                await _ask_tech_grz(flow, chat_id, "Кому передаете:")
            else:
                flow.step = "action"
                await _ask(flow, chat_id, "Действие:", _KEY_ACTION, include_back=False)
            return True
        if text.strip() not in _KEY_COMPANY:
            await _ask(flow, chat_id, "Выберите компанию из списка:", _KEY_COMPANY)
            return True
        flow.data["company"] = text.strip()
        flow.data["item_draft"] = MoveItem()
        if _is_pickup_from_stock(flow):
            flow.step = "pickup_type"
            await _ask_pickup_type(flow, chat_id)
            return True
        flow.step = "item_marka_ts"
        await _send_plain(flow, chat_id, "Марка автомобиля:")
        return True

    if flow.step == "pickup_type":
        if ctrl == "back":
            flow.step = "company"
            await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
            return True
        wheel_type = _normalize_wheel_type(text)
        if wheel_type not in {"Комплект", "Ось", "Правое", "Левое"}:
            await _ask_pickup_type(flow, chat_id)
            return True
        ok = await _ask_pickup_option(flow, chat_id, wheel_type)
        if not ok:
            return True
        return True

    if flow.step == "pickup_option":
        if ctrl == "back":
            flow.step = "pickup_type"
            await _ask_pickup_type(flow, chat_id)
            return True
        options = flow.data.get("pickup_options") or []
        value = text.strip()
        if value not in options:
            await _ask(flow, chat_id, "Выберите вариант из предложенных:", options)
            return True
        _append_pickup_item_from_option(flow, value)
        flow.data.pop("pickup_options", None)
        flow.step = "need_more"
        await _send_need_more_prompt(flow, chat_id)
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
        await _send_plain(flow, chat_id, "Радиус:")
        return True

    if flow.step == "item_radius":
        if ctrl == "back":
            flow.step = "item_marka_ts"
            await _send_plain(flow, chat_id, "Марка автомобиля:")
            return True
        draft.radius = text.strip().upper()
        flow.data["item_draft"] = draft
        flow.step = "item_razmer"
        await _send_plain(flow, chat_id, "Размер:")
        return True

    if flow.step == "item_razmer":
        if ctrl == "back":
            flow.step = "item_radius"
            await _send_plain(flow, chat_id, "Радиус:")
            return True
        draft.razmer = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_marka_rez"
        await _send_plain(flow, chat_id, "Марка:")
        return True

    if flow.step == "item_marka_rez":
        if ctrl == "back":
            flow.step = "item_razmer"
            await _send_plain(flow, chat_id, "Размер:")
            return True
        draft.marka_rez = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_model_rez"
        await _send_plain(flow, chat_id, "Модель:")
        return True

    if flow.step == "item_model_rez":
        if ctrl == "back":
            flow.step = "item_marka_rez"
            await _send_plain(flow, chat_id, "Марка:")
            return True
        draft.model_rez = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_sezon"
        await _ask(flow, chat_id, "Сезон:", _KEY_SEASON)
        return True

    if flow.step == "item_sezon":
        if ctrl == "back":
            flow.step = "item_model_rez"
            await _send_plain(flow, chat_id, "Модель:")
            return True
        if text.strip() not in _KEY_SEASON:
            await _ask(flow, chat_id, "Сезон:", _KEY_SEASON)
            return True
        draft.sezon = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_tip_diska"
        await _send_plain(flow, chat_id, "Тип диска:")
        return True

    if flow.step == "item_tip_diska":
        if ctrl == "back":
            flow.step = "item_sezon"
            await _ask(flow, chat_id, "Сезон:", _KEY_SEASON)
            return True
        draft.tip_diska = text.strip()
        flow.data["item_draft"] = draft
        flow.step = "item_count_left"
        await _send_plain(flow, chat_id, "Сколько левых колес:")
        return True

    if flow.step == "item_count_left":
        if ctrl == "back":
            flow.step = "item_tip_diska"
            await _send_plain(flow, chat_id, "Тип диска:")
            return True
        try:
            draft.count_left = int(text.strip())
        except Exception:
            await send_text(chat_id, "Введите целое число")
            return True
        flow.data["item_draft"] = draft
        flow.step = "item_count_right"
        await _send_plain(flow, chat_id, "Сколько правых колес:")
        return True

    if flow.step == "item_count_right":
        if ctrl == "back":
            flow.step = "item_count_left"
            await _send_plain(flow, chat_id, "Сколько левых колес:")
            return True
        try:
            draft.count_right = int(text.strip())
        except Exception:
            await send_text(chat_id, "Введите целое число")
            return True
        flow.items.append(draft)
        flow.data["item_draft"] = MoveItem()
        flow.step = "need_more"
        await _send_need_more_prompt(flow, chat_id)
        return True

    if flow.step == "need_more":
        if ctrl == "back":
            if flow.items:
                flow.items.pop()
            if _is_pickup_from_stock(flow):
                flow.step = "pickup_type"
                await _ask_pickup_type(flow, chat_id)
            else:
                flow.data["item_draft"] = MoveItem()
                flow.step = "item_count_left"
                await _send_plain(flow, chat_id, "Сколько левых колес:")
            return True
        if text.strip() == "Да":
            flow.data["item_draft"] = MoveItem()
            if _is_pickup_from_stock(flow):
                flow.step = "pickup_type"
                await _ask_pickup_type(flow, chat_id)
            else:
                flow.step = "item_marka_ts"
                await _send_plain(flow, chat_id, "Марка автомобиля:")
            return True
        if text.strip() == "Нет":
            if not flow.items:
                await send_text(chat_id, "Добавьте хотя бы одну позицию")
                return True
            flow.step = "review"
            await _send_review_prompt(flow, chat_id)
            return True
        await _send_need_more_prompt(flow, chat_id)
        return True

    if flow.step == "review":
        if ctrl == "back":
            flow.step = "need_more"
            await _send_need_more_prompt(flow, chat_id)
            return True
        if ctrl == "ok":
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Прикрепите от 2 до 10 фото")
            return True
        if ctrl == "fix":
            flow.step = "delete_item"
            await _send_delete_prompt(flow, chat_id)
            return True
        await _send_review_prompt(flow, chat_id)
        return True

    if flow.step == "delete_item":
        if ctrl == "add":
            flow.data["item_draft"] = MoveItem()
            if _is_pickup_from_stock(flow):
                flow.step = "pickup_type"
                await _ask_pickup_type(flow, chat_id)
            else:
                flow.step = "item_marka_ts"
                await _send_plain(flow, chat_id, "Марка автомобиля:")
            return True
        if ctrl == "delete_done":
            if not flow.items:
                await send_text(chat_id, "Добавьте хотя бы одну позицию")
                return True
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Прикрепите от 2 до 10 фото")
            return True
        lines = get_report_move_list(flow)
        value = text.strip()
        try:
            idx = lines.index(value)
        except ValueError:
            await _send_delete_prompt(flow, chat_id)
            return True
        flow.items.pop(idx)
        await _send_delete_prompt(flow, chat_id)
        return True

    if flow.step == "files":
        if ctrl == "back":
            flow.step = "review"
            await _send_review_prompt(flow, chat_id)
            return True
        if ctrl == "done":
            if len(flow.files) < 2:
                await send_text(chat_id, "Нужно минимум 2 фото")
                return True
            await _finish_flow(st, user_id, chat_id, flow)
            return True
        attachments = _extract_attachments(msg, include_nested=not isinstance(msg.get("callback"), dict))
        if not attachments:
            await _send_files_prompt(flow, chat_id, "Прикрепите от 2 до 10 фото")
            return True
        added = _add_files(flow, attachments, max_files=10)
        await _send_files_prompt(flow, chat_id, f"Файлов добавлено: {added}. Текущее количество: {len(flow.files)}/10")
        return True

    return True


def reset_move_progress(st: MoveState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)
    release_move_cache(user_id)
