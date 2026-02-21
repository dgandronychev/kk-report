from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
import logging
import re
from typing import Dict, List
from app.utils.helper import get_fio_async

from app.utils.max_api import delete_message, extract_message_id, send_message, send_text, send_text_with_reply_buttons
from app.utils.gsheets import (
    get_max_nomer_sborka,
    load_sborka_reference_data,
    write_soberi_in_google_sheets_rows,
)

logger = logging.getLogger(__name__)


class GHRezina(IntEnum):
    NOMER = 0
    RADIUS = 1
    RAZMER = 2
    SEZON = 3
    MARKA = 4
    MODEL = 5
    COMPANY = 6
    MARKA_TS = 7


@dataclass
class SoberiFlow:
    step: str = "type_soberi"
    data: dict = field(default_factory=dict)


@dataclass
class SoberiState:
    flows_by_user: Dict[int, SoberiFlow] = field(default_factory=dict)


_KEY_OBJECT = ["Комплект", "Ось", "Колесо"]
_KEY_COMPANY = ["СитиДрайв", "Яндекс"]
_KEY_TYPE_DISK = ["Литой оригинальный", "Литой неоригинальный", "Штамп"]
_KEY_CHISLA = ["0", "1", "2", "3", "4", "5"]
_MARKA_TS_PAGE_SIZE = 15

_ref_data: dict | None = None


def _normalize(text: str) -> str:
    return text.strip().strip("«»\"'").lower()


def _control_candidates(text: str, msg: dict) -> set[str]:
    vals: list[str] = [text]
    cb = msg.get("callback")
    if isinstance(cb, dict):
        for node in (cb, cb.get("payload") if isinstance(cb.get("payload"), dict) else None):
            if not isinstance(node, dict):
                continue
            for key in ("payload", "data", "value", "command", "action", "text"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    vals.append(value)
    return {_normalize(v) for v in vals if isinstance(v, str) and v.strip()}


def _rows(company: str) -> tuple[list[list[str]], list[list[str]]]:
    key = "city" if company == "СитиДрайв" else "yandex" if company == "Яндекс" else "belka"
    return _ref_data["rezina"][key], _ref_data["cars"][key]


def _list_radius(company: str) -> list[str]:
    rez, _ = _rows(company)
    out = []
    for row in rez:
        if len(row) > GHRezina.RADIUS:
            out.append(str(row[GHRezina.RADIUS]).strip())
    return sorted(set(out), key=lambda x: int(re.sub(r"\D", "", x) or "0"))


def _filter_values(
    company: str,
    radius: str = "",
    razmer: str = "",
    marka: str = "",
    model: str = "",
    short: bool = False,
    field: int = GHRezina.RAZMER,
) -> list[str]:
    rez, _ = _rows(company)
    vals = set()
    for row in rez:
        if len(row) <= GHRezina.MODEL:
            continue
        if radius and str(row[GHRezina.RADIUS]).strip() != radius.strip():
            continue
        if razmer and str(row[GHRezina.RAZMER]).strip() != razmer.strip():
            continue
        if not short:
            if marka and str(row[GHRezina.MARKA]).strip() != marka.strip():
                continue
            if model and str(row[GHRezina.MODEL]).strip() != model.strip():
                continue
        vals.add(str(row[field]).strip())
    return sorted(vals)


def _list_marka_ts(company: str) -> list[str]:
    _, car_rows = _rows(company)
    idx = 2 if company == "СитиДрайв" else 3 if company == "Яндекс" else 1
    out = []
    for row in car_rows:
        if len(row) > idx:
            out.append(str(row[idx]).strip())
    return sorted(set(out))


async def _ensure_refs_loaded() -> None:
    global _ref_data
    if _ref_data is None:
        _ref_data = await load_sborka_reference_data()


def _clear(st: SoberiState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)

async def warmup_soberi_refs() -> None:
    await _ensure_refs_loaded()

async def refresh_soberi_refs() -> None:
    global _ref_data
    _ref_data = await load_sborka_reference_data()

def reset_soberi_progress(st: SoberiState, user_id: int) -> None:
    _clear(st, user_id)

async def _send_flow_text(flow: SoberiFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask(flow: SoberiFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    button_texts = options + (["Назад"] if include_back else []) + ["Выход"]
    payloads = options + (["soberi_back"] if include_back else []) + ["soberi_exit"]
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(chat_id, text, button_texts=button_texts, button_payloads=payloads)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask_marka_ts(flow: SoberiFlow, chat_id: int, text: str = "Марка автомобиля:") -> None:
    options = _list_marka_ts(flow.data["company"])
    total = len(options)
    if total == 0:
        await _send_flow_text(flow, chat_id, "Список марок ТС пуст")
        return

    max_page = max(0, (total - 1) // _MARKA_TS_PAGE_SIZE)
    page = int(flow.data.get("marka_ts_page", 0))
    page = min(max(page, 0), max_page)
    flow.data["marka_ts_page"] = page

    start = page * _MARKA_TS_PAGE_SIZE
    end = start + _MARKA_TS_PAGE_SIZE
    page_options = options[start:end]
    page_payloads = page_options[:]

    if page > 0:
        page_options.append("⬅️ Предыдущая")
        page_payloads.append("soberi_page_prev")
    if page < max_page:
        page_options.append("➡️ Следующая")
        page_payloads.append("soberi_page_next")

    button_texts = page_options + ["Назад", "Выход"]
    payloads = page_payloads + ["soberi_back", "soberi_exit"]
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(chat_id, f"{text}\nСтраница {page + 1}/{max_page + 1}", button_texts=button_texts, button_payloads=payloads)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def cmd_soberi(st: SoberiState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    await _ensure_refs_loaded()
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    st.flows_by_user[user_id] = SoberiFlow(step="type_soberi", data={"fio": fio})
    await _ask(st.flows_by_user[user_id], chat_id, "Укажите, что собираем:", _KEY_OBJECT, include_back=False)


async def cmd_soberi_belka(st: SoberiState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    await _ensure_refs_loaded()
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    st.flows_by_user[user_id] = SoberiFlow(
        step="type_soberi",
        data={"fio": fio, "company": "Белка", "preset_company": True},
    )
    await _ask(st.flows_by_user[user_id], chat_id, "Укажите, что собираем:", _KEY_OBJECT, include_back=False)


def _next_rows(data: dict) -> list[list[str]]:
    out: list[list[str]] = []
    count_1 = int(data.get("count_1") or 0)
    count_2 = int(data.get("count_2") or 0)
    nomer = get_max_nomer_sborka() + 1

    def _row(position: str, num: int) -> list[str]:
        return [
            "",
            "",
            (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
            data.get("company", ""),
            data.get("marka_ts", ""),
            data.get("radius", ""),
            data.get("razmer", ""),
            data.get("marka_rez", ""),
            data.get("model_rez", ""),
            data.get("sezon", ""),
            data.get("type_disk", ""),
            position,
            "",
            f"sb{num}",
            data.get("fio", ""),
        ]

    type_soberi = data.get("type_soberi")
    if type_soberi == "Колесо":
        for _ in range(count_1):
            out.append(_row("Левое", nomer))
            nomer += 1
        for _ in range(count_2):
            out.append(_row("Правое", nomer))
            nomer += 1
        return out

    if type_soberi == "Комплект":
        for _ in range(count_1):
            for _ in range(2):
                out.append(_row("Левое", nomer))
            for _ in range(2):
                out.append(_row("Правое", nomer))
            nomer += 1
        return out

    for _ in range(count_1):
        out.append(_row("Левое", nomer))
        out.append(_row("Правое", nomer))
        nomer += 1
    return out


async def _finalize(st: SoberiState, user_id: int, chat_id: int) -> bool:
    flow = st.flows_by_user[user_id]
    rows = _next_rows(flow.data)
    if not rows:
        await _send_flow_text(flow, chat_id, "Количество не может быть пустым")
        return True
    write_soberi_in_google_sheets_rows(rows)
    _clear(st, user_id)
    await _send_flow_text(flow, chat_id, "Заявка на сборку оформлена")
    return True


async def try_handle_soberi_step(st: SoberiState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if flow is None:
        return False

    controls = _control_candidates(text, msg)
    if controls & {"выход", "soberi_exit"}:
        prompt_msg_id = flow.data.get("prompt_msg_id")
        if prompt_msg_id:
            await delete_message(chat_id, prompt_msg_id)
        _clear(st, user_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    t = text.strip()
    step = flow.step
    data = flow.data

    if step == "type_soberi":
        if t not in _KEY_OBJECT:
            await _ask(st.flows_by_user[user_id], chat_id, "Укажите, что собираем:", _KEY_OBJECT, include_back=False)
            return True
        data["type_soberi"] = t
        if data.get("preset_company"):
            flow.step = "marka_ts"
            data["marka_ts_page"] = 0
            await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
            return True
        flow.step = "company"
        await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        return True

    if controls & {"назад", "soberi_back"}:
        if step == "company":
            flow.step = "type_soberi"
            await _ask(st.flows_by_user[user_id], chat_id, "Укажите, что собираем:", _KEY_OBJECT, include_back=False)
        elif step == "marka_ts":
            if data.get("preset_company"):
                flow.step = "type_soberi"
                await _ask(st.flows_by_user[user_id], chat_id, "Укажите, что собираем:", _KEY_OBJECT, include_back=False)
            else:
                flow.step = "company"
                await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
        elif step == "type_disk":
            flow.step = "marka_ts"
            await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
        elif step == "radius":
            flow.step = "marka_ts" if data.get("type_soberi") == "Комплект" else "type_disk"
            if flow.step == "marka_ts":
                await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
            else:
                await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        elif step == "razmer":
            flow.step = "marka_ts" if data.get("type_soberi") == "Комплект" else "radius"
            if flow.step == "marka_ts":
                await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
            else:
                await _ask(flow, chat_id, "Радиус:", _list_radius(data["company"]))
        elif step == "marka_rez":
            flow.step = "razmer"
            await _ask(flow, chat_id, "Размер:", _filter_values(data["company"], radius=data["radius"], field=GHRezina.RAZMER))
        elif step == "model_rez":
            flow.step = "marka_rez"
            await _ask(flow, chat_id, "Марка резины:", _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], field=GHRezina.MARKA))
        elif step == "sezon":
            if data.get("type_soberi") == "Колесо":
                flow.step = "marka_rez"
                await _ask(flow, chat_id, "Марка резины:", _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], field=GHRezina.MARKA))
            else:
                flow.step = "razmer"
                await _ask(flow, chat_id, "Размер:", _filter_values(data["company"], radius=data["radius"], field=GHRezina.RAZMER))
        elif step == "count_1":
            flow.step = "sezon"
            await _ask(flow, chat_id, "Сезонность резины:", _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], marka=data.get("marka_rez", ""), model=data.get("model_rez", ""), short=data.get("type_soberi") != "Колесо", field=GHRezina.SEZON))
        elif step == "count_2":
            flow.step = "count_1"
            await _ask(flow, chat_id, "Уточните количество левых колес:", _KEY_CHISLA)
        return True

    if flow.step == "marka_ts" and controls & {"soberi_page_prev", "soberi_page_next"}:
        page = int(data.get("marka_ts_page", 0))
        if "soberi_page_prev" in controls:
            data["marka_ts_page"] = page - 1
        else:
            data["marka_ts_page"] = page + 1
        await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
        return True

    if step == "company":
        if t not in _KEY_COMPANY:
            await _ask(flow, chat_id, "Компания:", _KEY_COMPANY)
            return True
        data["company"] = t
        data["marka_ts_page"] = 0
        flow.step = "marka_ts"
        await _ask_marka_ts(flow, chat_id, "Марка автомобиля:")
        return True

    if step == "marka_ts":
        options = _list_marka_ts(data["company"])
        if t not in options:
            await _ask_marka_ts(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nМарка автомобиля:")
            return True
        data["marka_ts"] = t
        if data.get("type_soberi") == "Комплект":
            flow.step = "radius"
            await _ask(flow, chat_id, "Радиус:", _list_radius(data["company"]))
        else:
            flow.step = "type_disk"
            await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "type_disk":
        if t not in _KEY_TYPE_DISK:
            await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
            return True
        data["type_disk"] = t
        flow.step = "radius"
        await _ask(flow, chat_id, "Радиус:", _list_radius(data["company"]))
        return True

    if step == "radius":
        options = _list_radius(data["company"])
        if t not in options:
            await _ask(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nРадиус:", options)
            return True
        data["radius"] = t
        flow.step = "razmer"
        await _ask(flow, chat_id, "Размер:", _filter_values(data["company"], radius=t, field=GHRezina.RAZMER))
        return True

    if step == "razmer":
        options = _filter_values(data["company"], radius=data["radius"], field=GHRezina.RAZMER)
        if t not in options:
            await _ask(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nРазмер:", options)
            return True
        data["razmer"] = t
        if data.get("type_soberi") == "Колесо":
            flow.step = "marka_rez"
            await _ask(flow, chat_id, "Марка резины:", _filter_values(data["company"], radius=data["radius"], razmer=t, field=GHRezina.MARKA))
        else:
            flow.step = "sezon"
            await _ask(flow, chat_id, "Сезонность резины:", _filter_values(data["company"], radius=data["radius"], razmer=t, short=True, field=GHRezina.SEZON))
        return True

    if step == "marka_rez":
        options = _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], field=GHRezina.MARKA)
        if t not in options:
            await _ask(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nМарка резины:", options)
            return True
        data["marka_rez"] = t
        flow.step = "model_rez"
        await _ask(flow, chat_id, "Модель резины:", _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], marka=t, field=GHRezina.MODEL))
        return True

    if step == "model_rez":
        options = _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], marka=data["marka_rez"], field=GHRezina.MODEL)
        if t not in options:
            await _ask(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nМодель резины:", options)
            return True
        data["model_rez"] = t
        flow.step = "sezon"
        await _ask(flow, chat_id, "Сезонность резины:", _filter_values(data["company"], radius=data["radius"], razmer=data["razmer"], marka=data["marka_rez"], model=t, field=GHRezina.SEZON))
        return True

    if step == "sezon":
        options = _filter_values(
            data["company"],
            radius=data["radius"],
            razmer=data["razmer"],
            marka=data.get("marka_rez", ""),
            model=data.get("model_rez", ""),
            short=data.get("type_soberi") != "Колесо",
            field=GHRezina.SEZON,
        )
        if t not in options:
            await _ask(flow, chat_id, "Введенного значения нет в базе. Попробуйте еще\nСезонность резины:", options)
            return True
        data["sezon"] = t
        if data.get("type_soberi") == "Комплект":
            flow.step = "count_1"
            await _ask(flow, chat_id, "Количество комплектов:", _KEY_CHISLA)
        elif data.get("type_soberi") == "Ось":
            flow.step = "count_1"
            await _ask(flow, chat_id, "Количество осей:", _KEY_CHISLA)
        else:
            flow.step = "count_1"
            await _ask(flow, chat_id, "Уточните количество левых колес:", _KEY_CHISLA)
        return True

    if step == "count_1":
        if t not in _KEY_CHISLA:
            label = "Количество комплектов:" if data.get("type_soberi") == "Комплект" else "Количество осей:" if data.get("type_soberi") == "Ось" else "Уточните количество левых колес:"
            await _ask(flow, chat_id, label, _KEY_CHISLA)
            return True
        data["count_1"] = int(t)
        if data.get("type_soberi") in {"Комплект", "Ось"}:
            return await _finalize(st, user_id, chat_id)
        flow.step = "count_2"
        await _ask(flow, chat_id, "Уточните количество правых колес:", _KEY_CHISLA)
        return True

    if step == "count_2":
        if t not in _KEY_CHISLA:
            await _ask(flow, chat_id, "Уточните количество правых колес:", _KEY_CHISLA)
            return True
        data["count_2"] = int(t)
        return await _finalize(st, user_id, chat_id)

    return True
