from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
import logging
from typing import Dict, List, Set

from app.config import SBORKA_CHAT_ID_BELKA, SBORKA_CHAT_ID_CITY, SBORKA_CHAT_ID_YANDEX
from app.utils.helper import get_fio_async
from app.utils.max_api import delete_message, extract_message_id, send_message, send_text, send_text_with_reply_buttons
from app.utils.gsheets import (
    load_sborka_reference_data,
    nomer_sborka,
    nomer_sborka_ko,
    update_data_sborka,
    update_record_sborka,
    write_in_answers_ras,
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
class SborkaFlow:
    step: str = "company"
    data: dict = field(default_factory=dict)
    files: List[dict] = field(default_factory=list)
    file_keys: Set[str] = field(default_factory=set)


@dataclass
class SborkaState:
    flows_by_user: Dict[int, SborkaFlow] = field(default_factory=dict)


_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_TYPE_DISK = ["Литой оригинальный", "Литой неоригинальный", "Штамп"]
_KEY_TYPE_SBORKA = ["Комплект", "Ось"]
_KEY_TYPE_CHECK = ["Левое колесо", "Правое колесо", "Ось", "Комплект"]
_KEY_SIDE = ["Левое", "Правое"]
_KEY_ZAYAVKA = ["Да", "Нет"]

_ref_data: dict | None = None


def _company_key(company: str) -> str:
    if company == "СитиДрайв":
        return "city"
    if company == "Яндекс":
        return "yandex"
    return "belka"


async def _ensure_refs_loaded() -> None:
    global _ref_data
    if _ref_data is None:
        _ref_data = await load_sborka_reference_data()


def _kb_control(include_back: bool = True) -> tuple[list[str], list[str]]:
    if include_back:
        return ["Назад", "Выход"], ["sborka_back", "sborka_exit"]
    return ["Выход"], ["sborka_exit"]

async def _send_flow_text(flow: SborkaFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _ask(flow: SborkaFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    buttons, payloads = _kb_control(include_back=include_back)
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + buttons,
        button_payloads=options + payloads,
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id

async def _ask_company(flow: SborkaFlow, chat_id: int, text: str = "Компания:") -> None:
    await _ask(flow, chat_id, text, _KEY_COMPANY, include_back=False)

def _normalize(text: str) -> str:
    return text.strip().strip("«»\"'").lower()


def _control_candidates(text: str, msg: dict) -> set[str]:
    vals: list[str] = [text]
    cb = msg.get("callback")
    if isinstance(cb, dict):
        for node in (cb, cb.get("payload") if isinstance(cb.get("payload"), dict) else None):
            if not isinstance(node, dict):
                continue
            for k in ("payload", "data", "value", "command", "action", "text"):
                v = node.get(k)
                if isinstance(v, str) and v.strip():
                    vals.append(v)
    return {_normalize(v) for v in vals if isinstance(v, str) and v.strip()}


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
        if t in {"image", "video", "file", "audio"}:
            out.append({"type": t, "payload": item.get("payload")})
    return out


def _attachment_key(item: dict) -> str:
    return f"{item.get('type')}::{item.get('payload')}"


def _rows(company: str) -> tuple[list[list[str]], list[list[str]]]:
    key = _company_key(company)
    return _ref_data["rezina"][key], _ref_data["cars"][key]


def _list_radius(company: str) -> list[str]:
    rez, _ = _rows(company)
    return sorted({str(r[GHRezina.RADIUS]).strip() for r in rez if len(r) > GHRezina.RADIUS})


def _filter_values(company: str, radius: str = "", razmer: str = "", marka: str = "", model: str = "", field: int = GHRezina.RAZMER) -> list[str]:
    rez_rows, _ = _rows(company)
    vals: set[str] = set()
    for row in rez_rows:
        if len(row) <= GHRezina.MODEL:
            continue
        if radius and str(row[GHRezina.RADIUS]).strip() != radius.strip():
            continue
        if razmer and str(row[GHRezina.RAZMER]).strip() != razmer.strip():
            continue
        if marka and str(row[GHRezina.MARKA]).strip() != marka.strip():
            continue
        if model and str(row[GHRezina.MODEL]).strip() != model.strip():
            continue
        vals.add(str(row[field]).strip())
    return sorted(vals)


def _list_marka_ts(company: str) -> list[str]:
    _, car_rows = _rows(company)
    if company == "СитиДрайв":
        idx = 2
    elif company == "Яндекс":
        idx = 3
    else:
        idx = 1
    out: list[str] = []
    for row in car_rows:
        if len(row) <= idx:
            continue
        value = str(row[idx]).strip()
        if value:
            out.append(value)
    return sorted(set(out))


def _company_chat(company: str) -> int:
    if company == "СитиДрайв":
        return int(SBORKA_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(SBORKA_CHAT_ID_YANDEX)
    return int(SBORKA_CHAT_ID_BELKA)


def _render_report(data: dict, fio: str) -> str:
    report = ""
    if data.get("type") == "check":
        if data.get("type_kolesa") == "Ось":
            report += "Проверка готовой оси\n\n"
        elif data.get("type_kolesa") == "Комплект":
            report += "Проверка готового комплекта\n\n"
        else:
            report += "Проверка готового колеса\n\n"

    report += f"⌚️ {(datetime.now() + timedelta(hours=3)).strftime('%d.%m.%Y %H:%M:%S')}\n\n"
    if fio:
        report += f"👷 {fio}\n"
    report += "\n"
    report += f"🚗 {data['marka_ts']}\n\n"
    report += f"🛞 {data['marka_rez']} {data['model_rez']}\n\n"
    report += f"{data['razmer']}/{data['radius']}\n"

    season = str(data.get("sezon", ""))
    if season.startswith("Лето"):
        report += f"☀️ {season}\n"
    elif season.startswith("Зима"):
        report += f"❄️ {season}\n"
    else:
        report += f"{season}\n"

    report += f"{data['type_disk']}\n"

    wheel_type = str(data.get("type_kolesa", ""))
    if wheel_type == "Левое":
        report += "⬅️ Левое\n"
    elif wheel_type == "Правое":
        report += "➡️ Правое\n"
    elif wheel_type == "Ось":
        report += "↔️ Ось\n"
    elif wheel_type == "Комплект":
        report += "🔄 Комплект\n"
    elif wheel_type:
        report += f"{wheel_type}\n"

    report += f"\n#{data['company']}\n"
    report += f"\n📝 Сбор под заявку: {data['zayavka']}\n"
    report += f"\n#️⃣ Номер заявки: {data.get('nomer_sborka', '')}\n"
    return report


async def cmd_sborka(st: SborkaState, user_id: int, chat_id: int, username: str, cmd: str = "sborka") -> None:
    await _ensure_refs_loaded()
    if cmd == "check":
        st.flows_by_user[user_id] = SborkaFlow(step="company", data={"username": username, "type_sborka": "sborka", "type": "check"})
        await _ask_company(st.flows_by_user[user_id], chat_id)
        return
    st.flows_by_user[user_id] = SborkaFlow(step="company", data={"username": username, "type_sborka": cmd, "type": "sborka"})
    await _ask_company(st.flows_by_user[user_id], chat_id)


def _clear(st: SborkaState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)

async def warmup_sborka_refs() -> None:
    await _ensure_refs_loaded()


def reset_sborka_progress(st: SborkaState, user_id: int) -> None:
    _clear(st, user_id)

async def start_from_damage_transfer(st: SborkaState, user_id: int, chat_id: int, username: str, transfer: dict) -> None:
    prefill = dict(transfer.get("prefill") or {})
    prefill["username"] = username
    prefill["type"] = "sborka"
    prefill["type_sborka"] = prefill.get("type_sborka") or "sborka"

    mode = str(transfer.get("mode") or "")
    if mode == "replace_tire_confirm":
        st.flows_by_user[user_id] = SborkaFlow(step="damage_replace_tire_confirm", data=prefill)
        await _ask(st.flows_by_user[user_id], chat_id, "Есть покрышка на замену:", _KEY_ZAYAVKA)
        return

    if mode == "replace_disk_confirm":
        st.flows_by_user[user_id] = SborkaFlow(step="damage_replace_disk_confirm", data=prefill)
        await _ask(st.flows_by_user[user_id], chat_id, "Есть другой диск на замену:", _KEY_ZAYAVKA)
        return

    if mode == "pick_disk":
        st.flows_by_user[user_id] = SborkaFlow(step="damage_pick_disk", data=prefill)
        await _ask(st.flows_by_user[user_id], chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return

    st.flows_by_user[user_id] = SborkaFlow(step="damage_pick_side", data=prefill)
    await _ask(st.flows_by_user[user_id], chat_id, "Уточните какое колесо:", _KEY_SIDE)

async def _send_files_prompt(flow: SborkaFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id,
        text,
        ["Готово", "Выход"],
        ["sborka_done", "sborka_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id

def _write_sborka_rows(data: dict, fio: str) -> None:
    base = [
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        data["company"],
        data["marka_ts"],
        data["radius"],
        data["razmer"],
        data["marka_rez"],
        data["model_rez"],
        data["sezon"],
        data["type_disk"],
        data["type_kolesa"],
        data["zayavka"],
        data.get("nomer_sborka", ""),
        "",
        fio,
    ]

    pos = data["type_kolesa"]
    if pos not in ("Комплект", "Ось"):
        write_in_answers_ras(base, "Выгрузка сборка")
        write_in_answers_ras(base, "Онлайн остатки Хаба")
        return

    if pos == "Комплект":
        sides = [("Правое", 2), ("Левое", 2)]
    else:
        sides = [("Правое", 1), ("Левое", 1)]

    for side, count in sides:
        for _ in range(count):
            row = base.copy()
            row[9] = side
            write_in_answers_ras(row, "Выгрузка сборка")
            write_in_answers_ras(row, "Онлайн остатки Хаба")

def _write_check_rows(data: dict, fio: str) -> None:
    count = 1
    if data.get("type_check") == "Ось":
        count = 2
    elif data.get("type_check") == "Комплект":
        count = 4

    row = [
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        data["company"],
        "Проверка колеса",
        "",
        data["marka_ts"],
        data["radius"],
        data["razmer"],
        data["marka_rez"],
        data["model_rez"],
        data["sezon"],
        data["type_disk"],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        fio,
    ]
    for _ in range(count):
        write_in_answers_ras(row, "Выгрузка ремонты/утиль")

async def _finalize(st: SborkaState, user_id: int, chat_id: int, msg: dict) -> bool:
    flow = st.flows_by_user[user_id]
    if not flow.files:
        await _send_flow_text(flow, chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    data = flow.data
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    report = _render_report(data, fio)

    response = await send_message(chat_id=_company_chat(data["company"]), text=report, attachments=flow.files)
    msg_ref = ""
    if isinstance(response, dict):
        msg_ref = str(response.get("message_id") or response.get("id") or "")

    try:
        if data.get("type") == "check":
            _write_check_rows(data, fio)
        else:
            _write_sborka_rows(data, fio)
    except Exception:
        logger.exception("failed to write sborka/check rows")

    if data.get("type_sborka") == "sborka":
        try:
            update_data_sborka(data["marka_rez"], data["model_rez"], data["type_disk"], data["type_kolesa"], data.get("nomer_sborka", ""))
        except Exception:
            logger.exception("failed update_data_sborka")

    if data.get("nomer_sborka") and data.get("nomer_sborka") != "не найден":
        try:
            update_record_sborka(
                data["company"],
                username,
                data["radius"],
                data["razmer"],
                data["marka_rez"],
                data["model_rez"],
                data["sezon"],
                data["marka_ts"],
                data["type_disk"],
                data["type_kolesa"],
                "",
                data["nomer_sborka"],
            )
        except Exception:
            logger.exception("failed update_record_sborka")

    prompt_msg_id = flow.data.get("prompt_msg_id")
    if prompt_msg_id:
        await delete_message(chat_id, prompt_msg_id)
    _clear(st, user_id)
    await send_text(chat_id, "Ваша заявка сформирована")
    return True

async def _handle_back(flow: SborkaFlow, chat_id: int) -> bool:
    step = flow.step
    company = flow.data.get("company", "")

    if step in {"damage_replace_tire_confirm", "damage_replace_disk_confirm", "damage_pick_disk", "damage_pick_side"}:
        return True

    if step == "company":
        return True

    if step == "type_disk":
        if flow.data.get("type") == "check":
            flow.step = "type_check"
            await _ask(flow, chat_id, "Укажите, что проверяем:", _KEY_TYPE_CHECK)
            return True
        flow.step = "company"
        await _ask_company(flow, chat_id)
        return True

    if step == "radius":
        if flow.data.get("type") == "check":
            flow.step = "type_disk"
            await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
            return True
        flow.step = "type_disk"
        await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "razmer":
        flow.step = "radius"
        await _ask(flow, chat_id, "Радиус:", _list_radius(company))
        return True

    if step == "marka":
        flow.step = "razmer"
        await _ask(flow, chat_id, "Размер:", _filter_values(company, radius=flow.data.get("radius", ""), field=GHRezina.RAZMER))
        return True

    if step == "model":
        flow.step = "marka"
        await _ask(flow, chat_id, "Марка резины:", _filter_values(company, radius=flow.data.get("radius", ""), razmer=flow.data.get("razmer", ""), field=GHRezina.MARKA))
        return True

    if step == "sezon":
        flow.step = "model"
        await _ask(flow, chat_id, "Модель резины:", _filter_values(company, radius=flow.data.get("radius", ""), razmer=flow.data.get("razmer", ""), marka=flow.data.get("marka_rez", ""), field=GHRezina.MODEL))
        return True

    if step == "marka_ts":
        if flow.data.get("type") == "check":
            flow.step = "company"
            await _ask_company(flow, chat_id)
            return True
        flow.step = "sezon"
        await _ask(flow, chat_id, "Сезон:", _filter_values(company, radius=flow.data.get("radius", ""), razmer=flow.data.get("razmer", ""), marka=flow.data.get("marka_rez", ""), model=flow.data.get("model_rez", ""), field=GHRezina.SEZON))
        return True

    if step == "type_check":
        flow.step = "marka_ts"
        await _ask(flow, chat_id, "Марка авто:", _list_marka_ts(company)[:40])
        return True

    if step == "type_kolesa":
        flow.step = "marka_ts"
        await _ask(flow, chat_id, "Марка авто:", _list_marka_ts(company)[:40])
        return True

    if step == "zayavka":
        if flow.data.get("type") == "check":
            flow.step = "sezon"
            await _ask(flow, chat_id, "Сезон:", _filter_values(company, radius=flow.data.get("radius", ""), razmer=flow.data.get("razmer", ""), marka=flow.data.get("marka_rez", ""), model=flow.data.get("model_rez", ""), field=GHRezina.SEZON))
            return True
        flow.step = "type_kolesa"
        await _ask(flow, chat_id, "Вид сборки:", _KEY_TYPE_SBORKA + _KEY_SIDE)
        return True

    if step == "nomer":
        flow.step = "zayavka"
        await _ask(flow, chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
        return True

    if step == "files":
        flow.step = "nomer"
        candidates = nomer_sborka(
            flow.data["company"], flow.data["radius"], flow.data["razmer"], flow.data["marka_rez"],
            flow.data["model_rez"], flow.data["sezon"], flow.data["marka_ts"], flow.data["type_disk"], flow.data["type_kolesa"]
        )
        if flow.data.get("type_sborka") == "sborka_ko":
            candidates = nomer_sborka_ko(
                flow.data["company"], flow.data["radius"], flow.data["razmer"], flow.data["marka_rez"],
                flow.data["model_rez"], flow.data["sezon"], flow.data["marka_ts"], flow.data["type_disk"], flow.data["type_kolesa"]
            )
        if candidates:
            await _ask(flow, chat_id, "Номер заявки:", sorted(set(candidates))[:50])
        else:
            await _send_flow_text(flow, chat_id, "Номер заявки не найден. Отправьте номер вручную или текст 'не найден'.")
        return True

    return True


async def try_handle_sborka_step(st: SborkaState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if flow is None:
        return False

    controls = _control_candidates(text, msg)
    if controls & {"выход", "sborka_exit"}:
        prompt_msg_id = flow.data.get("prompt_msg_id")
        if prompt_msg_id:
            await delete_message(chat_id, prompt_msg_id)
        _clear(st, user_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    if controls & {"назад", "sborka_back"}:
        return await _handle_back(flow, chat_id)

    step = flow.step
    t = text.strip()

    if step == "damage_replace_tire_confirm":
        if t not in _KEY_ZAYAVKA:
            await _ask(flow, chat_id, "Есть покрышка на замену:", _KEY_ZAYAVKA)
            return True
        if t == "Нет":
            prompt_msg_id = flow.data.get("prompt_msg_id")
            if prompt_msg_id:
                await delete_message(chat_id, prompt_msg_id)
            _clear(st, user_id)
            await send_text(chat_id, "Завершение формирования заявки")
            return True
        flow.step = "razmer"
        await _ask(flow, chat_id, "Размер:", _filter_values(flow.data["company"], radius=flow.data.get("radius", ""), field=GHRezina.RAZMER))
        return True

    if step == "damage_replace_disk_confirm":
        if t not in _KEY_ZAYAVKA:
            await _ask(flow, chat_id, "Есть другой диск на замену:", _KEY_ZAYAVKA)
            return True
        if t == "Нет":
            prompt_msg_id = flow.data.get("prompt_msg_id")
            if prompt_msg_id:
                await delete_message(chat_id, prompt_msg_id)
            _clear(st, user_id)
            await send_text(chat_id, "Завершение формирования заявки")
            return True
        flow.step = "damage_pick_disk"
        await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "damage_pick_disk":
        if t not in _KEY_TYPE_DISK:
            await _ask(flow, chat_id, "Выберите тип диска:", _KEY_TYPE_DISK)
            return True
        flow.data["type_disk"] = t
        flow.step = "damage_pick_side"
        await _ask(flow, chat_id, "Уточните какое колесо:", _KEY_SIDE)
        return True

    if step == "damage_pick_side":
        if t not in _KEY_SIDE:
            await _ask(flow, chat_id, "Уточните какое колесо:", _KEY_SIDE)
            return True
        flow.data["type_kolesa"] = t
        flow.step = "zayavka"
        await _ask(flow, chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
        return True

    if step == "company":
        if t not in _KEY_COMPANY:
            await _ask_company(flow, chat_id, "Выберите компанию:")
            return True
        flow.data["company"] = t
        if flow.data.get("type") == "check":
            flow.step = "marka_ts"
            await _ask(flow, chat_id, "Марка авто:", _list_marka_ts(flow.data["company"])[:40])
            return True
        flow.step = "type_disk"
        await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "type_disk":
        if t not in _KEY_TYPE_DISK:
            await _ask(flow, chat_id, "Выберите тип диска:", _KEY_TYPE_DISK)
            return True
        flow.data["type_disk"] = t
        flow.step = "radius"
        await _ask(flow, chat_id, "Радиус:", _list_radius(flow.data["company"]))
        return True

    if step == "radius":
        options = _list_radius(flow.data["company"])
        if t not in options:
            await _ask(flow, chat_id, "Выберите радиус:", options)
            return True
        flow.data["radius"] = t
        flow.step = "razmer"
        await _ask(flow, chat_id, "Размер:", _filter_values(flow.data["company"], radius=t, field=GHRezina.RAZMER))
        return True

    if step == "razmer":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], field=GHRezina.RAZMER)
        if t not in options:
            await _ask(flow, chat_id, "Выберите размер:", options)
            return True
        flow.data["razmer"] = t
        flow.step = "marka"
        await _ask(flow, chat_id, "Марка резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=t, field=GHRezina.MARKA))
        return True

    if step == "marka":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], field=GHRezina.MARKA)
        if t not in options:
            await _ask(flow, chat_id, "Выберите марку резины:", options)
            return True
        flow.data["marka_rez"] = t
        flow.step = "model"
        await _ask(flow, chat_id, "Модель резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=t, field=GHRezina.MODEL))
        return True

    if step == "model":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], field=GHRezina.MODEL)
        if t not in options:
            await _ask(flow, chat_id, "Выберите модель резины:", options)
            return True
        flow.data["model_rez"] = t
        flow.step = "sezon"
        await _ask(flow, chat_id, "Сезон:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=t, field=GHRezina.SEZON))
        return True

    if step == "sezon":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=flow.data["model_rez"], field=GHRezina.SEZON)
        if t not in options:
            await _ask(flow, chat_id, "Выберите сезон:", options)
            return True
        flow.data["sezon"] = t
        if flow.data.get("type") == "check":
            flow.step = "zayavka"
            await _ask(flow, chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
            return True
        flow.step = "marka_ts"
        await _ask(flow, chat_id, "Марка авто:", _list_marka_ts(flow.data["company"])[:40])
        return True

    if step == "marka_ts":
        options = _list_marka_ts(flow.data["company"])
        if t not in options:
            await _ask(flow, chat_id, "Выберите марку авто:", options[:40])
            return True
        flow.data["marka_ts"] = t
        if flow.data.get("type") == "check":
            flow.step = "type_check"
            await _ask(flow, chat_id, "Укажите, что проверяем:", _KEY_TYPE_CHECK)
            return True
        flow.step = "type_kolesa"
        await _ask(flow, chat_id, "Вид сборки:", _KEY_TYPE_SBORKA)
        return True

    if step == "type_check":
        if t not in _KEY_TYPE_CHECK:
            await _ask(flow, chat_id, "Укажите, что проверяем:", _KEY_TYPE_CHECK)
            return True
        flow.data["type_check"] = t
        base = t.split()[0]
        flow.data["type_kolesa"] = base
        flow.data["type_sborka"] = "sborka_ko" if base in ("Ось", "Комплект") else "sborka"
        flow.step = "type_disk"
        await _ask(flow, chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "type_kolesa":
        options = _KEY_TYPE_SBORKA + _KEY_SIDE
        if t not in options:
            await _ask(flow, chat_id, "Выберите вид сборки:", options)
            return True
        flow.data["type_kolesa"] = t
        flow.step = "zayavka"
        await _ask(flow, chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
        return True

    if step == "zayavka":
        if t not in _KEY_ZAYAVKA:
            await _ask(flow, chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
            return True
        flow.data["zayavka"] = t
        flow.step = "nomer"
        candidates = nomer_sborka(
            flow.data["company"], flow.data["radius"], flow.data["razmer"], flow.data["marka_rez"],
            flow.data["model_rez"], flow.data["sezon"], flow.data["marka_ts"], flow.data["type_disk"], flow.data["type_kolesa"]
        )
        if flow.data.get("type_sborka") == "sborka_ko":
            candidates = nomer_sborka_ko(
                flow.data["company"], flow.data["radius"], flow.data["razmer"], flow.data["marka_rez"],
                flow.data["model_rez"], flow.data["sezon"], flow.data["marka_ts"], flow.data["type_disk"], flow.data["type_kolesa"]
            )
        if candidates:
            await _ask(flow, chat_id, "Номер заявки:", sorted(set(candidates))[:50])
        else:
            await _send_flow_text(flow, chat_id, "Номер заявки не найден. Отправьте номер вручную или текст 'не найден'.")
        return True

    if step == "nomer":
        flow.data["nomer_sborka"] = t or "не найден"
        flow.step = "files"
        await _send_files_prompt(flow, chat_id, "Прикрепите фото/видео/файл и нажмите «Готово»")
        return True

    if step == "files":
        if controls & {"готово", "sborka_done"}:
            return await _finalize(st, user_id, chat_id, msg)

        attachments = _extract_attachments(msg, include_nested=not isinstance(msg.get("callback"), dict))
        if attachments:
            new_items = 0
            for item in attachments:
                key = _attachment_key(item)
                if key in flow.file_keys:
                    continue
                flow.file_keys.add(key)
                flow.files.append(item)
                new_items += 1
            await _send_files_prompt(flow, chat_id, f"Файлов добавлено: {new_items}. Текущее количество: {len(flow.files)}")
            return True

        await _send_files_prompt(flow, chat_id, "Прикрепите файлы и нажмите «Готово»")
        return True

    return True
