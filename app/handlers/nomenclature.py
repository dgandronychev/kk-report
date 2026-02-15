from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Dict

from app.config import NOMENCLATURE_ALLOWED_USER_IDS
from app.utils.gsheets import load_nomenclature_reference_data, write_in_answers_ras_nomen
from app.utils.max_api import send_text, send_text_with_reply_buttons

logger = logging.getLogger(__name__)


@dataclass
class NomenclatureFlow:
    step: str = "company"
    data: dict = field(default_factory=dict)


@dataclass
class NomenclatureState:
    flows_by_user: Dict[int, NomenclatureFlow] = field(default_factory=dict)


_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_RADIUS = ["15", "16", "17", "18", "19", "20"]
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


async def _ensure_refs_loaded() -> None:
    global _ref_data
    if _ref_data is None:
        _ref_data = await load_nomenclature_reference_data()


def _company_key(company: str) -> str:
    if company == "СитиДрайв":
        return "city"
    if company == "Яндекс":
        return "yandex"
    return "belka"


def _rows(company: str) -> list[list[str]]:
    return _ref_data[_company_key(company)]


def _column_values(company: str, idx: int) -> list[str]:
    vals: list[str] = []
    for row in _rows(company):
        if len(row) > idx and str(row[idx]).strip():
            vals.append(str(row[idx]).strip())
    return sorted(set(vals))


async def _ask(chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    button_texts = options + (["Назад"] if include_back else []) + ["Выход"]
    payloads = options + (["nomenclature_back"] if include_back else []) + ["nomenclature_exit"]
    await send_text_with_reply_buttons(chat_id, text, button_texts=button_texts, button_payloads=payloads)


def _clear(st: NomenclatureState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)


async def cmd_nomenclature(st: NomenclatureState, user_id: int, chat_id: int, username: str) -> None:
    if NOMENCLATURE_ALLOWED_USER_IDS and user_id not in NOMENCLATURE_ALLOWED_USER_IDS:
        await send_text(chat_id, "У вас нет прав для вызова данной команды")
        return

    await _ensure_refs_loaded()
    st.flows_by_user[user_id] = NomenclatureFlow(step="company", data={"username": username})
    await _ask(chat_id, "Компания:", _KEY_COMPANY, include_back=False)


def _save(data: dict) -> None:
    company = data.get("company", "")
    if company == "СитиДрайв":
        company_cell = "Сити"
        sheet = "Резина Сити"
    elif company == "Яндекс":
        company_cell = "Яндекс"
        sheet = "Резина ЯД"
    else:
        company_cell = "Белка"
        sheet = "Резина Белка"

    row = [
        "",
        data.get("radius", ""),
        data.get("razmer", ""),
        data.get("sezon", ""),
        data.get("marka", ""),
        data.get("model", ""),
        company_cell,
        data.get("al", ""),
    ]
    write_in_answers_ras_nomen(row, sheet)


async def try_handle_nomenclature_step(st: NomenclatureState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if flow is None:
        return False

    controls = _control_candidates(text, msg)
    if controls & {"выход", "nomenclature_exit"}:
        _clear(st, user_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    t = text.strip()
    step = flow.step
    data = flow.data

    if controls & {"назад", "nomenclature_back"}:
        if step == "radius":
            flow.step = "company"
            await _ask(chat_id, "Компания:", _KEY_COMPANY, include_back=False)
        elif step == "razmer":
            flow.step = "radius"
            await _ask(chat_id, "Введите радиус:", _KEY_RADIUS)
        elif step == "marka":
            flow.step = "razmer"
            await _ask(chat_id, "Введите размер:", _column_values(data["company"], 2))
        elif step == "model":
            flow.step = "marka"
            await _ask(chat_id, "Введите марку резины:", _column_values(data["company"], 4))
        elif step == "sezon":
            flow.step = "model"
            await _ask(chat_id, "Введите модель резины:", [], include_back=True)
        elif step == "al":
            flow.step = "sezon"
            await _ask(chat_id, "Введите сезонность резины:", _column_values(data["company"], 3))
        return True

    if step == "company":
        if t not in _KEY_COMPANY:
            await _ask(chat_id, "Выберите компанию:", _KEY_COMPANY, include_back=False)
            return True
        data["company"] = t
        flow.step = "radius"
        await _ask(chat_id, "Введите радиус:", _KEY_RADIUS)
        return True

    if step == "radius":
        data["radius"] = t
        flow.step = "razmer"
        await _ask(chat_id, "Введите размер:", _column_values(data["company"], 2))
        return True

    if step == "razmer":
        data["razmer"] = t
        flow.step = "marka"
        await _ask(chat_id, "Введите марку резины:", _column_values(data["company"], 4))
        return True

    if step == "marka":
        data["marka"] = t
        flow.step = "model"
        await _ask(chat_id, "Введите модель резины:", [], include_back=True)
        return True

    if step == "model":
        data["model"] = t
        flow.step = "sezon"
        await _ask(chat_id, "Введите сезонность резины:", _column_values(data["company"], 3))
        return True

    if step == "sezon":
        data["sezon"] = t
        if data["company"] == "СитиДрайв":
            flow.step = "al"
            await _ask(chat_id, "Введите АЛ:", [], include_back=True)
            return True
        _save(data)
        _clear(st, user_id)
        await send_text(chat_id, "Добавление новой номенклатуры выполнено")
        return True

    if step == "al":
        data["al"] = t
        _save(data)
        _clear(st, user_id)
        await send_text(chat_id, "Добавление новой номенклатуры выполнено")
        return True

    return True
