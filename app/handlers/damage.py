from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
import logging
from typing import Dict, List, Optional, Set

from app.config import DAMAGE_CHAT_ID_BELKA, DAMAGE_CHAT_ID_CITY, DAMAGE_CHAT_ID_YANDEX
from app.utils.gsheets import load_damage_reference_data, write_in_answers_ras
from app.utils.helper import get_fio_async
from app.utils.max_api import send_message, send_text, send_text_with_reply_buttons

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
class DamageFlow:
    step: str = "company"
    data: dict = field(default_factory=dict)
    files: List[dict] = field(default_factory=list)
    file_keys: Set[str] = field(default_factory=set)


@dataclass
class DamageState:
    flows_by_user: Dict[int, DamageFlow] = field(default_factory=dict)


_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_TYPE = ["В сборе", "Только резина"]
_KEY_TYPE_DISK = ["Литой оригинальный", "Литой неоригинальный", "Штамп"]
_KEY_CONDITION = ["Ок", "Ремонт", "Утиль"]
_KEY_REASON_TIRE_UTIL = ["Езда на спущенном", "Износ протектора", "Боковой пробой", "Грыжа"]
_KEY_REASON_TIRE_REPAIR = ["Латка", "Грибок", "Замена вентиля", "Герметик борта"]
_KEY_REASON_DISK = ["Искревление ОСИ", "Трещина", "Отколот кусок", "Замена датчика давления"]


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
        _ref_data = await load_damage_reference_data()


def _kb_control() -> tuple[list[str], list[str]]:
    return ["Назад", "Выход"], ["damage_back", "damage_exit"]


async def _ask(chat_id: int, text: str, options: list[str]) -> None:
    buttons, payloads = _kb_control()
    await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + buttons,
        button_payloads=options + payloads,
    )


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


def _rows_by_company(company: str) -> tuple[list[list[str]], list[list[str]]]:
    key = _company_key(company)
    rez_rows = _ref_data["rezina"][key]
    car_rows = _ref_data["cars"][key]
    return rez_rows, car_rows


def _list_radius(company: str) -> list[str]:
    rez_rows, _ = _rows_by_company(company)
    return sorted({str(r[GHRezina.RADIUS]).strip() for r in rez_rows if len(r) > GHRezina.RADIUS})


def _filter_values(company: str, radius: str = "", razmer: str = "", marka: str = "", model: str = "", field: int = GHRezina.RAZMER) -> list[str]:
    rez_rows, _ = _rows_by_company(company)
    vals: set[str] = set()
    for row in rez_rows:
        if len(row) <= max(field, GHRezina.MODEL):
            continue
        if radius and str(row[GHRezina.RADIUS]).strip() != str(radius).strip():
            continue
        if razmer and str(row[GHRezina.RAZMER]).strip() != str(razmer).strip():
            continue
        if marka and str(row[GHRezina.MARKA]).strip() != str(marka).strip():
            continue
        if model and str(row[GHRezina.MODEL]).strip() != str(model).strip():
            continue
        vals.add(str(row[field]).strip())
    return sorted(vals)


def _find_car_mark(company: str, grz: str) -> str:
    _, car_rows = _rows_by_company(company)
    if company == "СитиДрайв":
        grz_idx, model_idx = 0, 2
    elif company == "Яндекс":
        grz_idx, model_idx = 0, 3
    else:
        grz_idx, model_idx = 2, 1
    for row in car_rows:
        if len(row) > model_idx and len(row) > grz_idx and str(row[grz_idx]).strip().lower() == grz.lower().strip():
            return str(row[model_idx]).strip()
    return ""


def _find_grz_matches(company: str, prefix: str) -> list[str]:
    _, car_rows = _rows_by_company(company)
    idx = 2 if company == "Белка" else 0
    out = []
    for row in car_rows:
        if len(row) <= idx:
            continue
        val = str(row[idx]).strip()
        if val.lower().startswith(prefix.lower().strip()):
            out.append(val)
    return sorted(set(out))


def _company_chat_id(company: str) -> int:
    if company == "СитиДрайв":
        return int(DAMAGE_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(DAMAGE_CHAT_ID_YANDEX)
    return int(DAMAGE_CHAT_ID_BELKA)


def _render_report(data: dict, fio: str, username: str) -> str:
    lines = [
        f"⌚️ {(datetime.now() + timedelta(hours=3)).strftime('%d.%m.%Y %H:%M:%S')}",
        "",
        f"👷 {username}",
        fio,
        "",
        f"#Компания_{data['company'].replace(' ', '_')}",
        f"#Тип_{data['vid_kolesa'].replace(' ', '_')}",
        f"#ГРЗ_{data.get('grz','б/н')}",
        f"#Авто_{data.get('marka_ts','')}",
        f"#Размер_{data['razmer']}/{data['radius']}",
        f"#Резина_{data['marka_rez']} {data['model_rez']}",
        f"#Сезон_{data['sezon'].replace(' ', '_')}",
        f"#Диск_{data['type_disk'].replace(' ', '_')}",
        f"#Сост_диск_{data['sost_disk'].replace(' ', '_')}",
        f"#Причина_диск_{data.get('sost_disk_prich','').replace(' ', '_')}",
        f"#Сост_резина_{data['sost_rez'].replace(' ', '_')}",
        f"#Причина_резина_{data.get('sost_rez_prich','').replace(' ', '_')}",
    ]
    return "\n".join(lines)


async def cmd_damage(st: DamageState, user_id: int, chat_id: int, username: str) -> None:
    await _ensure_refs_loaded()
    st.flows_by_user[user_id] = DamageFlow(step="company", data={"username": username})
    await _ask(chat_id, "Компания:", _KEY_COMPANY)


def _clear(st: DamageState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)


async def _finalize(st: DamageState, user_id: int, chat_id: int, msg: dict) -> bool:
    flow = st.flows_by_user[user_id]
    if not flow.files:
        await send_text(chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    data = flow.data
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    username = f"@{data.get('username') or user_id}"
    report = _render_report(data, fio, username)

    response = await send_message(chat_id=_company_chat_id(data["company"]), text=report, attachments=flow.files)
    msg_ref = ""
    if isinstance(response, dict):
        msg_ref = str(response.get("message_id") or response.get("id") or "")

    row = [
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        data["company"],
        data["vid_kolesa"],
        data.get("grz", ""),
        data.get("marka_ts", ""),
        data["radius"],
        data["razmer"],
        data["marka_rez"],
        data["model_rez"],
        data["sezon"],
        data["type_disk"],
        data["sost_disk"],
        data.get("sost_disk_prich", ""),
        "",
        data["sost_rez"],
        data.get("sost_rez_prich", ""),
        "",
        msg_ref,
        username,
    ]
    try:
        write_in_answers_ras(row, "Выгрузка ремонты/утиль")
    except Exception:
        logger.exception("failed to write damage report to google sheets")

    _clear(st, user_id)
    await send_text(chat_id, "Ваша заявка на оформление повреждения сформирована")
    return True


async def try_handle_damage_step(st: DamageState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if flow is None:
        return False

    controls = _control_candidates(text, msg)
    if controls & {"выход", "damage_exit"}:
        _clear(st, user_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    if controls & {"назад", "damage_back"}:
        await send_text(chat_id, "Используйте /damage для перезапуска анкеты")
        return True

    step = flow.step
    t = text.strip()

    if step == "company":
        if t not in _KEY_COMPANY:
            await _ask(chat_id, "Выберите компанию:", _KEY_COMPANY)
            return True
        flow.data["company"] = t
        flow.step = "wheel_type"
        await _ask(chat_id, "Вид колеса:", _KEY_TYPE)
        return True

    if step == "wheel_type":
        if t not in _KEY_TYPE:
            await _ask(chat_id, "Выберите вид колеса:", _KEY_TYPE)
            return True
        flow.data["vid_kolesa"] = t
        if t == "В сборе":
            flow.step = "grz"
            await send_text(chat_id, "Начните ввод госномера задачи:")
        else:
            flow.step = "radius"
            await _ask(chat_id, "Радиус:", _list_radius(flow.data["company"]))
        return True

    if step == "grz":
        matches = _find_grz_matches(flow.data["company"], t)
        flow.data["grz"] = t
        flow.step = "marka_ts"
        if matches:
            await _ask(chat_id, "Подтвердите ГРЗ из списка или отправьте свой:", matches[:20])
            return True
        await send_text(chat_id, "Номер не найден в базе, ввод продолжен вручную")
        marka = _find_car_mark(flow.data["company"], t)
        if marka:
            await send_text(chat_id, f"Марка автомобиля (из базы): {marka}. Можете отправить другую вручную.")
        else:
            await send_text(chat_id, "Введите марку/модель автомобиля:")
        return True

    if step == "marka_ts":
        flow.data["marka_ts"] = t
        flow.step = "radius"
        await _ask(chat_id, "Радиус:", _list_radius(flow.data["company"]))
        return True

    if step == "radius":
        options = _list_radius(flow.data["company"])
        if t not in options:
            await _ask(chat_id, "Введенного радиуса нет в базе. Выберите из списка:", options)
            return True
        flow.data["radius"] = t
        flow.step = "razmer"
        await _ask(chat_id, "Размер:", _filter_values(flow.data["company"], radius=t, field=GHRezina.RAZMER))
        return True

    if step == "razmer":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], field=GHRezina.RAZMER)
        if t not in options:
            await _ask(chat_id, "Введенного размера нет в базе. Выберите из списка:", options)
            return True
        flow.data["razmer"] = t
        flow.step = "marka_rez"
        await _ask(chat_id, "Марка резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=t, field=GHRezina.MARKA))
        return True

    if step == "marka_rez":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], field=GHRezina.MARKA)
        if t not in options:
            await _ask(chat_id, "Введенной марки нет в базе. Выберите из списка:", options)
            return True
        flow.data["marka_rez"] = t
        flow.step = "model_rez"
        await _ask(chat_id, "Модель резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=t, field=GHRezina.MODEL))
        return True

    if step == "model_rez":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], field=GHRezina.MODEL)
        if t not in options:
            await _ask(chat_id, "Введенной модели нет в базе. Выберите из списка:", options)
            return True
        flow.data["model_rez"] = t
        flow.step = "sezon"
        await _ask(chat_id, "Сезонность:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=t, field=GHRezina.SEZON))
        return True

    if step == "sezon":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=flow.data["model_rez"], field=GHRezina.SEZON)
        if t not in options:
            await _ask(chat_id, "Введенного сезона нет в базе. Выберите из списка:", options)
            return True
        flow.data["sezon"] = t
        flow.step = "type_disk"
        await _ask(chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "type_disk":
        if t not in _KEY_TYPE_DISK:
            await _ask(chat_id, "Выберите тип диска:", _KEY_TYPE_DISK)
            return True
        flow.data["type_disk"] = t
        flow.step = "sost_disk"
        await _ask(chat_id, "Состояние диска:", _KEY_CONDITION)
        return True

    if step == "sost_disk":
        if t not in _KEY_CONDITION:
            await _ask(chat_id, "Выберите состояние диска:", _KEY_CONDITION)
            return True
        flow.data["sost_disk"] = t
        if t == "Ок":
            flow.data["sost_disk_prich"] = ""
            flow.step = "sost_rez"
            await _ask(chat_id, "Состояние резины:", _KEY_CONDITION)
            return True
        flow.step = "sost_disk_prich"
        await _ask(chat_id, "Причина повреждения диска:", _KEY_REASON_DISK)
        return True

    if step == "sost_disk_prich":
        flow.data["sost_disk_prich"] = t
        flow.step = "sost_rez"
        await _ask(chat_id, "Состояние резины:", _KEY_CONDITION)
        return True

    if step == "sost_rez":
        if t not in _KEY_CONDITION:
            await _ask(chat_id, "Выберите состояние резины:", _KEY_CONDITION)
            return True
        flow.data["sost_rez"] = t
        if t == "Ок":
            flow.data["sost_rez_prich"] = ""
            flow.step = "files"
            await _ask(chat_id, "Прикрепите файлы и нажмите «Готово»", ["Готово"])
            return True
        flow.step = "sost_rez_prich"
        reasons = _KEY_REASON_TIRE_UTIL if t == "Утиль" else _KEY_REASON_TIRE_REPAIR
        await _ask(chat_id, "Причина по резине:", reasons)
        return True

    if step == "sost_rez_prich":
        flow.data["sost_rez_prich"] = t
        flow.step = "files"
        await _ask(chat_id, "Прикрепите файлы и нажмите «Готово»", ["Готово"])
        return True

    if step == "files":
        if _normalize(t) == "готово":
            return await _finalize(st, user_id, chat_id, msg)

        attachments = _extract_attachments(msg, include_nested=not isinstance(msg.get("callback"), dict))
        if not attachments:
            await _ask(chat_id, "Прикрепите минимум 1 файл и нажмите «Готово»", ["Готово"])
            return True

        added = 0
        for item in attachments:
            key = f"{item.get('type')}::{item.get('payload')}"
            if key in flow.file_keys:
                continue
            flow.file_keys.add(key)
            flow.files.append(item)
            added += 1
        await send_text(chat_id, f"Файлов добавлено: {added}. Текущее количество: {len(flow.files)}")
        return True

    return True
