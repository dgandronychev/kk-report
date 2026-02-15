from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
import logging
from typing import Dict, List, Set

from app.config import SBORKA_CHAT_ID_BELKA, SBORKA_CHAT_ID_CITY, SBORKA_CHAT_ID_YANDEX
from app.utils.helper import get_fio_async
from app.utils.max_api import send_message, send_text, send_text_with_reply_buttons
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


def _kb_control() -> tuple[list[str], list[str]]:
    return ["Назад", "Выход"], ["sborka_back", "sborka_exit"]


async def _ask(chat_id: int, text: str, options: list[str]) -> None:
    buttons, payloads = _kb_control()
    await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=options + buttons,
        button_payloads=options + payloads,
    )


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
    out = []
    for row in car_rows:
        if len(row) > idx:
            out.append(str(row[idx]).strip())
    return sorted(set(out))


def _company_chat(company: str) -> int:
    if company == "СитиДрайв":
        return int(SBORKA_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(SBORKA_CHAT_ID_YANDEX)
    return int(SBORKA_CHAT_ID_BELKA)


def _render_report(data: dict, fio: str, username: str) -> str:
    prefix = ""
    if data.get("type") == "check":
        if data.get("type_kolesa") == "Ось":
            prefix = "Проверка готовой оси\n\n"
        elif data.get("type_kolesa") == "Комплект":
            prefix = "Проверка готового комплекта\n\n"
        else:
            prefix = "Проверка готового колеса\n\n"

    return (
        f"{prefix}⌚️ {(datetime.now() + timedelta(hours=3)).strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"👷 {username}\n\n"
        f"🚗 {data['marka_ts']}\n\n"
        f"🛞 {data['marka_rez']} {data['model_rez']}\n\n"
        f"{data['razmer']}/{data['radius']}\n"
        f"{data['sezon']}\n"
        f"{data['type_disk']}\n"
        f"{data['type_kolesa']}\n"
        f"\n#{data['company']}\n"
        f"\n📝 Сбор под заявку: {data['zayavka']}\n"
        f"\n#️⃣ Номер заявки: {data.get('nomer_sborka','')}\n"
        f"\n{fio}"
    )


async def cmd_sborka(st: SborkaState, user_id: int, chat_id: int, username: str, cmd: str = "sborka") -> None:
    await _ensure_refs_loaded()
    st.flows_by_user[user_id] = SborkaFlow(step="company", data={"username": username, "type_sborka": cmd, "type": "sborka"})
    await _ask(chat_id, "Компания:", _KEY_COMPANY)


def _clear(st: SborkaState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)


def _write_sborka_rows(data: dict, message_ref: str, username: str) -> None:
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
        message_ref,
        username,
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


async def _finalize(st: SborkaState, user_id: int, chat_id: int, msg: dict) -> bool:
    flow = st.flows_by_user[user_id]
    if not flow.files:
        await send_text(chat_id, "Нужно прикрепить как минимум 1 файл")
        return True

    data = flow.data
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    username = f"@{data.get('username') or user_id}"
    report = _render_report(data, fio, username)

    response = await send_message(chat_id=_company_chat(data["company"]), text=report, attachments=flow.files)
    msg_ref = ""
    if isinstance(response, dict):
        msg_ref = str(response.get("message_id") or response.get("id") or "")

    try:
        if data.get("type") != "check":
            _write_sborka_rows(data, msg_ref, username)
    except Exception:
        logger.exception("failed to write sborka rows")

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
                msg_ref,
                data["nomer_sborka"],
            )
        except Exception:
            logger.exception("failed update_record_sborka")

    _clear(st, user_id)
    await send_text(chat_id, "Ваша заявка сформирована")
    return True


async def try_handle_sborka_step(st: SborkaState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if flow is None:
        return False

    controls = _control_candidates(text, msg)
    if controls & {"выход", "sborka_exit"}:
        _clear(st, user_id)
        await send_text(chat_id, "Оформление заявки отменено")
        return True

    if controls & {"назад", "sborka_back"}:
        await send_text(chat_id, "Используйте /sborka для перезапуска анкеты")
        return True

    step = flow.step
    t = text.strip()

    if step == "company":
        if t not in _KEY_COMPANY:
            await _ask(chat_id, "Выберите компанию:", _KEY_COMPANY)
            return True
        flow.data["company"] = t
        flow.step = "type_disk"
        await _ask(chat_id, "Тип диска:", _KEY_TYPE_DISK)
        return True

    if step == "type_disk":
        if t not in _KEY_TYPE_DISK:
            await _ask(chat_id, "Выберите тип диска:", _KEY_TYPE_DISK)
            return True
        flow.data["type_disk"] = t
        flow.step = "radius"
        await _ask(chat_id, "Радиус:", _list_radius(flow.data["company"]))
        return True

    if step == "radius":
        options = _list_radius(flow.data["company"])
        if t not in options:
            await _ask(chat_id, "Выберите радиус:", options)
            return True
        flow.data["radius"] = t
        flow.step = "razmer"
        await _ask(chat_id, "Размер:", _filter_values(flow.data["company"], radius=t, field=GHRezina.RAZMER))
        return True

    if step == "razmer":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], field=GHRezina.RAZMER)
        if t not in options:
            await _ask(chat_id, "Выберите размер:", options)
            return True
        flow.data["razmer"] = t
        flow.step = "marka"
        await _ask(chat_id, "Марка резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=t, field=GHRezina.MARKA))
        return True

    if step == "marka":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], field=GHRezina.MARKA)
        if t not in options:
            await _ask(chat_id, "Выберите марку резины:", options)
            return True
        flow.data["marka_rez"] = t
        flow.step = "model"
        await _ask(chat_id, "Модель резины:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=t, field=GHRezina.MODEL))
        return True

    if step == "model":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], field=GHRezina.MODEL)
        if t not in options:
            await _ask(chat_id, "Выберите модель резины:", options)
            return True
        flow.data["model_rez"] = t
        flow.step = "sezon"
        await _ask(chat_id, "Сезон:", _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=t, field=GHRezina.SEZON))
        return True

    if step == "sezon":
        options = _filter_values(flow.data["company"], radius=flow.data["radius"], razmer=flow.data["razmer"], marka=flow.data["marka_rez"], model=flow.data["model_rez"], field=GHRezina.SEZON)
        if t not in options:
            await _ask(chat_id, "Выберите сезон:", options)
            return True
        flow.data["sezon"] = t
        flow.step = "marka_ts"
        await _ask(chat_id, "Марка авто:", _list_marka_ts(flow.data["company"])[:40])
        return True

    if step == "marka_ts":
        options = _list_marka_ts(flow.data["company"])
        if t not in options:
            await _ask(chat_id, "Выберите марку авто:", options[:40])
            return True
        flow.data["marka_ts"] = t
        flow.step = "type_kolesa"
        await _ask(chat_id, "Вид сборки:", _KEY_TYPE_SBORKA)
        return True

    if step == "type_kolesa":
        options = _KEY_TYPE_SBORKA + _KEY_SIDE
        if t not in options:
            await _ask(chat_id, "Выберите вид сборки:", options)
            return True
        flow.data["type_kolesa"] = t
        flow.step = "zayavka"
        await _ask(chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
        return True

    if step == "zayavka":
        if t not in _KEY_ZAYAVKA:
            await _ask(chat_id, "Сбор под заявку:", _KEY_ZAYAVKA)
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
            await _ask(chat_id, "Номер заявки:", sorted(set(candidates))[:50])
        else:
            await send_text(chat_id, "Номер заявки не найден. Отправьте номер вручную или текст 'не найден'.")
        return True

    if step == "nomer":
        flow.data["nomer_sborka"] = t or "не найден"
        flow.step = "files"
        await send_text_with_reply_buttons(
            chat_id,
            "Прикрепите фото/видео/файл и нажмите «Готово».",
            ["Готово", "Выход"],
            ["sborka_done", "sborka_exit"],
        )
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
            await send_text(chat_id, f"Файлов добавлено: {new_items}. Текущее количество: {len(flow.files)}")
            return True

        await send_text_with_reply_buttons(
            chat_id,
            "Прикрепите файлы и нажмите «Готово».",
            ["Готово", "Выход"],
            ["sborka_done", "sborka_exit"],
        )
        return True

    return True
