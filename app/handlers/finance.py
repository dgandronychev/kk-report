from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

from app.config import DAMAGE_CHAT_ID_BELKA, DAMAGE_CHAT_ID_CITY, DAMAGE_CHAT_ID_YANDEX
from app.utils.gsheets import write_in_answers_ras
from app.utils.helper import get_fio_async, get_open_tasks_async
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)

logger = logging.getLogger(__name__)


@dataclass
class FinanceFlow:
    kind: str = ""
    step: str = ""
    data: dict = field(default_factory=dict)
    files: List[dict] = field(default_factory=list)


@dataclass
class FinanceState:
    flows_by_user: Dict[int, FinanceFlow] = field(default_factory=dict)


_KEY_COMPANY = ["СитиДрайв", "Яндекс", "Белка"]
_KEY_CITY = ["Москва", "Санкт-Петербург"]
_KEY_PAYMENT = ["Бизнес-карта", "Наличные <> Перевод <> Личная карта"]
_KEY_PAYMENT_EXTRA = ["Подача на возмещение(свои деньги) + 6%"]


async def _ask(flow: FinanceFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls = ["Выход"] if not include_back else ["Назад", "Выход"]
    payloads = ["fin_exit"] if not include_back else ["fin_back", "fin_exit"]

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


async def _send_plain(flow: FinanceFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


def _normalize(text: str) -> str:
    return text.strip().strip("«»\"'").lower()


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
    if "fin_exit" in norms or "выход" in norms:
        return "exit"
    if "fin_back" in norms or "назад" in norms:
        return "back"
    if "fin_done" in norms or "готово" in norms:
        return "done"
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


async def cmd_parking(st: FinanceState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    st.flows_by_user[user_id] = FinanceFlow(
        kind="parking",
        step="grz_tech",
        data={"username": username, "fio": fio},
    )
    await _send_plain(st.flows_by_user[user_id], chat_id, "ГРЗ технички:")


async def _start_task_based_flow(st: FinanceState, user_id: int, chat_id: int, username: str, msg: dict, kind: str) -> None:
    fio = await get_fio_async(max_chat_id=chat_id, user_id=user_id, msg=msg)
    tasks = await get_open_tasks_async(max_chat_id=chat_id)
    if not tasks:
        await send_text(chat_id, "У вас нет активной задачи")
        return

    task_buttons = []
    for task in tasks:
        plate = str(task.get("car_plate") or "—")
        company = str(task.get("carsharing__name") or "—")
        task_buttons.append(f"{plate} | {company}")

    flow = FinanceFlow(kind=kind, step="task", data={"username": username, "fio": fio, "tasks": tasks})
    st.flows_by_user[user_id] = flow

    if len(task_buttons) == 1:
        flow.data["grz_tech"] = str(tasks[0].get("car_plate") or "—")
        flow.data["company"] = str(tasks[0].get("carsharing__name") or "—")
        if kind == "zapravka":
            flow.step = "odometer"
            await _send_plain(flow, chat_id, f"Активная задача:\nГРЗ технички: {flow.data['grz_tech']}\nКомпания: {flow.data['company']}\nУкажите показания одометра:")
            return
        flow.step = "city"
        await _ask(flow, chat_id, f"Активная задача:\nГРЗ технички: {flow.data['grz_tech']}\nКомпания: {flow.data['company']}\nВыберите город из списка или введите вручную:", _KEY_CITY, include_back=False)
        return

    await _ask(
        flow,
        chat_id,
        "У вас несколько активных задач.\nВыберите задачу:",
        task_buttons,
        include_back=False,
    )


async def cmd_zapravka(st: FinanceState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    await _start_task_based_flow(st, user_id, chat_id, username, msg, kind="zapravka")


async def cmd_expense(st: FinanceState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    await _start_task_based_flow(st, user_id, chat_id, username, msg, kind="expense")


async def _send_files_prompt(flow: FinanceFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)

    response = await send_text_with_reply_buttons(
        chat_id=chat_id,
        text=text,
        button_texts=["Готово", "Назад", "Выход"],
        button_payloads=["fin_done", "fin_back", "fin_exit"],
    )
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


async def _finish_flow(st: FinanceState, user_id: int, chat_id: int, flow: FinanceFlow) -> None:
    report = _render_report(flow)
    company = str(flow.data.get("company") or "")
    out_chat = _company_chat_id(company)
    try:
        await send_text(out_chat, report)
    except Exception:
        logger.exception("failed to send finance report to company chat")

    try:
        _write_sheet(flow)
    except Exception:
        logger.exception("failed to write finance report row")

    await send_text(chat_id, "Ваша заявка сформирована")
    st.flows_by_user.pop(user_id, None)


def _render_report(flow: FinanceFlow) -> str:
    data = flow.data
    base = "⌚️ " + (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S") + "\n\n"
    base += f"👷 @{data.get('username', '—')}\n{data.get('fio', '—')}\n\n"

    if flow.kind == "parking":
        return (
            base
            + f"#{data.get('grz_tech', '—')}\n"
            + f"{data.get('company', '—')}\n\n"
            + f"{data.get('grz_task', '—')}\n"
            + "#Парковка"
        )

    if flow.kind == "zapravka":
        return (
            base
            + f"#{data.get('grz_tech', '—')}\n"
            + f"{data.get('company', '—')}\n\n"
            + f"{data.get('odometer', '—')}\n"
            + f"{data.get('summa', '—')}\n"
            + "#Заправка"
        )

    add_sum = ""
    if data.get("payment_extra") == "Подача на возмещение(свои деньги) + 6%":
        try:
            add_sum = str(round(float(data.get("summa", 0)) / 94 * 100, 2)).replace(".", ",")
        except Exception:
            add_sum = ""

    report = (
        base
        + f"{data.get('city', '—')}\n"
        + "ШМ\n"
        + f"{data.get('summa', '—')}\n"
    )
    if add_sum:
        report += f"{add_sum}\n\n"
    report += (
        f"{data.get('company', '—')}\n"
        + f"{data.get('payment', '—')}\n"
        + f"{data.get('reason', '—')}\n\n"
        + f"#{data.get('grz_tech', '—')}\n"
        + f"{data.get('grz_task', '—')}"
    )
    return report


def _write_sheet(flow: FinanceFlow) -> None:
    data = flow.data
    now = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    if flow.kind == "parking":
        row = [
            now,
            data.get("fio", ""),
            data.get("username", ""),
            data.get("company", ""),
            data.get("grz_tech", ""),
            data.get("grz_task", ""),
            len(flow.files),
        ]
        write_in_answers_ras(row, "Городская парковка")
        return

    if flow.kind == "zapravka":
        row = [
            now,
            data.get("grz_tech", ""),
            data.get("company", ""),
            data.get("odometer", ""),
            data.get("summa", ""),
            data.get("username", ""),
            data.get("fio", ""),
            len(flow.files),
        ]
        write_in_answers_ras(row, "Заправка техничек")
        return

    add_sum = ""
    if data.get("payment_extra") == "Подача на возмещение(свои деньги) + 6%":
        try:
            add_sum = str(round(float(data.get("summa", 0)) / 94 * 100, 2)).replace(".", ",")
        except Exception:
            add_sum = ""

    row = [
        now,
        data.get("fio", ""),
        data.get("username", ""),
        data.get("city", ""),
        data.get("summa", ""),
        add_sum,
        "КлинКар Сервис",
        "ШМ",
        data.get("payment", ""),
        data.get("payment_extra", ""),
        data.get("reason", ""),
        data.get("grz_tech", ""),
        data.get("grz_task", ""),
    ]
    write_in_answers_ras(row, "Лист1")


async def try_handle_finance_step(st: FinanceState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if not flow:
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        st.flows_by_user.pop(user_id, None)
        await send_text(chat_id, "Оформление отменено")
        return True

    if flow.step == "task":
        tasks = flow.data.get("tasks") or []
        chosen = None
        for task in tasks:
            plate = str(task.get("car_plate") or "—")
            company = str(task.get("carsharing__name") or "—")
            if text.strip() == f"{plate} | {company}":
                chosen = task
                break
        if not chosen:
            await send_text(chat_id, "Выберите задачу кнопкой из списка")
            return True

        flow.data["grz_tech"] = str(chosen.get("car_plate") or "—")
        flow.data["company"] = str(chosen.get("carsharing__name") or "—")
        if flow.kind == "zapravka":
            flow.step = "odometer"
            await _send_plain(flow, chat_id, f"Активная задача:\nГРЗ технички: {flow.data['grz_tech']}\nКомпания: {flow.data['company']}\nУкажите показания одометра:")
            return True

        flow.step = "city"
        await _ask(flow, chat_id, f"Активная задача:\nГРЗ технички: {flow.data['grz_tech']}\nКомпания: {flow.data['company']}\nВыберите город из списка или введите вручную:", _KEY_CITY, include_back=False)
        return True

    if flow.kind == "parking":
        if flow.step == "grz_tech":
            flow.data["grz_tech"] = text.strip().upper()
            flow.step = "company"
            await _ask(flow, chat_id, "Компания:", _KEY_COMPANY, include_back=False)
            return True
        if flow.step == "company":
            flow.data["company"] = text.strip()
            flow.step = "grz_task"
            await _send_plain(flow, chat_id, "Введите ГРЗ задачи:")
            return True
        if flow.step == "grz_task":
            flow.data["grz_task"] = text.strip().upper()
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Добавьте скриншот из приложения парковок (от 1 до 2 файлов)")
            return True
        if flow.step == "files":
            if ctrl == "done":
                if len(flow.files) < 1:
                    await send_text(chat_id, "Нужно добавить хотя бы 1 файл")
                    return True
                await _finish_flow(st, user_id, chat_id, flow)
                return True
            atts = _extract_attachments(msg)
            if atts:
                flow.files.extend(atts[: max(0, 2 - len(flow.files))])
                await send_text(chat_id, f"Файлов добавлено: {len(flow.files)}/2")
            return True

    if flow.kind == "zapravka":
        if flow.step == "odometer":
            try:
                flow.data["odometer"] = float(text.strip().replace(",", "."))
            except Exception:
                await send_text(chat_id, "Введите значение в формате 101.11 или 101,11")
                return True
            flow.step = "summa"
            await _send_plain(flow, chat_id, "Укажите сумму заправки:")
            return True
        if flow.step == "summa":
            try:
                flow.data["summa"] = float(text.strip().replace(",", "."))
            except Exception:
                await send_text(chat_id, "Введите значение в формате 101.11 или 101,11")
                return True
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Добавьте скриншот ППР и фото приборной панели ДО и ПОСЛЕ заправки")
            return True
        if flow.step == "files":
            if ctrl == "done":
                if len(flow.files) < 3:
                    await send_text(chat_id, "Нужно минимум 3 файла")
                    return True
                await _finish_flow(st, user_id, chat_id, flow)
                return True
            atts = _extract_attachments(msg)
            if atts:
                flow.files.extend(atts[: max(0, 3 - len(flow.files))])
                await send_text(chat_id, f"Файлов добавлено: {len(flow.files)}/3")
            return True

    if flow.kind == "expense":
        if flow.step == "city":
            flow.data["city"] = text.strip()
            flow.step = "grz_task"
            await _send_plain(flow, chat_id, "Введите ГРЗ задачи:")
            return True
        if flow.step == "grz_task":
            flow.data["grz_task"] = text.strip().upper()
            flow.step = "summa"
            await _send_plain(flow, chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
            return True
        if flow.step == "summa":
            try:
                flow.data["summa"] = float(text.strip().replace(",", "."))
            except Exception:
                await send_text(chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
                return True
            flow.step = "payment"
            await _ask(flow, chat_id, "Способ оплаты:", _KEY_PAYMENT)
            return True
        if flow.step == "payment":
            if text.strip() not in _KEY_PAYMENT:
                await _ask(flow, chat_id, "Выберите способ оплаты из списка:", _KEY_PAYMENT)
                return True
            flow.data["payment"] = text.strip()
            if flow.data["payment"] == "Наличные <> Перевод <> Личная карта":
                flow.step = "payment_extra"
                await _ask(flow, chat_id, "Выберите из следующих категорий:", _KEY_PAYMENT_EXTRA)
                return True
            flow.data["payment_extra"] = ""
            flow.step = "reason"
            await _send_plain(flow, chat_id, "Укажите причину расхода")
            return True
        if flow.step == "payment_extra":
            flow.data["payment_extra"] = text.strip()
            flow.step = "reason"
            await _send_plain(flow, chat_id, "Укажите причину расхода")
            return True
        if flow.step == "reason":
            flow.data["reason"] = text.strip()
            flow.step = "files"
            await _send_files_prompt(flow, chat_id, "Загрузите фото чека/счета (от 1 до 4 файлов)")
            return True
        if flow.step == "files":
            if ctrl == "done":
                if len(flow.files) < 1:
                    await send_text(chat_id, "Нужно добавить хотя бы 1 файл")
                    return True
                await _finish_flow(st, user_id, chat_id, flow)
                return True
            atts = _extract_attachments(msg)
            if atts:
                free_slots = max(0, 4 - len(flow.files))
                flow.files.extend(atts[:free_slots])
                await send_text(chat_id, f"Файлов добавлено: {len(flow.files)}/4")
            return True

    if ctrl == "back":
        await send_text(chat_id, "Шаг «Назад» для этого сценария пока не поддержан, используйте «Выход» и начните заново")
        return True

    return True


def reset_finance_progress(st: FinanceState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)
