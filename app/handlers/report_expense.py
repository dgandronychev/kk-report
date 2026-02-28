from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import logging
from typing import Dict, List, Set

from app.config import (
    DAMAGE_CHAT_ID_BELKA,
    DAMAGE_CHAT_ID_CITY,
    DAMAGE_CHAT_ID_YANDEX,
    TELEGRAM_CHAT_ID_SBORKA_BELKA,
    TELEGRAM_CHAT_ID_SBORKA_CITY,
    TELEGRAM_CHAT_ID_SBORKA_YANDEX,
    TELEGRAM_THREAD_ID_FINANCE_EXPENSE_BELKA,
    TELEGRAM_THREAD_ID_FINANCE_EXPENSE_CITY,
    TELEGRAM_THREAD_ID_FINANCE_EXPENSE_YANDEX,
    TELEGRAM_BOT_TOKEN_FINANCE,
)
from app.utils.gsheets import load_expense_guide, write_in_answers_ras
from app.utils.helper import get_open_tasks_async
from app.utils.max_api import (
    delete_message,
    extract_message_id,
    send_message,
    send_text,
    send_text_with_reply_buttons,
)
from app.utils.telegram_api import send_report as send_telegram_report

_FINANCE_STUB_USER_IDS = {199909595}

logger = logging.getLogger(__name__)

@dataclass
class ReportExpenseFlow:
    step: str = ""
    data: dict = field(default_factory=dict)
    files: List[dict] = field(default_factory=list)
    file_keys: Set[str] = field(default_factory=set)


@dataclass
class ReportExpenseState:
    flows_by_user: Dict[int, ReportExpenseFlow] = field(default_factory=dict)


_KEY_CITY = ["Москва", "Санкт-Петербург", "Нижний Новгород", "Другое"]
_KEY_COMPANY = ["КлинКар", "КлинКар Сервис", "КлинКар Логистика"]
_KEY_PAYMENT = ["Бизнес-карта", "Наличные <> Перевод <> Личная карта", "Счёт", "Отчетные документы(УПД/акты и тд.)", "Другое"]
_KEY_PAYMENT_EXTRA = ["Подача на возмещение(свои деньги) + 6%", "Отчёт из подочётных"]


def _control(text: str, msg: dict) -> str:
    values = {text.strip().lower()}
    callback = msg.get("callback")
    if isinstance(callback, dict):
        for key in ("payload", "data", "value", "command", "action", "text"):
            val = callback.get(key)
            if isinstance(val, str):
                values.add(val.strip().lower())
        payload = callback.get("payload")
        if isinstance(payload, dict):
            for key in ("payload", "data", "value", "command", "action", "text"):
                val = payload.get(key)
                if isinstance(val, str):
                    values.add(val.strip().lower())

    if "fin_exit" in values or "выход" in values:
        return "exit"
    if "fin_back" in values or "назад" in values:
        return "back"
    if "fin_done" in values or "готово" in values:
        return "done"
    return ""


async def _ask(flow: ReportExpenseFlow, chat_id: int, text: str, options: list[str], include_back: bool = True) -> None:
    controls = ["Выход"]
    payloads = ["fin_exit"]
    if include_back:
        controls.insert(0, "Назад")
        payloads.insert(0, "fin_back")

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


async def _send_plain(flow: ReportExpenseFlow, chat_id: int, text: str) -> None:
    prev_msg_id = flow.data.get("prompt_msg_id")
    if prev_msg_id:
        await delete_message(chat_id, prev_msg_id)
    response = await send_message(chat_id=chat_id, text=text)
    msg_id = extract_message_id(response)
    if msg_id:
        flow.data["prompt_msg_id"] = msg_id


def _extract_attachments(msg: dict) -> list[dict]:
    attachments = msg.get("attachments")
    if not isinstance(attachments, list):
        body = msg.get("body")
        if isinstance(body, dict):
            attachments = body.get("attachments")
    if not isinstance(attachments, list):
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


def _add_files(flow: ReportExpenseFlow, attachments: list[dict], max_files: int) -> int:
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


def _expense_directions(flow: ReportExpenseFlow) -> list[str]:
    guide = flow.data.get("expense_guide")
    if not isinstance(guide, dict):
        return []
    return [str(key).strip() for key in guide.keys() if str(key).strip()]


def _expense_reason_options(flow: ReportExpenseFlow) -> list[str]:
    guide = flow.data.get("expense_guide")
    if not isinstance(guide, dict):
        return []
    direction = str(flow.data.get("direction") or "").strip()
    values = guide.get(direction)
    if not isinstance(values, list):
        return []
    return [str(item).strip() for item in values if str(item).strip()]


async def _ask_expense_reason(flow: ReportExpenseFlow, chat_id: int) -> None:
    options = _expense_reason_options(flow)
    if options:
        await _ask(flow, chat_id, "Укажите причину расхода", options)
        return
    await _send_plain(flow, chat_id, "Укажите причину расхода")


async def _send_files_prompt(flow: ReportExpenseFlow, chat_id: int, text: str) -> None:
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


def _company_chat_id(company: str) -> int:
    if company == "СитиДрайв":
        return int(DAMAGE_CHAT_ID_CITY)
    if company == "Яндекс":
        return int(DAMAGE_CHAT_ID_YANDEX)
    return int(DAMAGE_CHAT_ID_BELKA)


def _telegram_target_for_report_expense(company: str) -> tuple[int, int | None]:
    if company == "СитиДрайв":
        return TELEGRAM_CHAT_ID_SBORKA_CITY, (TELEGRAM_THREAD_ID_FINANCE_EXPENSE_CITY or None)
    if company == "Яндекс":
        return TELEGRAM_CHAT_ID_SBORKA_YANDEX, (TELEGRAM_THREAD_ID_FINANCE_EXPENSE_YANDEX or None)
    return TELEGRAM_CHAT_ID_SBORKA_BELKA, (TELEGRAM_THREAD_ID_FINANCE_EXPENSE_BELKA or None)


def _render_report(flow: ReportExpenseFlow) -> str:
    data = flow.data
    base = "⌚️ " + (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S") + "\n\n"
    base += f"👷 @{data.get('username', '—')}\n{data.get('fio', '—')}\n\n"

    add_sum = ""
    if data.get("payment_extra") == "Подача на возмещение(свои деньги) + 6%":
        try:
            add_sum = str(round(float(data.get("summa", 0)) / 94 * 100, 2)).replace(".", ",")
        except Exception:
            add_sum = ""

    report = (
        base
        + f"{data.get('city', '—')}\n"
        + f"{data.get('direction', 'ШМ')}\n"
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
    if data.get("payment_extra") == "Подача на возмещение(свои деньги) + 6%":
        report += "\n\n@Anastasiya_CleanCar, cогласуйте, пожалуйста"
    return report


def _write_sheet(flow: ReportExpenseFlow, report_link: str) -> None:
    data = flow.data
    now = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
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
        data.get("company", ""),
        data.get("direction", ""),
        data.get("payment", ""),
        data.get("payment_extra", ""),
        data.get("reason", ""),
        data.get("reason_description", ""),
        data.get("grz_tech", ""),
        data.get("grz_task", ""),
        data.get("invoice_org", ""),
    ]
    logger.info("[REPORT_EXPENSE->GSHEETS] sheet=%s row=%s", "Лист1", row)
    write_in_answers_ras(row, "Лист1")


async def _finish_flow(st: ReportExpenseState, user_id: int, chat_id: int, flow: ReportExpenseFlow) -> None:
    report = _render_report(flow)
    company = str(flow.data.get("company") or "")

    max_link = ""
    try:
        response = await send_text(_company_chat_id(company), report)
        message_id = extract_message_id(response)
        if message_id:
            max_link = f"max://chat/{_company_chat_id(company)}/message/{message_id}"
    except Exception:
        logger.exception("failed to send report_expense report to max")

    telegram_link = ""
    try:
        tg_chat_id, tg_thread_id = _telegram_target_for_report_expense(company)
        # telegram_link = await send_telegram_report(
        #     chat_id=tg_chat_id,
        #     thread_id=tg_thread_id,
        #     text=report,
        #     attachments=flow.files,
        #     bot_token=TELEGRAM_BOT_TOKEN_FINANCE,
        # ) or ""
    except Exception:
        logger.exception("failed to mirror report_expense to telegram")

    report_link = telegram_link or max_link
    try:
        _write_sheet(flow, report_link)
    except Exception:
        logger.exception("failed to write report_expense row")

    await send_text(chat_id, "Ваша заявка сформирована")
    st.flows_by_user.pop(user_id, None)





def _sync_task_for_company(flow: ReportExpenseFlow) -> None:
    tasks = flow.data.get("tasks") or []
    company = str(flow.data.get("company") or "").strip()
    for task in tasks:
        if str(task.get("carsharing__name") or "").strip() == company:
            flow.data["grz_tech"] = str(task.get("car_plate") or "—")
            return
    flow.data["grz_tech"] = "—"

async def cmd_report_expense(st: ReportExpenseState, user_id: int, chat_id: int, username: str, msg: dict) -> None:
    if chat_id < 0:
        await send_text(chat_id, "Эта команда доступна только в личных сообщениях с ботом")
        return

    tasks = await get_open_tasks_async(max_chat_id=chat_id)
    expense_guide: dict[str, list[str]] = {}
    try:
        expense_guide = await asyncio.to_thread(load_expense_guide)
    except Exception:
        logger.exception("failed to load expense guide from google sheets")

    if not tasks and user_id in _FINANCE_STUB_USER_IDS:
        tasks = [
            {
                "task_type": "Перегон СШМ",
                "carsharing__name": "Тестовая компания",
                "car_plate": "Т000ТТ000",
                "car_model": "TestCar",
            }
        ]

    if not tasks:
        await send_text(chat_id, "У вас нет активной задачи")
        return

    initial_company = _KEY_COMPANY[0]

    flow = ReportExpenseFlow(
        step="fio",
        data={
            "username": username,
            "tasks": tasks,
            "company": initial_company,
            "expense_guide": expense_guide,
            "grz_task": "",
        },
    )
    _sync_task_for_company(flow)
    st.flows_by_user[user_id] = flow
    await _send_plain(flow, chat_id, "Введите ФИО")


async def try_handle_report_expense_step(st: ReportExpenseState, user_id: int, chat_id: int, text: str, msg: dict) -> bool:
    flow = st.flows_by_user.get(user_id)
    if not flow:
        return False

    ctrl = _control(text, msg)
    if ctrl == "exit":
        st.flows_by_user.pop(user_id, None)
        await send_text(chat_id, "Оформление отменено")
        return True

    if flow.step == "fio":
        if ctrl == "back":
            await send_text(chat_id, "Это первый шаг сценария")
            return True
        fio = text.strip()
        if not fio:
            await _send_plain(flow, chat_id, "Введите ФИО")
            return True
        flow.data["fio"] = fio
        flow.step = "city"
        await _ask(flow, chat_id, "Укажите Ваш город", _KEY_CITY)
        return True

    if flow.step == "city":
        if ctrl == "back":
            flow.step = "fio"
            await _send_plain(flow, chat_id, "Введите ФИО")
            return True
        if text.strip() == "Другое":
            flow.step = "city_custom"
            await _send_plain(flow, chat_id, "Введите название Вашего города")
            return True
        flow.data["city"] = text.strip()
        directions = _expense_directions(flow)
        flow.step = "direction"
        if directions:
            await _ask(flow, chat_id, "Укажите направление, для которого производится расход", directions)
            return True
        await _send_plain(flow, chat_id, "Введите направление")
        return True

    if flow.step == "city_custom":
        if ctrl == "back":
            flow.step = "city"
            await _ask(flow, chat_id, "Укажите Ваш город", _KEY_CITY)
            return True
        flow.data["city"] = text.strip()
        directions = _expense_directions(flow)
        flow.step = "direction"
        if directions:
            await _ask(flow, chat_id, "Укажите направление, для которого производится расход", directions)
            return True
        await _send_plain(flow, chat_id, "Введите направление")
        return True

    if flow.step == "direction":
        if ctrl == "back":
            flow.step = "city"
            await _ask(flow, chat_id, "Укажите Ваш город", _KEY_CITY)
            return True
        if text.strip() == "Другое":
            flow.step = "direction_custom"
            await _send_plain(flow, chat_id, "Введите направление")
            return True
        flow.data["direction"] = text.strip()
        flow.step = "summa"
        await _send_plain(flow, chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
        return True

    if flow.step == "direction_custom":
        if ctrl == "back":
            directions = _expense_directions(flow)
            flow.step = "direction"
            if directions:
                await _ask(flow, chat_id, "Укажите направление, для которого производится расход", directions)
                return True
            await _send_plain(flow, chat_id, "Введите направление")
            return True
        flow.data["direction"] = text.strip()
        flow.step = "summa"
        await _send_plain(flow, chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
        return True

    if flow.step == "summa":
        if ctrl == "back":
            directions = _expense_directions(flow)
            flow.step = "direction"
            if directions:
                await _ask(flow, chat_id, "Укажите направление, для которого производится расход", directions)
                return True
            await _send_plain(flow, chat_id, "Введите направление")
            return True
        raw_sum = text.strip()
        if "," in raw_sum:
            await send_text(chat_id, "Используйте точку как разделитель, пример: 5678.91")
            return True
        try:
            flow.data["summa"] = float(raw_sum)
        except Exception:
            await send_text(chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
            return True
        flow.step = "org"
        await _ask(flow, chat_id, "Укажите компанию, с которой произведен расход", _KEY_COMPANY)
        return True

    if flow.step == "org":
        if ctrl == "back":
            flow.step = "summa"
            await _send_plain(flow, chat_id, "Введите сумму с 2 знаками после точки, пример: 5678.91")
            return True
        company = text.strip()
        options = _KEY_COMPANY
        if company not in options:
            await _ask(flow, chat_id, "Вы ввели компанию не из предложенного списка.\nУкажите компанию, с которой произведен расход", options)
            return True
        flow.data["company"] = company
        _sync_task_for_company(flow)
        flow.step = "payment"
        await _ask(flow, chat_id, "Способ оплаты:", _KEY_PAYMENT)
        return True

    if flow.step == "payment":
        if ctrl == "back":
            flow.step = "org"
            await _ask(flow, chat_id, "Укажите компанию, с которой произведен расход", _KEY_COMPANY)
            return True
        if text.strip() == "Другое":
            flow.step = "payment_custom"
            await _send_plain(flow, chat_id, "Укажите способ оплаты в произвольной форме")
            return True
        flow.data["payment"] = text.strip()
        if flow.data["payment"] == "Наличные <> Перевод <> Личная карта":
            flow.step = "payment_extra"
            await _ask(flow, chat_id, "Выберите из следующих категорий:", _KEY_PAYMENT_EXTRA)
            return True
        flow.data["payment_extra"] = ""
        flow.step = "reason"
        await _ask_expense_reason(flow, chat_id)
        return True

    if flow.step == "payment_custom":
        if ctrl == "back":
            flow.step = "payment"
            await _ask(flow, chat_id, "Способ оплаты:", _KEY_PAYMENT)
            return True
        flow.data["payment"] = text.strip()
        flow.data["payment_extra"] = ""
        flow.step = "reason"
        await _ask_expense_reason(flow, chat_id)
        return True

    if flow.step == "payment_extra":
        if ctrl == "back":
            flow.step = "payment"
            await _ask(flow, chat_id, "Способ оплаты:", _KEY_PAYMENT)
            return True
        if text.strip() not in _KEY_PAYMENT_EXTRA:
            await _ask(flow, chat_id, "Выберите из следующих категорий:", _KEY_PAYMENT_EXTRA)
            return True
        flow.data["payment_extra"] = text.strip()
        flow.step = "reason"
        await _ask_expense_reason(flow, chat_id)
        return True

    if flow.step == "reason":
        if ctrl == "back":
            if flow.data.get("payment") == "Наличные <> Перевод <> Личная карта":
                flow.step = "payment_extra"
                await _ask(flow, chat_id, "Выберите из следующих категорий:", _KEY_PAYMENT_EXTRA)
            else:
                flow.step = "payment"
                await _ask(flow, chat_id, "Способ оплаты:", _KEY_PAYMENT)
            return True
        flow.data["reason"] = text.strip()
        flow.step = "reason_description"
        await _send_plain(flow, chat_id, "Описание причины расхода")
        return True

    if flow.step == "reason_description":
        if ctrl == "back":
            flow.step = "reason"
            await _ask_expense_reason(flow, chat_id)
            return True
        flow.data["reason_description"] = text.strip()
        payment = str(flow.data.get("payment") or "")
        if payment in {"Счёт", "Отчетные документы(УПД/акты и тд.)", "Другое"}:
            flow.step = "invoice_org"
            await _send_plain(flow, chat_id, "Укажите наименование организации")
            return True
        flow.step = "files"
        await _send_files_prompt(flow, chat_id, "Загрузите фото чека/счета (от 1 до 4 файлов)")
        return True

    if flow.step == "invoice_org":
        if ctrl == "back":
            flow.step = "reason_description"
            await _send_plain(flow, chat_id, "Описание причины расхода")
            return True
        flow.data["invoice_org"] = text.strip()
        flow.step = "files"
        await _send_files_prompt(flow, chat_id, "Загрузите фото чека/счета (от 1 до 4 файлов)")
        return True

    if flow.step == "files":
        if ctrl == "back":
            payment = str(flow.data.get("payment") or "")
            if payment in {"Счёт", "Отчетные документы(УПД/акты и тд.)", "Другое"}:
                flow.step = "invoice_org"
                await _send_plain(flow, chat_id, "Укажите наименование организации")
                return True
            flow.step = "reason_description"
            await _send_plain(flow, chat_id, "Описание причины расхода")
            return True
        if ctrl == "done":
            if len(flow.files) < 1:
                await send_text(chat_id, "Нужно добавить хотя бы 1 файл")
                return True
            await _finish_flow(st, user_id, chat_id, flow)
            return True
        attachments = _extract_attachments(msg)
        if not attachments:
            await _send_files_prompt(flow, chat_id, "Загрузите фото чека/счета (от 1 до 4 файлов)")
            return True
        added = _add_files(flow, attachments, max_files=4)
        await _send_files_prompt(flow, chat_id, f"Файлов добавлено: {added}. Текущее количество: {len(flow.files)}/4")
        return True

    if ctrl == "back":
        await send_text(chat_id, "Это первый шаг сценария")
        return True

    return True


def reset_report_expense_progress(st: ReportExpenseState, user_id: int) -> None:
    st.flows_by_user.pop(user_id, None)
