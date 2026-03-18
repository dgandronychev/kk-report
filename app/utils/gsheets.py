from __future__ import annotations

import logging
import re
import time as time_module
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, TypeVar

import gspread
from gspread import Client
from gspread.exceptions import APIError
from gspread.models import Spreadsheet, Worksheet

from app.config import (
    GOOGLE_SHEETS_SHIFT,
    GSPREAD_URL_ANSWER,
    GSPREAD_URL_GATES,
    GSPREAD_URL_INFO_FINANCE,
    GSPREAD_URL_MAIN,
    GSPREAD_URL_SKLAD,
    URL_GOOGLE_SHEETS_CHART,
    URL_GOOGLE_SHEETS_LOC_SHM,
    URL_GOOGLE_SHEETS_ORDER,
    URL_GOOGLE_SHEETS_SKLAD,
)

T = TypeVar("T")
EXCEL_EPOCH = datetime(1899, 12, 30)
CREDS_PATH = "app/creds.json"
MSK_OFFSET_HOURS = 3

logger = logging.getLogger(__name__)

_CLIENT_CACHE: Optional[Client] = None
_SPREADSHEET_CACHE: dict[str, Spreadsheet] = {}
_MOVE_REF_CACHE: Optional[dict[str, list[list[str]]]] = None
_MOVE_CAR_CACHE: Optional[dict[str, list[list[str]]]] = None
_MOVE_XAB_CACHE: Optional[dict[tuple[str, str, str, str, str, str, str], list[list[str]]]] = None

_MOVE_REZ_RADIUS = 1
_MOVE_REZ_RAZMER = 2
_MOVE_REZ_SEZON = 3
_MOVE_REZ_MARKA = 4
_MOVE_REZ_MODEL = 5

_MOVE_CAR_MARKA_CITY = 2
_MOVE_CAR_MARKA_YANDEX = 3
_MOVE_CAR_MARKA_BELKA = 1


@dataclass
class WarehouseRequestRow:
    row_index: int
    request_number: str
    material_name: str
    quantity: int
    status: str


def _now_msk() -> datetime:
    return datetime.now() + timedelta(hours=MSK_OFFSET_HOURS)


def _log_sheet_write(action: str, sheet: str, payload: Any) -> None:
    logger.info("[GSHEETS] %s | sheet=%s | payload=%s", action, sheet, payload)


def reset_gsheets_connection_cache() -> None:
    global _CLIENT_CACHE
    _CLIENT_CACHE = None
    _SPREADSHEET_CACHE.clear()


def _gc(force_refresh: bool = False) -> Client:
    global _CLIENT_CACHE
    if force_refresh or _CLIENT_CACHE is None:
        _CLIENT_CACHE = gspread.service_account(CREDS_PATH)
    return _CLIENT_CACHE


def _open(url: str, force_refresh: bool = False) -> Spreadsheet:
    if force_refresh or url not in _SPREADSHEET_CACHE:
        _SPREADSHEET_CACHE[url] = _gc(force_refresh=force_refresh).open_by_url(url)
    return _SPREADSHEET_CACHE[url]


def _worksheet(url: str, title: str, force_refresh: bool = False) -> Worksheet:
    return _open(url, force_refresh=force_refresh).worksheet(title)


def _with_retries(
    func: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    action_name: str = "gsheets_call",
) -> T:
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except APIError as exc:
            last_error = exc
            reset_gsheets_connection_cache()
            if attempt == max_attempts:
                raise
            logger.warning("%s retry %s/%s after APIError: %s", action_name, attempt, max_attempts, exc)
            time_module.sleep(base_delay * attempt)
        except Exception as exc:
            last_error = exc
            if attempt == max_attempts:
                raise
            logger.warning("%s retry %s/%s after error: %s", action_name, attempt, max_attempts, exc)
            time_module.sleep(base_delay * attempt)
    assert last_error is not None
    raise last_error


def _append_row(
    url: str,
    sheet_name: str,
    row: list[Any],
    *,
    value_input_option: str = "USER_ENTERED",
    table_range: Optional[str] = None,
    insert_data_option: Optional[str] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    def _op() -> None:
        ws = _worksheet(url, sheet_name)
        _log_sheet_write("append_row", sheet_name, row)
        kwargs: dict[str, Any] = {"value_input_option": value_input_option}
        if table_range is not None:
            kwargs["table_range"] = table_range
        if insert_data_option is not None:
            kwargs["insert_data_option"] = insert_data_option
        ws.append_row(row, **kwargs)

    _with_retries(
        _op,
        max_attempts=max_attempts,
        base_delay=base_delay,
        action_name=f"append_row:{sheet_name}",
    )


def _append_rows(
    url: str,
    sheet_name: str,
    rows: list[list[Any]],
    *,
    value_input_option: str = "USER_ENTERED",
    table_range: Optional[str] = None,
    insert_data_option: Optional[str] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    def _op() -> None:
        ws = _worksheet(url, sheet_name)
        _log_sheet_write("append_rows", sheet_name, rows)
        kwargs: dict[str, Any] = {"value_input_option": value_input_option}
        if table_range is not None:
            kwargs["table_range"] = table_range
        if insert_data_option is not None:
            kwargs["insert_data_option"] = insert_data_option
        ws.append_rows(rows, **kwargs)

    _with_retries(
        _op,
        max_attempts=max_attempts,
        base_delay=base_delay,
        action_name=f"append_rows:{sheet_name}",
    )


def _batch_update(
    url: str,
    sheet_name: str,
    updates: list[dict[str, Any]],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    def _op() -> None:
        ws = _worksheet(url, sheet_name)
        _log_sheet_write("batch_update", sheet_name, updates)
        ws.batch_update(updates)

    _with_retries(
        _op,
        max_attempts=max_attempts,
        base_delay=base_delay,
        action_name=f"batch_update:{sheet_name}",
    )


def _values_batch_update(
    url: str,
    body: dict[str, Any],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    def _op() -> None:
        sh = _open(url)
        _log_sheet_write("values_batch_update", url, body)
        sh.values_batch_update(body)

    _with_retries(
        _op,
        max_attempts=max_attempts,
        base_delay=base_delay,
        action_name="values_batch_update",
    )


def _require_url(url: str, setting_name: str) -> str:
    if not url:
        raise RuntimeError(f"Не настроен URL Google Sheets: {setting_name}")
    return url


def _parse_dt(val: Any, fallback_date: Optional[Any] = None) -> Optional[datetime]:
    if val is None:
        return None

    if isinstance(val, (int, float)):
        try:
            return EXCEL_EPOCH + timedelta(days=float(val))
        except Exception:
            return None

    text = str(val).strip()
    if not text:
        return None

    formats = (
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%d.%m.%Y",
        "%H:%M:%S",
        "%H:%M",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if fmt in ("%H:%M:%S", "%H:%M"):
                if fallback_date:
                    fallback_dt = _parse_dt(fallback_date)
                    if fallback_dt:
                        return fallback_dt.replace(hour=dt.hour, minute=dt.minute, second=dt.second)
                return None
            return dt
        except Exception:
            continue
    return None


def _sheet_values(url: str, title: str, *, skip_header: bool = True) -> list[list[str]]:
    rows = _worksheet(url, title).get_all_values()
    if skip_header:
        return rows[1:]
    return rows


def _open_sklad_sheet() -> Spreadsheet:
    return _open(GSPREAD_URL_SKLAD)


def loading_bz_znaniya(company: str) -> list[list[str]]:
    return _sheet_values(GSPREAD_URL_MAIN, company)


def load_expense_guide() -> dict[str, list[str]]:
    rows = _worksheet(GSPREAD_URL_INFO_FINANCE, "Справочник").get_all_values()
    if len(rows) < 2:
        return {}

    headers = rows[0]
    body = rows[1:]
    result: dict[str, list[str]] = {}

    for index, header in enumerate(headers):
        name = str(header).strip()
        if not name:
            continue
        values: list[str] = []
        for row in body:
            if index >= len(row):
                continue
            value = str(row[index]).strip()
            if value:
                values.append(value)
        result[name] = values
    return result


async def load_nomenclature_reference_data() -> dict[str, list[list[str]]]:
    return {
        "city": _sheet_values(GSPREAD_URL_MAIN, "Резина Сити"),
        "yandex": _sheet_values(GSPREAD_URL_MAIN, "Резина ЯД"),
        "belka": _sheet_values(GSPREAD_URL_MAIN, "Резина Белка"),
    }


async def load_sborka_reference_data() -> dict[str, dict[str, list[list[str]]]]:
    return {
        "rezina": {
            "city": _sheet_values(GSPREAD_URL_MAIN, "Резина Сити"),
            "yandex": _sheet_values(GSPREAD_URL_MAIN, "Резина ЯД"),
            "belka": _sheet_values(GSPREAD_URL_MAIN, "Резина Белка"),
        },
        "cars": {
            "city": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Сити"),
            "yandex": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Яд"),
            "belka": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Белка"),
        },
    }


async def load_damage_reference_data() -> dict[str, dict[str, list[list[str]]]]:
    return {
        "rezina": {
            "city": _sheet_values(GSPREAD_URL_MAIN, "Резина Сити"),
            "yandex": _sheet_values(GSPREAD_URL_MAIN, "Резина ЯД"),
            "belka": _sheet_values(GSPREAD_URL_MAIN, "Резина Белка"),
        },
        "cars": {
            "city": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Сити"),
            "yandex": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Яд"),
            "belka": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Белка"),
        },
    }


def find_logistics_rows_shift(now: Optional[datetime] = None) -> list[tuple[str, str, str]]:
    current = _now_msk() if now is None else now
    rows = _worksheet(URL_GOOGLE_SHEETS_CHART, "Логисты выход на смену").get_all_records()

    result: list[tuple[str, str, str]] = []
    for row in rows:
        fio = str(row.get("ФИО", "")).strip()
        tag = str(row.get("Тег", "")).strip()
        direction = str(row.get("Направление", "")).strip()
        start_raw = row.get("Время начала смены")
        end_raw = row.get("Время конца смены")
        date_raw = row.get("Дата")

        if isinstance(end_raw, str) and end_raw.strip():
            continue
        if end_raw and not isinstance(end_raw, str):
            continue

        start_dt = _parse_dt(start_raw, fallback_date=date_raw)
        if not start_dt:
            continue

        delta_seconds = (current - start_dt).total_seconds()
        if 0 <= delta_seconds <= 3 * 3600 and fio and tag and direction:
            result.append((direction, fio, tag))
    return result


def write_in_answers_ras_shift(
    tlist: list[Any],
    page: str = "Лист1",
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> Optional[str]:
    duration: Optional[str] = None
    dt_end = _parse_dt(tlist[0]) if tlist else None
    fio = str(tlist[1]).strip() if len(tlist) > 1 else ""
    action = str(tlist[2]).strip() if len(tlist) > 2 else ""

    def _op() -> Optional[str]:
        nonlocal duration
        ws = _worksheet(GOOGLE_SHEETS_SHIFT, page)

        if action == "Окончание смены" and dt_end and fio:
            all_rows = ws.get_all_values()
            for row in reversed(all_rows):
                if len(row) < 3:
                    continue
                if str(row[1]).strip() != fio or str(row[2]).strip() != "Начало смены":
                    continue
                start_dt = _parse_dt(str(row[0]).lstrip("'"))
                if not start_dt:
                    continue
                delta = dt_end - start_dt if dt_end >= start_dt else dt_end + timedelta(days=1) - start_dt
                if delta < timedelta(hours=24):
                    total_seconds = int(delta.total_seconds())
                    hours, rem = divmod(total_seconds, 3600)
                    minutes, seconds = divmod(rem, 60)
                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    duration = "Нет данных"
                break

        row_to_append = list(tlist)
        if len(row_to_append) < 6:
            row_to_append.extend([""] * (6 - len(row_to_append)))
        row_to_append[5] = duration or "Нет данных"

        _log_sheet_write("append_row", page, row_to_append)
        ws.append_row(row_to_append, value_input_option="RAW")
        return duration

    return _with_retries(
        _op,
        max_attempts=max_attempts,
        base_delay=base_delay,
        action_name=f"write_in_answers_ras_shift:{page}",
    )


def get_max_nomer_sborka() -> int:
    rows = _sheet_values(GSPREAD_URL_SKLAD, "Заявка на сборку")
    numbers: list[int] = []
    for row in rows:
        if len(row) > 13 and row[13]:
            digits = re.sub(r"[^0-9]", "", str(row[13]))
            if digits:
                numbers.append(int(digits))
    return max(numbers) if numbers else 0


def get_number_util(company: str, column_index: int) -> str:
    rows = _sheet_values(GSPREAD_URL_ANSWER, "Выгрузка ремонты/утиль")
    values = [str(row[column_index]).strip() for row in rows if len(row) > column_index and row[column_index]]

    if company == "СитиДрайв":
        prefix = "su"
    elif company == "Яндекс":
        prefix = "yu"
    else:
        prefix = "blk"

    numbers: list[int] = []
    for value in values:
        if not value.startswith(prefix):
            continue
        digits = re.sub(r"[^0-9]", "", value)
        if digits:
            numbers.append(int(digits))
    next_number = max(numbers) + 1 if numbers else 1
    return f"{prefix}{next_number}"


def write_soberi_in_google_sheets_rows(rows: list[list[str]]) -> None:
    _append_rows(
        GSPREAD_URL_SKLAD,
        "Заявка на сборку",
        rows,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )


def write_soberi_in_google_sheets(tlist: list[str]) -> None:
    _append_row(
        GSPREAD_URL_SKLAD,
        "Заявка на сборку",
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )


def get_record_sklad() -> list[list[str]]:
    rows = _sheet_values(GSPREAD_URL_SKLAD, "Заявка на сборку")
    result: list[list[str]] = []
    for row in rows:
        if len(row) > 15 and not row[15] and len(row) > 2 and row[2]:
            result.append(row[3:14])
    return result


def nomer_sborka(
    company: str,
    radius: str,
    razmer: str,
    marka_rez: str,
    model_rez: str,
    sezon: str,
    marka_ts: str,
    type_disk: str,
    type_kolesa: str,
) -> list[str]:
    rows = _worksheet(GSPREAD_URL_SKLAD, "Заявка на сборку").get_all_values()
    unique_numbers: set[str] = set()

    for row in rows:
        if len(row) < 14:
            continue
        exact_match = (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[7]) == str(marka_rez)
            and str(row[8]) == str(model_rez)
            and str(row[9]) == str(sezon)
            and str(row[10]) == str(type_disk)
            and str(row[11]) == str(type_kolesa)
        )
        fallback_match = (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[9]) == str(sezon)
            and str(row[11]) == str(type_kolesa)
        )
        if exact_match or fallback_match:
            unique_numbers.add(str(row[13]))
    return list(unique_numbers)


def nomer_sborka_ko(
    company: str,
    radius: str,
    razmer: str,
    marka_rez: str,
    model_rez: str,
    sezon: str,
    marka_ts: str,
    type_disk: str,
    type_kolesa: str,
) -> list[str]:
    rows = _worksheet(GSPREAD_URL_SKLAD, "Заявка на сборку").get_all_values()
    matching_rows: list[list[str]] = []

    for row in rows:
        if len(row) < 17:
            continue
        if (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[7]) == str(marka_rez)
            and str(row[8]) == str(model_rez)
            and str(row[9]) == str(sezon)
            and str(row[10]) == str(type_disk)
            and str(row[16]) == ""
        ):
            matching_rows.append(row)

    if not matching_rows:
        for row in rows:
            if len(row) < 17:
                continue
            if (
                str(row[3]) == str(company)
                and str(row[4]) == str(marka_ts)
                and str(row[5]) == str(radius)
                and str(row[6]) == str(razmer)
                and str(row[9]) == str(sezon)
                and str(row[16]) == ""
            ):
                matching_rows.append(row)

    groups: dict[str, list[list[str]]] = {}
    for row in matching_rows:
        groups.setdefault(str(row[13]), []).append(row)

    komplekts = [key for key, group_rows in groups.items() if len(group_rows) == 4]
    axes: list[str] = []
    if len(matching_rows) >= 2:
        sides = {str(row[11]) for row in matching_rows}
        if "Левое" in sides and "Правое" in sides:
            axes.append(str(matching_rows[0][13]))

    if type_kolesa == "Ось":
        return axes
    if type_kolesa == "Комплект":
        return komplekts
    return []


def update_data_sborka(marka_rez: str, model_rez: str, type_disk: str, type_kolesa: str, nomer_sborka_value: str) -> None:
    rows = _worksheet(GSPREAD_URL_SKLAD, "Заявка на сборку").get_all_values()
    for index, row in enumerate(rows, start=1):
        if len(row) < 14:
            continue
        if str(row[13]) == str(nomer_sborka_value) and str(row[11]) == str(type_kolesa):
            updates = [
                {"range": f"K{index}:K{index}", "values": [[str(type_disk)]]},
                {"range": f"H{index}:I{index}", "values": [[str(marka_rez), str(model_rez)]]},
            ]
            _batch_update(GSPREAD_URL_SKLAD, "Заявка на сборку", updates)
            return


def update_record_sborka(
    company: str,
    username: str,
    radius: str,
    razmer: str,
    marka_rez: str,
    model_rez: str,
    sezon: str,
    marka_ts: str,
    type_disk: str,
    type_kolesa: str,
    message_link: str,
    nomer_sborka_value: str,
) -> None:
    rows = _worksheet(GSPREAD_URL_SKLAD, "Заявка на сборку").get_all_values()
    current_time = _now_msk().strftime("%d.%m.%Y %H:%M:%S")
    matches: list[tuple[int, list[str]]] = []

    for row_index, row in enumerate(rows):
        if len(row) < 17:
            continue
        exact_match = (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[7]) == str(marka_rez)
            and str(row[8]) == str(model_rez)
            and str(row[9]) == str(sezon)
            and str(row[10]) == str(type_disk)
            and str(row[13]) == str(nomer_sborka_value)
            and str(row[16]) == ""
        )
        fallback_match = (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[9]) == str(sezon)
            and str(row[13]) == str(nomer_sborka_value)
            and str(row[16]) == ""
        )
        if exact_match or fallback_match:
            matches.append((row_index, row))

    updates: list[dict[str, Any]] = []
    if type_kolesa not in ("Комплект", "Ось"):
        for row_index, row in matches:
            if str(row[11]) == str(type_kolesa):
                updates.append(
                    {
                        "range": f"P{row_index + 1}:S{row_index + 1}",
                        "values": [[current_time, "Собрано", str(message_link), str(username)]],
                    }
                )
                break
    else:
        required = 2 if type_kolesa == "Комплект" else 1
        sides_updated = {"Правое": 0, "Левое": 0}
        for row_index, row in matches:
            side = str(row[11])
            if side in sides_updated and sides_updated[side] < required:
                updates.append(
                    {
                        "range": f"P{row_index + 1}:S{row_index + 1}",
                        "values": [[current_time, "Собрано", str(message_link), str(username)]],
                    }
                )
                sides_updated[side] += 1

    if updates:
        _batch_update(GSPREAD_URL_SKLAD, "Заявка на сборку", updates)


def write_in_answers_ras(
    tlist: list[Any],
    name_sheet: str,
    URL: str = GSPREAD_URL_ANSWER,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    _append_row(
        URL,
        name_sheet,
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
        max_attempts=max_attempts,
        base_delay=base_delay,
    )


def write_in_answers_ras_nomen(
    tlist: list[Any],
    name_sheet: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    _append_row(
        GSPREAD_URL_MAIN,
        name_sheet,
        tlist,
        value_input_option="USER_ENTERED",
        max_attempts=max_attempts,
        base_delay=base_delay,
    )


def write_open_gate_row(fio: str, car_plate: str, company: str) -> None:
    now_msk = _now_msk()
    payload = [
        now_msk.strftime("%d.%m.%Y"),
        now_msk.strftime("%H:%M:%S"),
        fio,
        car_plate,
        company,
        "",
    ]
    _append_row(
        GSPREAD_URL_GATES,
        "Выгрузка Техники",
        payload,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )


def find_logistics_rows(limit_hours: int = 12) -> tuple[list[str], list[str]]:
    rows = _worksheet(URL_GOOGLE_SHEETS_CHART, "Логисты выход на смену").get_all_values()
    if not rows:
        return [], []

    header, data = rows[0], rows[1:]

    def col_idx(name: str) -> int:
        lowered = [str(item).strip().lower() for item in header]
        try:
            return lowered.index(name.lower())
        except ValueError:
            return -1

    i_fio = col_idx("ФИО")
    i_tag = col_idx("Тег")
    i_dir = col_idx("Направление")
    i_start = col_idx("Время начала смены")
    i_end = col_idx("Время конца смены")
    if min(i_fio, i_tag, i_dir, i_start, i_end) < 0:
        return [], []

    now_msk = _now_msk()
    window = timedelta(hours=limit_hours)
    tags: list[str] = []
    fios: list[str] = []

    for row in data:
        if max(i_fio, i_tag, i_dir, i_start, i_end) >= len(row):
            continue
        if str(row[i_dir]).strip() != "ВШМ":
            continue
        if str(row[i_end]).strip():
            continue

        start_dt = _parse_dt(row[i_start], fallback_date=row[i_start]) or _parse_dt(row[i_start])
        if not start_dt:
            continue
        if timedelta(0) <= (now_msk - start_dt) <= window:
            tag = str(row[i_tag]).strip()
            fio = str(row[i_fio]).strip()
            if tag and fio:
                tags.append(tag)
                fios.append(fio)
    return tags, fios


def load_tech_plates() -> list[str]:
    rows = _sheet_values(GSPREAD_URL_MAIN, "Наши технички")
    plates = {str(row[1]).strip().upper() for row in rows if len(row) >= 2 and str(row[1]).strip()}
    return sorted(plates)


def load_parking_task_grz_by_company() -> dict[str, list[str]]:
    company_sheets = {
        "СитиДрайв": ("Перечень ТС Сити", 0),
        "Яндекс": ("Перечень ТС Яд", 0),
        "Белка": ("Перечень ТС Белка", 2),
    }
    result: dict[str, list[str]] = {}
    for company, (sheet_name, column_index) in company_sheets.items():
        rows = _sheet_values(GSPREAD_URL_MAIN, sheet_name)
        values = {
            str(row[column_index]).strip()
            for row in rows
            if len(row) > column_index and str(row[column_index]).strip()
        }
        result[company] = sorted(values)
    return result


def _move_company_key(company: str) -> str:
    company = str(company).strip()
    if company == "СитиДрайв":
        return "city"
    if company == "Яндекс":
        return "yandex"
    return "belka"


def _move_car_marka_index(company: str) -> int:
    if str(company).strip() == "СитиДрайв":
        return _MOVE_CAR_MARKA_CITY
    if str(company).strip() == "Яндекс":
        return _MOVE_CAR_MARKA_YANDEX
    return _MOVE_CAR_MARKA_BELKA


def load_move_reference_cache(force_reload: bool = False) -> dict[str, list[list[str]]]:
    global _MOVE_REF_CACHE
    if _MOVE_REF_CACHE and not force_reload:
        return _MOVE_REF_CACHE
    _MOVE_REF_CACHE = {
        "city": _sheet_values(GSPREAD_URL_MAIN, "Резина Сити"),
        "yandex": _sheet_values(GSPREAD_URL_MAIN, "Резина ЯД"),
        "belka": _sheet_values(GSPREAD_URL_MAIN, "Резина Белка"),
    }
    return _MOVE_REF_CACHE


def load_move_car_cache(force_reload: bool = False) -> dict[str, list[list[str]]]:
    global _MOVE_CAR_CACHE
    if _MOVE_CAR_CACHE and not force_reload:
        return _MOVE_CAR_CACHE
    _MOVE_CAR_CACHE = {
        "city": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Сити"),
        "yandex": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Яд"),
        "belka": _sheet_values(GSPREAD_URL_MAIN, "Перечень ТС Белка"),
    }
    return _MOVE_CAR_CACHE


def _ensure_move_reference_rows(company: str, force_reload: bool = False) -> list[list[str]]:
    rows = load_move_reference_cache(force_reload=force_reload).get(_move_company_key(company), [])
    if not rows and not force_reload:
        rows = load_move_reference_cache(force_reload=True).get(_move_company_key(company), [])
    return rows


def _ensure_move_car_rows(company: str, force_reload: bool = False) -> list[list[str]]:
    rows = load_move_car_cache(force_reload=force_reload).get(_move_company_key(company), [])
    if not rows and not force_reload:
        rows = load_move_car_cache(force_reload=True).get(_move_company_key(company), [])
    return rows


def _move_filtered_rows(
    company: str,
    radius: Optional[str] = None,
    razmer: Optional[str] = None,
    marka_rez: Optional[str] = None,
    model_rez: Optional[str] = None,
    force_reload: bool = False,
) -> list[list[str]]:
    rows = _ensure_move_reference_rows(company, force_reload=force_reload)
    result: list[list[str]] = []
    for row in rows:
        if len(row) <= _MOVE_REZ_MODEL:
            continue
        if radius is not None and str(row[_MOVE_REZ_RADIUS]).strip() != str(radius).strip():
            continue
        if razmer is not None and str(row[_MOVE_REZ_RAZMER]).strip() != str(razmer).strip():
            continue
        if marka_rez is not None and str(row[_MOVE_REZ_MARKA]).strip() != str(marka_rez).strip():
            continue
        if model_rez is not None and str(row[_MOVE_REZ_MODEL]).strip() != str(model_rez).strip():
            continue
        result.append(row)
    return result


def _unique_sorted(values: list[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def get_move_marka_ts_options(company: str, force_reload: bool = False) -> list[str]:
    rows = _ensure_move_car_rows(company, force_reload=force_reload)
    index = _move_car_marka_index(company)
    return _unique_sorted([row[index] for row in rows if len(row) > index])


def get_move_radius_options(company: str, marka_ts: str = "", force_reload: bool = False) -> list[str]:
    del marka_ts
    rows = _move_filtered_rows(company, force_reload=force_reload)
    values = _unique_sorted([row[_MOVE_REZ_RADIUS] for row in rows if len(row) > _MOVE_REZ_RADIUS])

    def sort_key(value: str) -> tuple[int, Any]:
        stripped = str(value).strip()
        return (0, int(stripped)) if stripped.isdigit() else (1, stripped)

    return sorted(values, key=sort_key)


def get_move_razmer_options(company: str, marka_ts: str, radius: str, force_reload: bool = False) -> list[str]:
    del marka_ts
    rows = _move_filtered_rows(company, radius=radius, force_reload=force_reload)
    return _unique_sorted([row[_MOVE_REZ_RAZMER] for row in rows if len(row) > _MOVE_REZ_RAZMER])


def get_move_marka_options(company: str, marka_ts: str, radius: str, razmer: str, force_reload: bool = False) -> list[str]:
    del marka_ts
    rows = _move_filtered_rows(company, radius=radius, razmer=razmer, force_reload=force_reload)
    return _unique_sorted([row[_MOVE_REZ_MARKA] for row in rows if len(row) > _MOVE_REZ_MARKA])


def get_move_model_options(
    company: str,
    marka_ts: str,
    radius: str,
    razmer: str,
    marka_rez: str,
    force_reload: bool = False,
) -> list[str]:
    del marka_ts
    rows = _move_filtered_rows(
        company,
        radius=radius,
        razmer=razmer,
        marka_rez=marka_rez,
        force_reload=force_reload,
    )
    return _unique_sorted([row[_MOVE_REZ_MODEL] for row in rows if len(row) > _MOVE_REZ_MODEL])


def get_move_sezon_options(
    company: str,
    marka_ts: str,
    radius: str,
    razmer: str,
    marka_rez: str,
    model_rez: str,
    force_reload: bool = False,
) -> list[str]:
    del marka_ts
    rows = _move_filtered_rows(
        company,
        radius=radius,
        razmer=razmer,
        marka_rez=marka_rez,
        model_rez=model_rez,
        force_reload=force_reload,
    )
    return _unique_sorted([row[_MOVE_REZ_SEZON] for row in rows if len(row) > _MOVE_REZ_SEZON])


def reset_move_reference_cache() -> None:
    global _MOVE_REF_CACHE, _MOVE_CAR_CACHE
    _MOVE_REF_CACHE = None
    _MOVE_CAR_CACHE = None


def _open_move_xab_sheet() -> Worksheet:
    return _worksheet(GSPREAD_URL_ANSWER, "Онлайн остатки Хаба")


def load_xab_cache(force_reload: bool = False) -> dict[tuple[str, str, str, str, str, str, str], list[list[str]]]:
    global _MOVE_XAB_CACHE
    if _MOVE_XAB_CACHE and not force_reload:
        return _MOVE_XAB_CACHE

    rows = _open_move_xab_sheet().get_all_values()
    groups: dict[tuple[str, str, str, str, str, str, str], list[list[str]]] = {}
    for row in rows[1:]:
        if len(row) < 14:
            continue
        key = (
            str(row[2]).strip(),
            str(row[3]).strip(),
            str(row[4]).strip(),
            str(row[5]).strip(),
            str(row[6]).strip(),
            str(row[7]).strip(),
            str(row[8]).strip(),
        )
        groups.setdefault(key, []).append(row)
    _MOVE_XAB_CACHE = groups
    return _MOVE_XAB_CACHE


def get_xab_koles(company: str, wheel_type: str) -> list[str]:
    cache = load_xab_cache(force_reload=False) or load_xab_cache(force_reload=True)
    result: list[str] = []
    for key, rows in cache.items():
        filtered_rows = [row for row in rows if len(row) > 9 and str(row[1]).strip() == str(company).strip()]
        if not filtered_rows:
            continue
        positions = [str(row[9]).strip() for row in filtered_rows]
        if wheel_type in ("Правое", "Левое"):
            if wheel_type in positions:
                result.append("|".join(key))
        elif wheel_type == "Ось":
            if "Левое" in positions and "Правое" in positions:
                result.append("|".join(key))
        elif wheel_type == "Комплект":
            if positions.count("Левое") >= 2 and positions.count("Правое") >= 2:
                result.append("|".join(key))
    result.sort()
    return result


def _item_to_xab_key_tuple(item: Any) -> tuple[str, str, str, str, str, str, str]:
    return (
        str(getattr(item, "marka_ts", "")).strip(),
        str(getattr(item, "radius", "")).strip(),
        str(getattr(item, "razmer", "")).strip(),
        str(getattr(item, "marka_rez", "")).strip(),
        str(getattr(item, "model_rez", "")).strip(),
        str(getattr(item, "sezon", "")).strip(),
        str(getattr(item, "tip_diska", "")).strip(),
    )


def update_xab_koles_bulk(company: str, items: list[Any], username: str, grz_tech: str) -> int:
    try:
        sh = _open(GSPREAD_URL_ANSWER)
        ws_direct = sh.worksheet("Онлайн остатки Хаба")
        ws_upload = sh.worksheet("Выгрузка сборка")
        direct_data = ws_direct.get_all_values()
        upload_data = ws_upload.get_all_values()

        upload_index: dict[tuple[str, ...], list[int]] = {}
        for index, row in enumerate(upload_data[1:], start=2):
            padded = row[:14] if len(row) >= 14 else row + [""] * (14 - len(row))
            upload_key = tuple(str(value).strip() for value in padded[:14])
            upload_index.setdefault(upload_key, []).append(index)

        need_map: dict[tuple[str, str, str, str, str, str, str], dict[str, int]] = {}
        requested_total = 0
        for item in items:
            key_tuple = _item_to_xab_key_tuple(item)
            need = need_map.setdefault(key_tuple, {"Левое": 0, "Правое": 0})
            left_need = int(getattr(item, "count_left", 0) or 0)
            right_need = int(getattr(item, "count_right", 0) or 0)
            need["Левое"] += left_need
            need["Правое"] += right_need
            requested_total += left_need + right_need

        deletion_row_indexes: list[int] = []
        current_date = _now_msk().strftime("%d.%m.%Y")
        updates: list[tuple[int, list[str]]] = []

        for row_index, row in enumerate(direct_data[1:], start=2):
            if len(row) < 14 or str(row[1]).strip() != str(company).strip():
                continue
            key_tuple = (
                str(row[2]).strip(),
                str(row[3]).strip(),
                str(row[4]).strip(),
                str(row[5]).strip(),
                str(row[6]).strip(),
                str(row[7]).strip(),
                str(row[8]).strip(),
            )
            need = need_map.get(key_tuple)
            if not need:
                continue

            position = str(row[9]).strip()
            if position not in need or need[position] <= 0:
                continue

            deletion_row_indexes.append(row_index)
            need[position] -= 1

            candidate_key = tuple(str(value).strip() for value in row[:14])
            rows_in_upload = upload_index.get(candidate_key, [])
            if not rows_in_upload:
                logger.warning(
                    "update_xab_koles_bulk: не найдена строка в 'Выгрузка сборка' для списания. company=%s key=%s user=%s grz=%s",
                    company,
                    candidate_key,
                    username,
                    grz_tech,
                )
                return 0
            upload_row_index = rows_in_upload.pop(0)
            updates.append((upload_row_index, [current_date, str(username), str(grz_tech)]))

        selected_total = len(deletion_row_indexes)
        updated_total = len(updates)
        if requested_total > 0 and selected_total < requested_total:
            logger.warning(
                "update_xab_koles_bulk: недостаточно позиций в Хабе для списания. company=%s need=%s got=%s user=%s grz=%s",
                company,
                requested_total,
                selected_total,
                username,
                grz_tech,
            )
            return 0
        if selected_total > 0 and updated_total != selected_total:
            logger.warning(
                "update_xab_koles_bulk: несоответствие количества отметок и списаний. company=%s deleted=%s updated=%s user=%s grz=%s",
                company,
                selected_total,
                updated_total,
                username,
                grz_tech,
            )
            return 0

        if updates:
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {
                        "range": f"'Выгрузка сборка'!O{row_idx}:Q{row_idx}",
                        "values": [values],
                    }
                    for row_idx, values in updates
                ],
            }
            _values_batch_update(GSPREAD_URL_ANSWER, body)

        if deletion_row_indexes:
            deletion_row_indexes.sort(reverse=True)
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": ws_direct.id,
                            "dimension": "ROWS",
                            "startIndex": row_idx - 1,
                            "endIndex": row_idx,
                        }
                    }
                }
                for row_idx in deletion_row_indexes
            ]
            sh.batch_update({"requests": requests})

        load_xab_cache(force_reload=True)
        return 1
    except Exception as exc:
        logger.exception("Ошибка в update_xab_koles_bulk: %s", exc)
        return 0


def get_sheet_header_map(ws: Worksheet) -> dict[str, int]:
    return {str(name).strip(): index + 1 for index, name in enumerate(ws.row_values(1)) if str(name).strip()}


def find_request_rows(request_number: str) -> list[WarehouseRequestRow]:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    data = _worksheet(warehouse_url, "Реестр заказ ТМЦ").get_all_values()
    if not data:
        return []
    header = data[0]
    idx_num = header.index("Номер заявки")
    idx_name = header.index("Вид ТМЦ")
    idx_qty = header.index("Кол-во")
    idx_status = header.index("Статус")
    rows: list[WarehouseRequestRow] = []
    for row_index, row in enumerate(data[1:], start=2):
        if len(row) <= max(idx_num, idx_name, idx_qty, idx_status):
            continue
        if str(row[idx_num]).strip() != str(request_number).strip():
            continue
        rows.append(
            WarehouseRequestRow(
                row_index=row_index,
                request_number=str(row[idx_num]).strip(),
                material_name=str(row[idx_name]).strip(),
                quantity=int(str(row[idx_qty]).strip() or "0"),
                status=str(row[idx_status]).strip(),
            )
        )
    return rows


def append_report_link_by_request(request_number: str, report_link: str) -> int:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    ws = _worksheet(warehouse_url, "Реестр заказ ТМЦ")
    col_map = get_sheet_header_map(ws)
    col_link = col_map.get("Ссылка на отчет")
    if not col_link:
        raise RuntimeError("В листе 'Реестр заказ ТМЦ' нет колонки 'Ссылка на отчет'")
    rows = find_request_rows(request_number)
    if not rows:
        return 0
    cells = [gspread.Cell(row=row.row_index, col=col_link, value=report_link) for row in rows]
    ws.update_cells(cells, value_input_option="USER_ENTERED")
    return len(cells)


def get_material_total_quantity_map() -> dict[str, int]:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    data = _worksheet(warehouse_url, "Склад").get_all_values()
    if not data:
        return {}
    header = data[0]
    idx_name = header.index("Наименование")
    idx_qty = header.index("Количество")
    totals: dict[str, int] = {}
    for row in data[1:]:
        if len(row) <= max(idx_name, idx_qty):
            continue
        name = str(row[idx_name]).strip()
        if not name:
            continue
        quantity = int(str(row[idx_qty]).strip() or "0")
        totals[name] = totals.get(name, 0) + quantity
    return totals


def get_material_cells_with_row_indexes(material_name: str) -> list[dict[str, Any]]:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    data = _worksheet(warehouse_url, "Склад").get_all_values()
    if not data:
        return []
    header = data[0]
    idx_name = header.index("Наименование")
    idx_qty = header.index("Количество")
    idx_cell = header.index("Ячейка")
    rows: list[dict[str, Any]] = []
    for row_index, row in enumerate(data[1:], start=2):
        if len(row) <= max(idx_name, idx_qty, idx_cell):
            continue
        if str(row[idx_name]).strip() != str(material_name).strip():
            continue
        rows.append(
            {
                "row_index": row_index,
                "name": str(row[idx_name]).strip(),
                "quantity": int(str(row[idx_qty]).strip() or "0"),
                "cell": str(row[idx_cell]).strip(),
            }
        )
    return rows


def close_empty_cell_after_transfer(material_name: str, cell_name: str) -> bool:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    sklad_ws = _worksheet(warehouse_url, "Склад")
    free_ws = _worksheet(warehouse_url, "Справочник ячеек")
    rows = get_material_cells_with_row_indexes(material_name)
    target = next((row for row in rows if row["cell"] == cell_name), None)
    if not target or target["quantity"] != 0:
        return False
    qty_col = get_sheet_header_map(sklad_ws).get("Количество")
    if not qty_col:
        raise RuntimeError("В листе 'Склад' нет колонки 'Количество'")
    sklad_ws.update_cell(target["row_index"], qty_col, "")
    free_ws.append_row([cell_name], value_input_option="USER_ENTERED")
    return True


def reserve_free_cell(cell_name: str) -> bool:
    warehouse_url = _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")
    ws = _worksheet(warehouse_url, "Справочник ячеек")
    values = ws.get_all_values()
    for row_index, row in enumerate(values[1:], start=2):
        if row and str(row[0]).strip() == str(cell_name).strip():
            ws.delete_rows(row_index)
            return True
    return False


def get_shm_locations_by_company_normalized(company_ui: str) -> list[str]:
    locations_url = _require_url(URL_GOOGLE_SHEETS_LOC_SHM, "URL_GOOGLE_SHEETS_LOC_SHM")
    mapping = {"СитиДрайв": "СИТИ", "Ситидрайв": "СИТИ", "Яндекс": "ЯНДЕКС", "Белка": "БЕЛКА"}
    key = mapping.get(company_ui)
    if not key:
        return []
    data = _worksheet(locations_url, "Локации СШМ").get_all_values()
    if not data:
        return []
    header = data[0]
    idx_addr = header.index("Адрес")
    idx_company = header.index("Каршеринг")
    result = {
        str(row[idx_addr]).strip()
        for row in data[1:]
        if len(row) > max(idx_addr, idx_company)
        and str(row[idx_company]).strip().upper() == key
        and str(row[idx_addr]).strip()
    }
    return sorted(result, key=lambda value: value.lower())


def get_order_catalog_snapshot() -> dict[str, list[dict[str, Any]]]:
    order_url = _require_url(URL_GOOGLE_SHEETS_ORDER, "URL_GOOGLE_SHEETS_ORDER")
    sh = _open(order_url)
    return {
        "city_disk": sh.worksheet("Сити Диски сити new").get_all_records(),
        "yandex_disk": sh.worksheet("ЯД Диски").get_all_records(),
        "city_tire": sh.worksheet("СИТИ Лето РФ new").get_all_records(),
        "yandex_tire": sh.worksheet("ЯД Лето РФ new").get_all_records(),
    }


# --- Warehouse compatibility layer -------------------------------------------------

_CACHE_SKLAD_FILTERED_DATA: Optional[list[list[str]]] = None
_CACHE_SKLAD_FILTERED_AT: Optional[datetime] = None
_CACHE_SKLAD_FILTERED_TTL_SEC = 60


def _warehouse_url() -> str:
    return _require_url(URL_GOOGLE_SHEETS_SKLAD, "URL_GOOGLE_SHEETS_SKLAD")


def _warehouse_sheet_values(title: str, *, skip_header: bool = False) -> list[list[str]]:
    rows = _worksheet(_warehouse_url(), title).get_all_values()
    if skip_header:
        return rows[1:]
    return rows


def _warehouse_header_indexes(header: list[str], required: list[str]) -> dict[str, int]:
    index_map: dict[str, int] = {}
    normalized = {str(name).strip(): idx for idx, name in enumerate(header)}
    for name in required:
        if name not in normalized:
            raise RuntimeError(f"В листе отсутствует обязательная колонка: {name}")
        index_map[name] = normalized[name]
    return index_map


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        text = str(value).strip().replace(',', '.')
        return int(float(text)) if text else default
    except Exception:
        return default


def load_sklad_data(force_reload: bool = False) -> list[list[str]]:
    global _CACHE_SKLAD_FILTERED_DATA, _CACHE_SKLAD_FILTERED_AT
    now = datetime.now()
    if (
        not force_reload
        and _CACHE_SKLAD_FILTERED_DATA is not None
        and _CACHE_SKLAD_FILTERED_AT is not None
        and (now - _CACHE_SKLAD_FILTERED_AT).total_seconds() <= _CACHE_SKLAD_FILTERED_TTL_SEC
    ):
        return _CACHE_SKLAD_FILTERED_DATA

    try:
        data = _warehouse_sheet_values("Склад", skip_header=False)
        if not data:
            _CACHE_SKLAD_FILTERED_DATA = []
            _CACHE_SKLAD_FILTERED_AT = now
            return []

        header = data[0]
        idx = _warehouse_header_indexes(header, ["Наименование", "Количество", "Ячейка"])
        filtered: list[list[str]] = [header]
        for row in data[1:]:
            if len(row) <= max(idx.values()):
                continue
            qty_str = str(row[idx["Количество"]]).strip()
            if not qty_str:
                continue
            qty_norm = qty_str.replace(',', '.')
            try:
                qty = float(qty_norm)
            except ValueError:
                filtered.append(row)
                continue
            if qty != 0:
                filtered.append(row)

        _CACHE_SKLAD_FILTERED_DATA = filtered
        _CACHE_SKLAD_FILTERED_AT = now
        return filtered
    except Exception as exc:
        logger.exception("Ошибка при загрузке данных склада: %s", exc)
        _CACHE_SKLAD_FILTERED_DATA = []
        _CACHE_SKLAD_FILTERED_AT = now
        return []


def get_material_names() -> list[str]:
    data = load_sklad_data()
    if not data:
        return []
    header = data[0]
    try:
        idx_name = _warehouse_header_indexes(header, ["Наименование"])["Наименование"]
    except Exception:
        return []
    names = [str(row[idx_name]).strip() for row in data[1:] if len(row) > idx_name and str(row[idx_name]).strip()]
    return sorted(set(names), key=lambda s: s.lower())


def get_material_quantity(material_name: str) -> str:
    totals = get_material_total_quantity_map()
    qty = totals.get(str(material_name).strip())
    return str(qty) if qty is not None else ""


def get_recipient_names() -> list[str]:
    try:
        ws = _worksheet(_warehouse_url(), "Справочник")
        values = ws.col_values(1)[1:]
        return [str(value).strip() for value in values if str(value).strip()]
    except Exception as exc:
        logger.exception("Ошибка при загрузке получателей: %s", exc)
        return []


def get_material_cells(material_name: str) -> list[dict[str, str]]:
    rows = get_material_cells_with_row_indexes(material_name)
    result: list[dict[str, str]] = []
    for row in rows:
        quantity = row.get("quantity", 0)
        if _safe_int(quantity, 0) == 0:
            continue
        result.append({
            "cell": str(row.get("cell") or "").strip(),
            "quantity": str(row.get("quantity") if row.get("quantity") is not None else "0"),
        })
    return result


def write_row_to_sheet(worksheet_name: str, row: list[Any]) -> None:
    try:
        _append_row(_warehouse_url(), worksheet_name, row, value_input_option="USER_ENTERED")
    except Exception as exc:
        logger.exception("Ошибка записи строки в таблицу %s: %s", worksheet_name, exc)


def write_arrival_row(arrival_items: list[dict[str, Any]], user_tag: str, message_link: str) -> None:
    current_dt = _now_msk().strftime("%d.%m.%Y %H:%M:%S")
    rows = []
    for item in arrival_items:
        rows.append([
            current_dt,
            "Поступление",
            "",
            item.get("name", ""),
            item.get("quantity", ""),
            "",
            user_tag,
            message_link,
            item.get("cell", ""),
        ])
    if rows:
        _append_rows(_warehouse_url(), "Передача/поступление", rows, value_input_option="USER_ENTERED")


def write_transfer_row(transfer_items: list[dict[str, Any]], recipient: str, user_tag: str, message_link: str) -> None:
    current_dt = _now_msk().strftime("%d.%m.%Y %H:%M:%S")
    rows = []
    for item in transfer_items:
        rows.append([
            current_dt,
            "Выдача",
            "",
            item.get("name", ""),
            item.get("quantity", ""),
            recipient,
            user_tag,
            message_link,
            item.get("cell", ""),
        ])
    if rows:
        _append_rows(_warehouse_url(), "Передача/поступление", rows, value_input_option="USER_ENTERED")


def return_cell_to_free(cell: str) -> None:
    try:
        warehouse_url = _warehouse_url()
        sklad_ws = _worksheet(warehouse_url, "Склад")
        free_ws = _worksheet(warehouse_url, "Справочник ячеек")
        data = sklad_ws.get_all_values()
        if not data:
            return
        header = data[0]
        idx = _warehouse_header_indexes(header, ["Ячейка"])
        col_cell = idx["Ячейка"] + 1
        for row_index, row in enumerate(data[1:], start=2):
            if len(row) > idx["Ячейка"] and str(row[idx["Ячейка"]]).strip() == str(cell).strip():
                sklad_ws.update_cell(row_index, col_cell, "")
                break
        free_ws.append_row([cell], value_input_option="USER_ENTERED")
    except Exception as exc:
        logger.exception("Ошибка возврата ячейки в справочник свободных: %s", exc)


def get_free_cells() -> list[str]:
    try:
        data = _warehouse_sheet_values("Справочник ячеек", skip_header=False)
        if not data:
            return []
        free: list[str] = []
        for row in data[1:]:
            if row and str(row[0]).strip():
                free.append(str(row[0]).strip())
        return free
    except Exception as exc:
        logger.exception("Ошибка загрузки свободных ячеек: %s", exc)
        return []


def remove_free_cell(cell: str) -> None:
    try:
        reserve_free_cell(cell)
    except Exception as exc:
        logger.exception("Ошибка удаления свободной ячейки: %s", exc)


def get_data_order(name: str) -> list[dict[str, Any]]:
    order_url = _require_url(URL_GOOGLE_SHEETS_ORDER, "URL_GOOGLE_SHEETS_ORDER")
    return _worksheet(order_url, name).get_all_records()


def load_data_rez_disk() -> None:
    import app.config as cfg
    cfg.BAZA_DISK_SITY = get_data_order("Сити Диски сити new")
    cfg.BAZA_DISK_YNDX = get_data_order("ЯД Диски")
    cfg.BAZA_REZN_SITY = get_data_order("СИТИ Лето РФ new")
    cfg.BAZA_REZN_YNDX = get_data_order("ЯД Лето РФ new")


def get_shm_locations_by_company(company_ui: str) -> list[str]:
    return get_shm_locations_by_company_normalized(company_ui)


def get_next_request_number() -> str:
    try:
        rows = _worksheet(_warehouse_url(), "Реестр заказ ТМЦ").col_values(2)[1:]
        max_num = 0
        for value in rows:
            text = str(value).strip().lower()
            if not text.startswith("zv"):
                continue
            digits = text[2:]
            try:
                max_num = max(max_num, int(digits))
            except ValueError:
                continue
        return f"zv{max_num + 1 if max_num > 0 else 1}"
    except Exception as exc:
        logger.exception("Ошибка получения номера заявки ТМЦ: %s", exc)
        return "zv1"


def write_request_tmc_rows(
    request_number: str,
    fio: str,
    tag: str,
    department: str,
    items: list[dict[str, Any]],
    message_link: str,
) -> None:
    current_dt = _now_msk().strftime("%d.%m.%Y %H:%M:%S")
    rows: list[list[Any]] = []
    for item in items:
        rows.append([
            current_dt,
            request_number,
            fio,
            tag,
            department,
            item.get("name", ""),
            item.get("quantity", ""),
            "Новая",
            message_link,
        ])
    if rows:
        _append_rows(_warehouse_url(), "Реестр заказ ТМЦ", rows, value_input_option="USER_ENTERED")


def get_open_request_numbers() -> list[str]:
    try:
        data = _worksheet(_warehouse_url(), "Реестр заказ ТМЦ").get_all_values()
        if not data:
            return []
        header = data[0]
        idx = _warehouse_header_indexes(header, ["Номер заявки", "Статус"])
        result = {
            str(row[idx["Номер заявки"]]).strip()
            for row in data[1:]
            if len(row) > max(idx.values())
            and str(row[idx["Номер заявки"]]).strip()
            and str(row[idx["Статус"]]).strip().lower() == "новая"
        }
        return sorted(result)
    except Exception as exc:
        logger.exception("Ошибка получения открытых заявок ТМЦ: %s", exc)
        return []


def get_request_items(request_number: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    try:
        data = _worksheet(_warehouse_url(), "Реестр заказ ТМЦ").get_all_values()
        if not data:
            return []
        header = data[0]
        idx = _warehouse_header_indexes(header, ["Номер заявки", "Вид ТМЦ", "Кол-во"])
        for row in data[1:]:
            if len(row) <= max(idx.values()):
                continue
            if str(row[idx["Номер заявки"]]).strip() != str(request_number).strip():
                continue
            name = str(row[idx["Вид ТМЦ"]]).strip()
            qty = str(row[idx["Кол-во"]]).strip()
            if name:
                items.append({"name": name, "quantity": qty})
        return items
    except Exception as exc:
        logger.exception("Ошибка чтения позиций заявки ТМЦ: %s", exc)
        return []


def update_request_status(request_number: str, status: str, message_link: Optional[str] = None) -> None:
    try:
        ws = _worksheet(_warehouse_url(), "Реестр заказ ТМЦ")
        data = ws.get_all_values()
        if not data:
            return
        header = data[0]
        idx = _warehouse_header_indexes(header, ["Номер заявки", "Статус", "Ссылка на отчет"])
        col_status = chr(ord('A') + idx["Статус"])
        col_link = chr(ord('A') + idx["Ссылка на отчет"])
        updates: list[dict[str, Any]] = []
        for row_index, row in enumerate(data[1:], start=2):
            if len(row) <= idx["Номер заявки"]:
                continue
            if str(row[idx["Номер заявки"]]).strip() != str(request_number).strip():
                continue
            if message_link is None:
                updates.append({"range": f"{col_status}{row_index}", "values": [[status]]})
            else:
                updates.append({"range": f"{col_status}{row_index}:{col_link}{row_index}", "values": [[status, message_link]]})
        if updates:
            _batch_update(_warehouse_url(), "Реестр заказ ТМЦ", updates)
    except Exception as exc:
        logger.exception("Ошибка обновления статуса заявки ТМЦ: %s", exc)
