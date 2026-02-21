import logging
import gspread
import re
from gspread import Client, Spreadsheet
from typing import List, Tuple, Optional, Any
from datetime import datetime, timedelta, time
from app.config import URL_GOOGLE_SHEETS_CHART, GSPREAD_URL_MAIN, GSPREAD_URL_ANSWER, GSPREAD_URL_SKLAD, GSPREAD_URL_GATES
from gspread.exceptions import APIError

EXCEL_EPOCH = datetime(1899, 12, 30)

logger = logging.getLogger(__name__)

def _log_sheet_write(action: str, sheet: str, payload: Any) -> None:
    logger.info("[GSHEETS] %s | sheet=%s | payload=%s", action, sheet, payload)

def _parse_dt(val: Any, fallback_date: Optional[Any] = None) -> Optional[datetime]:
    """Пытается распарсить datetime из ячейки.
    val — может быть строкой/числом/пустым; fallback_date — из колонки 'Дата' (если start без даты)."""
    if val is None:
        return None

    # Число -> excel serial
    if isinstance(val, (int, float)):
        # дробная часть — время суток
        try:
            return EXCEL_EPOCH + timedelta(days=float(val))
        except Exception:
            return None

    s = str(val).strip()
    if not s:
        return None

    # Популярные форматы
    fmts = [
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%d.%m.%Y",
        "%H:%M:%S",
        "%H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            # если распознали только время — дополним датой из fallback_date
            if fmt in ("%H:%M:%S", "%H:%M"):
                if fallback_date:
                    fd = _parse_dt(fallback_date)
                    if fd:
                        return fd.replace(hour=dt.hour, minute=dt.minute, second=dt.second)
                return None
            return dt
        except Exception:
            continue
    return None

def find_logistics_rows_shift(now: Optional[datetime] = None) -> List[Tuple[str, str, str]]:
    now = (datetime.now() + timedelta(hours=3)) if now is None else now

    gc: Client = gspread.service_account("app/creds.json")
    ws = gc.open_by_url(URL_GOOGLE_SHEETS_CHART).worksheet("Логисты выход на смену")

    # читаем как записи по заголовкам
    rows = ws.get_all_records()  # ожидаем заголовки как на скрине

    result: List[Tuple[str, str, str]] = []
    for r in rows:
        fio = str(r.get("ФИО", "")).strip()
        tag = str(r.get("Тег", "")).strip()
        direction = str(r.get("Направление", "")).strip()

        start_raw = r.get("Время начала смены")
        end_raw = r.get("Время конца смены")
        date_raw = r.get("Дата")

        # 1) Конец смены должен быть пустым
        if isinstance(end_raw, str) and end_raw.strip():
            continue
        if end_raw and not isinstance(end_raw, str):  # число/дата — значит не пусто
            continue

        # 2) Корректно парсим start
        start_dt = _parse_dt(start_raw, fallback_date=date_raw)
        if not start_dt:
            continue

        # 3) Только начавшие не позднее 15 часов назад (и не из будущего)
        delta_sec = (now - start_dt).total_seconds()
        if 0 <= delta_sec <= 3 * 3600:
            if fio and tag and direction:
                result.append((direction, fio, tag))

    return result

def write_in_answers_ras_shift(
    tlist: list,
    page: str = "Лист1",
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            gc: Client = gspread.service_account("app/creds.json")
            sh = gc.open_by_url(URL_GOOGLE_SHEETS_CHART)
            ws = sh.worksheet(page)
            _log_sheet_write("append_row", page, tlist)
            ws.append_row(tlist, value_input_option="RAW")
            return
        except APIError as e:
            last_error = e
            if attempt == max_attempts:
                raise
            sleep_for = base_delay * attempt
            logger.warning(
                "write_in_answers_ras_shift retry %s/%s after APIError: %s",
                attempt,
                max_attempts,
                e,
            )
            import time as _time
            _time.sleep(sleep_for)
        except Exception as e:
            last_error = e
            if attempt == max_attempts:
                raise
            sleep_for = base_delay * attempt
            logger.warning(
                "write_in_answers_ras_shift retry %s/%s after error: %s",
                attempt,
                max_attempts,
                e,
            )
            import time as _time
            _time.sleep(sleep_for)

    if last_error:
        raise last_error

def _open_sklad_sheet() -> Spreadsheet:
    gc: Client = gspread.service_account("app/creds.json")
    return gc.open_by_url(GSPREAD_URL_SKLAD)


def loading_bz_znaniya(company: str) -> list[list[str]]:
    gc: Client = gspread.service_account("app/creds.json")
    sh_main = gc.open_by_url(GSPREAD_URL_MAIN)
    return sh_main.worksheet(company).get_all_values()[1:]

async def load_nomenclature_reference_data() -> dict:
    gc: Client = gspread.service_account("app/creds.json")
    sh_main = gc.open_by_url(GSPREAD_URL_MAIN)

    def _sheet_values(title: str) -> list[list[str]]:
        return sh_main.worksheet(title).get_all_values()[1:]

    return {
        "city": _sheet_values("Резина Сити"),
        "yandex": _sheet_values("Резина ЯД"),
        "belka": _sheet_values("Резина Белка"),
    }

async def load_sborka_reference_data() -> dict:
    gc: Client = gspread.service_account("app/creds.json")
    sh_main = gc.open_by_url(GSPREAD_URL_MAIN)

    def _sheet_values(title: str) -> list[list[str]]:
        return sh_main.worksheet(title).get_all_values()[1:]

    return {
        "rezina": {
            "city": _sheet_values("Резина Сити"),
            "yandex": _sheet_values("Резина ЯД"),
            "belka": _sheet_values("Резина Белка"),
        },
        "cars": {
            "city": _sheet_values("Перечень ТС Сити"),
            "yandex": _sheet_values("Перечень ТС Яд"),
            "belka": _sheet_values("Перечень ТС Белка"),
        },
    }

def get_max_nomer_sborka() -> int:
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()[1:]
    numbers: list[int] = []
    for row in rows:
        if len(row) > 13 and row[13]:
            num = re.sub(r"[^0-9]", "", str(row[13]))
            if num:
                numbers.append(int(num))
    return max(numbers) if numbers else 0

def get_number_util(company: str, column_index: int) -> str:
    """Возвращает следующий порядковый номер утиля по компании.

    column_index: индекс колонки в листе "Выгрузка ремонты/утиль"
    (для диска/резины используются разные колонки).
    """
    gc: Client = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(GSPREAD_URL_ANSWER)
    ws = sh.worksheet("Выгрузка ремонты/утиль")
    rows = ws.get_all_values()[1:]

    values: list[str] = []
    for row in rows:
        if len(row) > column_index and row[column_index]:
            values.append(str(row[column_index]).strip())

    if company == "СитиДрайв":
        prefix = "su"
    elif company == "Яндекс":
        prefix = "yu"
    else:
        prefix = "blk"

    nums: list[int] = []
    for value in values:
        if not value.startswith(prefix):
            continue
        num = re.sub(r"[^0-9]", "", value)
        if num:
            nums.append(int(num))

    next_num = (max(nums) + 1) if nums else 1
    return f"{prefix}{next_num}"

def write_soberi_in_google_sheets_rows(rows: list[list[str]]) -> None:
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    _log_sheet_write("append_rows", "Заявка на сборку", rows)
    ws.append_rows(
        rows,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )

def write_soberi_in_google_sheets(tlist: list) -> None:
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    _log_sheet_write("append_row", "Заявка на сборку", tlist)
    ws.append_row(
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )


def get_record_sklad() -> list[list[str]]:
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()[1:]
    out: list[list[str]] = []
    for row in rows:
        if len(row) > 15 and not row[15] and len(row) > 2 and row[2]:
            out.append(row[3:14])
    return out


def nomer_sborka(company, radius, razmer, marka_rez, model_rez, sezon, marka_ts, type_disk, type_kolesa) -> list[str]:
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()
    unique_numbers: set[str] = set()

    for row in rows:
        if len(row) < 14:
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
            and str(row[11]) == str(type_kolesa)
        ):
            unique_numbers.add(str(row[13]))

    for row in rows:
        if len(row) < 14:
            continue
        if (
            str(row[3]) == str(company)
            and str(row[4]) == str(marka_ts)
            and str(row[5]) == str(radius)
            and str(row[6]) == str(razmer)
            and str(row[9]) == str(sezon)
            and str(row[11]) == str(type_kolesa)
        ):
            unique_numbers.add(str(row[13]))

    return list(unique_numbers)


def nomer_sborka_ko(company, radius, razmer, marka_rez, model_rez, sezon, marka_ts, type_disk, type_kolesa):
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()

    komplekts = set()
    axes: list[str] = []
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
        key = row[13]
        groups.setdefault(key, []).append(row)

    for key, group_rows in groups.items():
        if len(group_rows) == 4:
            komplekts.add(key)

    if len(matching_rows) >= 2:
        sides = {row[11] for row in matching_rows}
        if "Левое" in sides and "Правое" in sides:
            axes.append(matching_rows[0][13])

    if type_kolesa == "Ось":
        return axes
    if type_kolesa == "Комплект":
        return list(komplekts)
    return []


def update_data_sborka(marka_rez, model_rez, type_disk, type_kolesa, nomer_sborka):
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()

    for idx, row in enumerate(rows, start=1):
        if len(row) < 14:
            continue
        if str(row[13]) == str(nomer_sborka) and str(row[11]) == str(type_kolesa):
            updates = [
                {"range": f"K{idx}:K{idx}", "values": [[str(type_disk)]]},
                {"range": f"H{idx}:I{idx}", "values": [[str(marka_rez), str(model_rez)]]},
            ]
            _log_sheet_write("batch_update", "Заявка на сборку", updates)
            ws.batch_update(updates)
            return


def update_record_sborka(company, username, radius, razmer, marka_rez, model_rez, sezon, marka_ts, type_disk, type_kolesa, message_link, nomer_sborka):
    sh = _open_sklad_sheet()
    ws = sh.worksheet("Заявка на сборку")
    rows = ws.get_all_values()
    current_time = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    matches: list[tuple[int, list[str]]] = []
    for j, row in enumerate(rows):
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
            and str(row[13]) == str(nomer_sborka)
            and str(row[16]) == ""
        ):
            matches.append((j, row))

    if not matches:
        for j, row in enumerate(rows):
            if len(row) < 17:
                continue
            if (
                str(row[3]) == str(company)
                and str(row[4]) == str(marka_ts)
                and str(row[5]) == str(radius)
                and str(row[6]) == str(razmer)
                and str(row[9]) == str(sezon)
                and str(row[13]) == str(nomer_sborka)
                and str(row[16]) == ""
            ):
                matches.append((j, row))

    updates: list[dict] = []
    if type_kolesa not in ("Комплект", "Ось"):
        for j, row in matches:
            if str(row[11]) == str(type_kolesa):
                updates.append({"range": f"P{j + 1}:S{j + 1}", "values": [[current_time, "Собрано", str(message_link), str(username)]]})
                break
    else:
        required = 2 if type_kolesa == "Комплект" else 1
        sides_updated = {"Правое": 0, "Левое": 0}
        for j, row in matches:
            side = str(row[11])
            if side in sides_updated and sides_updated[side] < required:
                updates.append({"range": f"P{j + 1}:S{j + 1}", "values": [[current_time, "Собрано", str(message_link), str(username)]]})
                sides_updated[side] += 1

    if updates:
        _log_sheet_write("batch_update", "Заявка на сборку", updates)
        ws.batch_update(updates)

async def load_damage_reference_data() -> dict:
    """Загружает справочники для сценария /damage из Google Sheets."""
    gc: Client = gspread.service_account("app/creds.json")
    sh_main = gc.open_by_url(GSPREAD_URL_MAIN)

    def _sheet_values(title: str) -> list[list[str]]:
        return sh_main.worksheet(title).get_all_values()[1:]

    rez_city = _sheet_values("Резина Сити")
    rez_yandex = _sheet_values("Резина ЯД")
    rez_belka = _sheet_values("Резина Белка")

    cars_city = _sheet_values("Перечень ТС Сити")
    cars_yandex = _sheet_values("Перечень ТС Яд")
    cars_belka = _sheet_values("Перечень ТС Белка")

    return {
        "rezina": {"city": rez_city, "yandex": rez_yandex, "belka": rez_belka},
        "cars": {"city": cars_city, "yandex": cars_yandex, "belka": cars_belka},
    }


def write_in_answers_ras(tlist: list, name_sheet: str, max_attempts: int = 3, base_delay: float = 1.0) -> None:
    """Совместимая запись в Google Sheets лист ответов (для damage-потока)."""
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            gc: Client = gspread.service_account("app/creds.json")
            sh = gc.open_by_url(GSPREAD_URL_ANSWER)
            ws = sh.worksheet(name_sheet)
            _log_sheet_write("append_row", name_sheet, tlist)
            ws.append_row(
                tlist,
                value_input_option="USER_ENTERED",
                table_range="A1",
                insert_data_option="INSERT_ROWS",
            )
            return
        except APIError as e:
            last_error = e
            if attempt == max_attempts:
                raise
            import time as _time
            _time.sleep(base_delay * attempt)
        except Exception as e:
            last_error = e
            if attempt == max_attempts:
                raise
            import time as _time
            _time.sleep(base_delay * attempt)

    if last_error:
        raise last_error

def write_in_answers_ras_nomen(tlist: list, name_sheet: str, max_attempts: int = 3, base_delay: float = 1.0) -> None:
    """Запись в основной файл номенклатуры (листы Резина Сити/ЯД/Белка)."""
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            gc: Client = gspread.service_account("app/creds.json")
            sh = gc.open_by_url(GSPREAD_URL_MAIN)
            ws = sh.worksheet(name_sheet)
            _log_sheet_write("append_row", name_sheet, tlist)
            ws.append_row(tlist, value_input_option="USER_ENTERED")
            return
        except APIError as e:
            last_error = e
            if attempt == max_attempts:
                raise
            import time as _time
            _time.sleep(base_delay * attempt)
        except Exception as e:
            last_error = e
            if attempt == max_attempts:
                raise
            import time as _time
            _time.sleep(base_delay * attempt)

    if last_error:
        raise last_error

def write_open_gate_row(fio: str, car_plate: str, company: str, message_link: str) -> None:
    ws = gspread.service_account("app/creds.json").open_by_url(GSPREAD_URL_GATES).worksheet("Выгрузка Техники")
    now_msk = datetime.now() + timedelta(hours=3)
    payload = [
        now_msk.strftime("%d.%m.%Y"),
        now_msk.strftime("%H:%M:%S"),
        fio,
        car_plate,
        company,
        message_link,
    ]
    _log_sheet_write("append_row", "Выгрузка Техники", payload)
    ws.append_row(
        payload,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )


def find_logistics_rows(limit_hours: int = 12) -> tuple[list[str], list[str]]:
    ws = gspread.service_account("app/creds.json").open_by_url(URL_GOOGLE_SHEETS_CHART).worksheet("Логисты выход на смену")
    rows = ws.get_all_values()
    if not rows:
        return [], []

    header, data = rows[0], rows[1:]

    def col_idx(name: str) -> int:
        try:
            return [h.strip().lower() for h in header].index(name.lower())
        except ValueError:
            return -1

    i_fio = col_idx("ФИО")
    i_tag = col_idx("Тег")
    i_dir = col_idx("Направление")
    i_start = col_idx("Время начала смены")
    i_end = col_idx("Время конца смены")

    if min(i_fio, i_tag, i_dir, i_start, i_end) < 0:
        return [], []

    now = datetime.now() + timedelta(hours=3)
    window = timedelta(hours=limit_hours)

    tags: list[str] = []
    fios: list[str] = []

    for row in data:
        if max(i_fio, i_tag, i_dir, i_start, i_end) >= len(row):
            continue

        if row[i_dir].strip() != "ВШМ":
            continue

        if row[i_end].strip():
            continue

        start_dt = _parse_dt(row[i_start], fallback_date=row[i_start])
        if not start_dt:
            start_dt = _parse_dt(row[i_start])
        if not start_dt:
            continue

        if timedelta(0) <= (now - start_dt) <= window:
            tag = row[i_tag].strip()
            fio = row[i_fio].strip()
            if tag and fio:
                tags.append(tag)
                fios.append(fio)

    return tags, fios