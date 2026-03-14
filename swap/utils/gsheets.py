import gspread
import requests
import calendar
import time
import random
import functools
from typing import Sequence, Tuple, Optional
from datetime import datetime, date, timedelta, timezone
from app.config import URL_GOOGLE_SHEETS_ANSWERS, URL_GOOGLE_SHEETS_PROBLEM, URL_GOOGLE_SHEETS_CHART, URL_IS_ACTIV_KK, URL_GET_USER_ID_BY_FIO, URL_GOOGLE_SHEETS_PROBLEM_NEW
from collections import defaultdict
from app.utils.script import shorten_name
from gspread.exceptions import APIError

# ───────────────────────────── Retry / Backoff / Rate-limit ─────────────────────────────
_RETRY_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5
_BASE_DELAY  = 0.5    # сек
_JITTER      = 0.35   # сек
_RATE_LIMIT_DELAY = 0.30  # минимальная пауза между любыми вызовами к GS
_last_call_ts = 0.0

def _ratelimit_sleep():
    global _last_call_ts
    now = time.time()
    left = _RATE_LIMIT_DELAY - (now - _last_call_ts)
    if left > 0:
        time.sleep(left)
    _last_call_ts = time.time()

def _with_retry(fn, *args, **kwargs):
    """Запускает fn с экспоненциальным бэкоффом и джиттером + мягкий rate-limit."""
    for attempt in range(_MAX_RETRIES):
        _ratelimit_sleep()
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code in _RETRY_STATUS:
                delay = _BASE_DELAY * (2 ** attempt) + random.uniform(0, _JITTER)
                time.sleep(delay)
                continue
            raise
        except requests.RequestException:
            delay = _BASE_DELAY * (2 ** attempt) + random.uniform(0, _JITTER)
            time.sleep(delay)
        except Exception:
            # неочевидные сетевые/транзитные сбои попробуем повторить 1-2 раза
            if attempt < 2:
                time.sleep(_BASE_DELAY + random.uniform(0, _JITTER))
                continue
            raise
    # если все попытки исчерпаны
    raise RuntimeError("Google Sheets call failed after retries")

# ───────────────────────────── Удобные обёртки для открытия и вызовов ─────────────────────────────
def _open_ws(url: str, sheet_title: str):
    gc = _with_retry(gspread.service_account, "app/creds.json")
    sh = _with_retry(gc.open_by_url, url)
    ws = _with_retry(sh.worksheet, sheet_title)
    return ws

# Worksheet-обёртки (единая точка для всех вызовов)
def ws_get_all_values(ws):      return _with_retry(ws.get_all_values)
def ws_get_all_records(ws):     return _with_retry(ws.get_all_records)
def ws_row_values(ws, r):       return _with_retry(ws.row_values, r)
def ws_col_values(ws, c):       return _with_retry(ws.col_values, c)
def ws_update_cell(ws, r, c, v):return _with_retry(ws.update_cell, r, c, v)
def ws_append_row(ws, vals, **kw): return _with_retry(ws.append_row, vals, **kw)
def ws_batch_update(ws, updates):  return _with_retry(ws.batch_update, updates)
def ws_update(ws, rng, vals):      return _with_retry(ws.update, rng, vals)
def ws_findall(ws, query, **kw):   return _with_retry(ws.findall, query, **kw)

def _bounds_current_half(today: date) -> tuple[date, date]:
    y, m, d = today.year, today.month, today.day
    if 10 <= d <= 25:
        return date(y, m, 10), date(y, m, 25)

    if d >= 26:
        nm = (today.replace(day=28) + timedelta(days=4))  # любой день след. месяца
        return date(y, m, 26), date(nm.year, nm.month, 9)

    prev = today.replace(day=1) - timedelta(days=1)  # последний день предыдущего
    return date(prev.year, prev.month, 26), date(y, m, 9)


def _bounds_next_half(today: date) -> tuple[date, date]:
    y, m, d = today.year, today.month, today.day
    if 10 <= d <= 25:
        nm = (today.replace(day=28) + timedelta(days=4))
        return date(y, m, 26), date(nm.year, nm.month, 9)

    if d >= 26:
        nm = (today.replace(day=28) + timedelta(days=4))
        return date(nm.year, nm.month, 10), date(nm.year, nm.month, 25)

    return date(y, m, 10), date(y, m, 25)


def get_available_dates_for_fio(fio: str) -> dict[str, list[str]]:
    """
    Для заданного ФИО возвращаем словарь
      { дата_выхода: [список дат из «следующей» половины месяца] }.
    Причём «следующая» половина считается так:
      — если сегодня 1–15, предлагаем 16–последний_день_текущего_месяца;
      — если сегодня 16–31, предлагаем 1–15 следующего месяца.
    """
    # ваш код чтения Google-таблицы целиком не трогаем,
    # оставляем ту же логику перебора records:
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    records = ws_get_all_records(ws)

    today = date.today()

    def make_dates(start: date, end: date) -> list[str]:
        out = []
        d = start
        while d <= end:
            out.append(d.strftime("%d.%m.%Y"))
            d += timedelta(days=1)
        return out

    start_next, end_next = _bounds_next_half(today)
    dates = make_dates(start_next, end_next)

    result: dict[str, list[str]] = {}
    for rec in records:
        if rec.get("ФИО") != fio:
            continue
        key = rec.get("Дата выхода", "")
        # если дата выхода неправильно отформатирована — просто пропустим
        try:
            # проверка, чтобы не вставлять мусор
            _ = datetime.strptime(key, "%d.%m.%Y")
        except:
            continue
        result[key] = dates

    # если по ФИО ничего не нашли — всё равно отдадим одну запись «по умолчанию»
    if not result:
        result[fio] = dates

    return result

def get_shift_locations() -> list[str]:
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Актуальные локации на смену")
    all_locations = ws_col_values(ws, 1)  # весь столбец A
    real_locations = all_locations[2:]  # пропускаем первые две строки
    locations = [loc.strip() for loc in real_locations if loc.strip()]
    return sorted(locations, key=str.lower)

def input_registration(fio, username, address):
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    all_values = ws_get_all_values(ws)
    headers = all_values[0]
    ci, ct, cm = (headers.index(x) + 1 for x in ("Исполнители", "Тег", "Ближайшее метро"))

    # 1) обновляем существующую запись, если есть
    new_tag = f"@{username}"
    for row_idx, row in enumerate(all_values[1:], start=2):
        if row[ci-1].strip() == fio:
            updates = []
            if row[cm-1].strip() != address:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, cm),
                    "values": [[address]]
                })
            if row[ct-1].strip() != new_tag:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, ct),
                    "values": [[new_tag]]
                })
            if updates:
                ws_batch_update(ws, updates)
            return

    # 2) ищем внутри all_values первую свободную (A–C)
    for row_idx, row in enumerate(all_values[1:], start=2):
        if not row[ci-1].strip() and not row[ct-1].strip() and not row[cm-1].strip():
            ws.update(f"A{row_idx}:C{row_idx}", [[fio, new_tag, address]])
            return

    # 3) если свободных не было в all_values — используем следующую строку
    free_row = len(all_values) + 1
    ws_update(ws, f"A{free_row}:C{free_row}", [[fio, new_tag, address]])




def vvod_grafica(fio,username,picked_dates,picked_locs,location):
    # метка времени в нужном формате
    timestamp = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")

    for date_exit in picked_dates:
        row = [
            timestamp,  # Дата и время
            fio,  # ФИО
            username,  # Тег
            date_exit,  # Дата выхода
            location,  # Локация
            "Заполнен"
        ]
        ws_append_row(ws, row, value_input_option="USER_ENTERED")


def get_available_dates_for_fio_add_shift(fio: str) -> dict[str, list[str]]:
    """
    Возвращает словарь с двумя списками:
      - 'available': даты от сегодня до конца текущей половины месяца, которых нет в таблице
      - 'used':     даты из той же половины, которые уже есть в таблице
    Формат дат — 'DD.MM.YYYY'.
    """
    # 1) Загружаем все записи
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    records = ws_get_all_records(ws)

    # 2) Собираем все использованные даты для данного ФИО
    used_all: set[str] = set()
    for rec in records:
        if rec.get("ФИО") != fio or rec.get("Статус") != "Заполнен":
            continue
        raw = rec.get("Дата выхода", "").strip()
        try:
            d = datetime.strptime(raw, "%d.%m.%Y").date()
            used_all.add(d)
        except Exception:
            continue

    # 3) Определяем границы текущей «половины» по новой схеме 10–25 / 26–9
    today = date.today()
    half_start, half_end = _bounds_current_half(today)

    # 4) Из них выбираем только использованные в этой половине
    used_in_half = sorted(d for d in used_all if half_start <= d <= half_end)
    used = [d.strftime("%d.%m.%Y") for d in used_in_half]

    # 5) Генерируем все даты от сегодня до конца половины
    available: list[str] = []
    cur = today
    while cur <= half_end:
        if cur not in used_all:
            available.append(cur.strftime("%d.%m.%Y"))
        cur += timedelta(days=1)

    return {
        "available": available,
        "used": used
    }

def get_available_dates_for_fio_add_shift_full_half(fio: str) -> dict[str, list[str]]:
    """
    Возвращает словарь с двумя списками для текущей половины месяца:
      - 'available': даты от начала половины до её конца, которых нет в таблице
      - 'used':      даты из той же половины, которые уже есть в таблице
    Формат дат — 'DD.MM.YYYY'.

    Требуются те же зависимости и константы, что и в исходной функции:
    - gspread + creds.json
    - URL_GOOGLE_SHEETS_ANSWERS
    - from datetime import date, datetime, timedelta
    - import calendar
    """
    # 1) Загружаем все записи
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    records = ws_get_all_records(ws)

    # 2) Собираем все использованные даты для данного ФИО
    used_all: set[date] = set()
    for rec in records:
        if rec.get("ФИО") != fio or rec.get("Статус") != "Заполнен":
            continue
        raw = (rec.get("Дата выхода") or "").strip()
        try:
            d = datetime.strptime(raw, "%d.%m.%Y").date()
            used_all.add(d)
        except Exception:
            continue

    # 3) Определяем границы текущей «половины» месяца
    today = date.today()
    half_start, half_end = _bounds_current_half(today)

    # 4) Даты, уже занятые в этой половине
    used_in_half = sorted(d for d in used_all if half_start <= d <= half_end)
    used = [d.strftime("%d.%m.%Y") for d in used_in_half]

    # 5) Генерируем все даты от начала половины до её конца
    available: list[str] = []
    cur = half_start
    while cur <= half_end:
        if cur not in used_all:
            available.append(cur.strftime("%d.%m.%Y"))
        cur += timedelta(days=1)

    return {
        "available": available,
        "used": used
    }
    
def get_upcoming_shifts_for_fio(fio: str) -> list[tuple[str, str]]:
    """
    Возвращает список (дата, локация) для заданного ФИО из листа "График исполнителей",
    где дата >= сегодня, отсортированный по возрастанию даты.
    Формат даты — 'DD.MM.YYYY'.
    """
    # 1) Открываем гугл-таблицу и лист «График исполнителей»
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    all_rows = ws_get_all_values(ws)

    if not all_rows or len(all_rows) < 2:
        return []

    headers = all_rows[0]       # строка заголовков: [ "Исполнители", "...", "20.06.2025", "21.06.2025", ... ]
    # 3) Ищем строку с нашим fio в первом столбце
    target_row = None
    for row in all_rows[1:]:
        if row and row[0].strip() == fio:
            target_row = row
            break
    if target_row is None:
        return []

    today = date.today()
    result: list[tuple[date, str]] = []

    # 4) Пробегаем по столбцам начиная с третьего (индекс 2)
    for col_idx in range(2, len(headers)):
        date_str = headers[col_idx].strip()
        try:
            d = datetime.strptime(date_str, "%d.%m.%Y").date()
        except ValueError:
            # в заголовке не похоже на дату — пропускаем
            continue

        if d < today:
            continue

        # берём ячейку в найденной строке, если она есть
        loc = ""
        if col_idx < len(target_row):
            loc = target_row[col_idx].strip()
        if loc:
            result.append((d, loc))

    # 5) Сортируем по дате и возвращаем в нужном формате
    result.sort(key=lambda x: x[0])
    return [(d.strftime("%d.%m.%Y"), loc) for d, loc in result]

def fetch_dates_availability(date_list):
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    data = ws_get_all_values(ws)

    # нам обязательно нужны хотя бы первые две строки
    if len(data) < 2:
        return {d: False for d in date_list}

    date_headers = data[0]      # даты, например ["Адрес","Город","5.08.2025","16.08.2025",...]
    capacity_row = data[1]      # строки вида ["Свободных лотов на этот день.","", "25 (20 мест + 5 резерв)", ...]

    availability = {}
    for d in date_list:
        try:
            col = date_headers.index(d)
        except ValueError:
            # колонки с этой датой нет
            availability[d] = False
            continue

        # Парсим total из capacity_row[col]
        cap_cell = capacity_row[col]
        try:
            total = int(cap_cell.split()[0])
        except Exception:
            availability[d] = False
            continue

        # Считаем used
        used = 0
        for row in data[2:]:
            cell = row[col].strip() if col < len(row) else ""
            if cell != "":
                used += 1

        availability[d] = (used < total)

    return availability


def cancel_shift_in_sheet(fio: str, date_str: str, location: str):
    """
    Находит в листе "Ответы" запись по fio и дате со статусом "Заполнен"
    и меняет статус на "Отменен". Локацию больше не учитываем.
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    headers = ws_row_values(ws, 1)
    records = ws_get_all_records(ws)

    try:
        status_col = headers.index("Статус") + 1
    except ValueError:
        return

    for idx, rec in enumerate(records, start=2):
        if (rec.get("ФИО") or "").strip() != fio:
            continue
        if (rec.get("Дата выхода") or "").strip() != date_str:
            continue
        if (rec.get("Статус") or "").strip() != "Заполнен":
            continue
        ws_update_cell(ws, idx, status_col, "Отменен")


def get_shifts_grouped_by_week(fio: str) -> dict[tuple[int,int], int]:
    from collections import defaultdict

    all_shifts = get_all_shifts_for_fio(fio)
    counts = defaultdict(int)
    for d, _ in all_shifts:
        year, week, _ = d.isocalendar()
        counts[(year, week)] += 1
    return counts

def weeks_below_minimum(fio: str, minimum: int = 3) -> list[tuple[int,int]]:
    counts = get_shifts_grouped_by_week(fio)
    return [(y,w) for (y,w), cnt in counts.items() if cnt < minimum]

def get_all_shifts_for_fio(fio: str) -> list[tuple[date, str]]:
    """
    Возвращает список (дата, локация) для всех записей
    из листа "График исполнителей" для fio, без учёта сегодняшней даты.
    """
    gc = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(URL_GOOGLE_SHEETS_ANSWERS)
    ws = sh.worksheet("График исполнителей")

    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []

    headers = all_rows[0]
    # Ищем строку с нашим fio
    target_row = next((r for r in all_rows[1:] if r and r[0].strip() == fio), None)
    if not target_row:
        return []

    result: list[tuple[date, str]] = []
    for col_idx in range(2, len(headers)):
        date_str = headers[col_idx].strip()
        try:
            d = datetime.strptime(date_str, "%d.%m.%Y").date()
        except ValueError:
            continue

        loc = target_row[col_idx].strip() if col_idx < len(target_row) else ""
        if loc:
            result.append((d, loc))

    return result

_DT_FORMATS = ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")

def _parse_dt_ru(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def find_logistics_rows(limit_hours: int = 12) -> tuple[list[str], list[str]]:
    ws = _open_ws(URL_GOOGLE_SHEETS_CHART, "Логисты выход на смену")
    rows = ws_get_all_values(ws)
    if not rows:
        return [], []
    header, data = rows[0], rows[1:]
    def col_idx(name: str) -> int:
        try:
            return [h.strip().lower() for h in header].index(name.lower())
        except ValueError:
            return -1

    i_fio   = col_idx("ФИО")
    i_tag   = col_idx("Тег")
    i_dir   = col_idx("Направление")
    i_start = col_idx("Время начала смены")
    i_end   = col_idx("Время конца смены")

    if min(i_fio, i_tag, i_dir, i_start, i_end) < 0:
        return [], []

    now = datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)  # сравниваем как наивные в МСК
    window = timedelta(hours=limit_hours)

    tags: list[str] = []
    fios: list[str] = []

    for row in data:
        # Защита от коротких строк
        if max(i_fio, i_tag, i_dir, i_start, i_end) >= len(row):
            continue

        direction = row[i_dir].strip()
        if direction != "Мойка":
            continue

        end_time_raw = row[i_end].strip()
        if end_time_raw:  # конец смены уже заполнен
            continue

        start_dt = _parse_dt_ru(row[i_start])
        if not start_dt:
            continue

        # Начало смены не старше limit_hours от текущего времени
        if now - start_dt <= window and now >= start_dt:
            tags.append(row[i_tag].strip())
            fios.append(row[i_fio].strip())

    return tags, fios

def get_max_report_number() -> int:
    gc = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(URL_GOOGLE_SHEETS_PROBLEM)
    ws = sh.worksheet("Мойка")

    all_values = ws.get_all_values()
    if not all_values:
        return 0

    header = all_values[0]
    if "Номер отчета" not in header:
        raise ValueError("Столбец 'Номер отчета' не найден в заголовке")

    report_idx = header.index("Номер отчета")
    max_report = 0

    # Обрабатываем все строки, начиная со второй (данные)
    for row in all_values[1:]:
        if len(row) <= report_idx:
            continue
        cell_value = row[report_idx].strip()
        if not cell_value:
            continue
        # Убираем символ '#' если он есть
        if cell_value.startswith("#"):
            cell_value = cell_value[1:]
        try:
            num = int(cell_value)
        except ValueError:
            continue
        if num > max_report:
            max_report = num

    return max_report

def write_problem_report(report_number, company, direction, fio, vehicle_number, address, problem_type,
                         problem_subtype, description, message_link):
    ws = _open_ws(URL_GOOGLE_SHEETS_PROBLEM, "Мойка")
    data = [
        "",
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        report_number,
        company,
        direction,
        fio,
        vehicle_number,
        address,
        problem_type,
        problem_subtype,
        description,
        message_link
    ]
    ws_append_row(ws, data, value_input_option='USER_ENTERED', table_range = 'A1', insert_data_option = 'INSERT_ROWS')

def write_problem_report_new(
        report_number,
        company,
        direction,
        fio,
        vehicle_number,
        address,
        problem_type,
        problem_subtype,
        description,
        message_link
):

    # Открываем таблицу
    ws = _open_ws(URL_GOOGLE_SHEETS_PROBLEM_NEW, "Мойка")

    data = [
        "",
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        report_number,
        company,
        direction,
        fio,
        vehicle_number,
        address,
        problem_type,
        problem_subtype,
        description,
        message_link
    ]

    ws_append_row( ws, data, value_input_option='USER_ENTERED', table_range='A1', insert_data_option='INSERT_ROWS')


def find_all_technician_rows(date_str: str, shift: str):
    """
    Ищет **все** строки в таблице "График", где:
      - Столбец "Роль" равен "техник" или "стажер" (предполагается, что "Роль" всегда в первом столбце)
      - Столбец "Время суток" соответствует shift ("День" или "Ночь")
      - Значение в колонке с заголовком date_str (из второй строки) не пустое
    Возвращает список кортежей: [(GRZ, ТГ тег, ФИО/Компания), ...]
    Если строк не найдено, возвращает пустой список.
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_CHART, "График")
    data = ws_get_all_values(ws)

    # Первая строка — статические заголовки, вторая — заголовки дат.
    static_headers = data[0]
    date_headers = data[1]

    role_idx = 0  # "Роль" всегда в первом столбце
    timeslot_idx = 2 # "Время суток" всегда в третьем столбце

    try:
        tg_tag_idx = static_headers.index("ТГ тег")
    except ValueError:
        tg_tag_idx = None

    try:
        fio_idx = static_headers.index("ФИО/Компания")
    except ValueError:
        fio_idx = None

    try:
        date_idx = date_headers.index(date_str)
    except ValueError:
        return []

    results = []
    # Начинаем обработку с третьей строки, так как первые две строки — заголовки.
    for i, row in enumerate(data[2:], start=3):
        role = row[role_idx].strip().lower() if len(row) > role_idx else ""
        if role not in ["техник", "стажер"]:
            continue
        if timeslot_idx is not None and len(row) > timeslot_idx:
            timeslot = row[timeslot_idx].strip().lower()
            if timeslot != shift.lower():
                continue
        if len(row) <= date_idx:
            continue
        grz = row[date_idx].strip()
        if not grz:
            continue
        tg_tag = row[tg_tag_idx].strip() if tg_tag_idx is not None and len(row) > tg_tag_idx else ""
        fio = row[fio_idx].strip() if fio_idx is not None and len(row) > fio_idx else ""
        results.append((grz, tg_tag, fio))
    return results

def get_performers_with_tags_for_date(target: date) -> list[tuple[str, str]]:
    """
    Возвращает список (fio, tag) для исполнителей на дату target.
    - Заголовки на 1-й строке.
    - Строки 2 и 3 пропускаем.
    - Возвращает только уникальные пары (fio, tag).
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    headers = ws_row_values(ws, 1)  # заголовки в 1-й строке
    date_str = target.strftime("%d.%m.%Y")

    try:
        headers.index("Исполнители")
        headers.index("Тег")
        headers.index(date_str)
    except ValueError:
        return []

    # читаем по заголовкам со 2-й строки и отбрасываем 2 и 3 строки (индексы 0 и 1)
    records = ws.get_all_records(head=1)
    sched = records[2:]  # <- тут и есть «начиная с 4-й строки»

    unique_pairs: set[tuple[str, str]] = set()

    for rec in sched:
        fio_val = rec.get("Исполнители", "")
        fio = fio_val.strip() if isinstance(fio_val, str) else ""
        if not fio:
            continue

        cell_val = rec.get(date_str, "")
        cell_filled = (
            (isinstance(cell_val, str) and cell_val.strip() != "")
            or (isinstance(cell_val, (int, float)) and cell_val != 0)
        )
        if not cell_filled:
            continue

        tag_val = rec.get("Тег", "")
        tag = tag_val.strip() if isinstance(tag_val, str) else ""
        if tag.startswith("@"):
            tag = tag[1:]

        unique_pairs.add((shorten_name(fio), tag))

    return list(unique_pairs)


def cancel_in_answers(fio: str):
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    header = ws_row_values(ws, 1)
    try:
        fio_col = header.index("ФИО") + 1
        status_col = header.index("Статус") + 1
    except ValueError:
        raise RuntimeError("В листе 'Ответы' нет колонки 'ФИО' или 'Статус'")
    # находим все совпадения ФИО и ставим "Отменен"
    for cell in ws.findall(fio, in_column=fio_col):
        ws.update_cell(cell.row, status_col, "Отменен")

def get_performers_for_date(target: date) -> list[tuple[str, str]]:
    """
    Возвращает список (фио, тег) из листа "График исполнителей" для даты target.
    Ищет в шапке колонку с датой в формате 'DD.MM.YYYY', а затем берёт
    всех исполнителей и их теги из строк, где в этой колонке непусто.
    """
    # 1) Открываем Google-таблицу и лист "График исполнителей"
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    headers = ws_row_values(ws, 1)
    date_str = target.strftime("%d.%m.%Y")

    # 3) Находим индекс колонки с нужной датой
    try:
        date_col = headers.index(date_str) + 1  # Google API — 1-based
    except ValueError:
        # Если колонки с датой нет — возвращаем пустой список
        return []

    # 4) Находим индексы колонок "Исполнители" и "Тег"
    try:
        fio_col = headers.index("Исполнители") + 1
    except ValueError:
        raise RuntimeError("В шапке нет колонки 'Исполнители'")
    try:
        tag_col = headers.index("Тег") + 1
    except ValueError:
        raise RuntimeError("В шапке нет колонки 'Тег'")

    # 5) Считываем все данные (начиная со второй строки)
    rows = ws.get_all_values()[1:]

    # 6) Собираем фио и теги, где в колонке date_col есть непустое значение
    out: list[tuple[str, str]] = []
    for row in rows:
        # проверяем, что в этой строке есть значение даты
        if len(row) >= date_col and row[date_col - 1].strip():
            fio = row[fio_col - 1].strip()
            tag = row[tag_col - 1].strip()
            if fio and tag:
                out.append((fio, tag))

    return out

def _read_assignments_for_date(target: date) -> list[tuple[str, str]]:
    """
    Читает лист «Ответы» и возвращает [(fio, address), ...] только для target.
    Ожидаемый формат листа «Ответы»:
        Дата и время | ФИО | Тег | Дата выхода | Локация | Статус
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    rows = ws_get_all_values(ws)
    if not rows:
        return []

    header = [h.strip() for h in rows[0]]
    # Пытаемся найти индексы по названиям; если не нашли — используем «как записываем» в vvod_grafica
    try:
        i_fio = header.index("ФИО")
    except ValueError:
        i_fio = 1
    try:
        i_date = header.index("Дата выхода")
    except ValueError:
        i_date = 3
    try:
        i_loc = header.index("Локация")
    except ValueError:
        i_loc = 4

    target_str = target.strftime("%d.%m.%Y")
    result: list[tuple[str, str]] = []
    for row in rows[1:]:
        if len(row) <= max(i_fio, i_date, i_loc):
            continue
        if row[i_date].strip() != target_str:
            continue
        fio = row[i_fio].strip()
        loc = row[i_loc].strip()
        if fio and loc:
            result.append((fio, loc))
    return result

def log_late_cancel_to_sheet(fio: str, date_str: str, loc: str, username: str) -> None:
    """
    Логирует отмену смены менее чем за 24 часа в лист "Отмена смены менее 24ч".
    Формат: [Дата и время фиксации, ФИО, Тег, Дата выхода, Локация, Статус]
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Отмена смены менее 24ч")
    when = datetime.now().strftime("%d.%m.%Y %H:%M")
    tg = f"@{username}" if username else ""
    status = "Отмена < 24ч"

    ws_append_row(ws, [when, fio, tg, date_str, loc, status], value_input_option="USER_ENTERED")


def read_today_locations_with_capacity() -> dict[str, int]:
    """
    Читает лист 'Актуальные локации на смене' и возвращает словарь
    {адрес -> доступные_места} на СЕГОДНЯ.
    Схемы таблиц у всех разные, поэтому делаем максимально устойчивый парсер:
    - ищем в ПЕРВОЙ строке ячейку с заголовком сегодняшней даты 'DD.MM.YYYY' (колонка date_col)
    - адрес берём из первого столбца (A), количество — из найденной колонки date_col
    - пропускаем пустые/нечисловые значения
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Актуальные локации на смену")
    values = ws_get_all_values(ws)

    if not values:
        return {}

    today_str = date.today().strftime("%d.%m.%Y")

    header = [c.strip() for c in values[0]]
    try:
        date_col = header.index(today_str)
    except ValueError:
        # fallback: попробуем ещё во второй строке (бывает “двойная шапка”)
        if len(values) > 1:
            subheader = [c.strip() for c in values[1]]
            date_col = subheader.index(today_str)  # если не найдём — пусть кинет ValueError
        else:
            raise

    start_row = 1 if header and header[0] != "Адрес" else 1  # гибко: всё равно переберём со второй строки
    out: dict[str, int] = {}
    for row in values[1:]:
        if not row or len(row) <= date_col:
            continue
        address = (row[0] or "").strip()
        capacity_cell = (row[date_col] or "").strip()
        if not address or not capacity_cell:
            continue
        # из ячейки под сегодняшней датой пробуем достать число (первые цифры до пробела/скобок)
        num = None
        token = ""
        for ch in capacity_cell:
            if ch.isdigit():
                token += ch
            else:
                break
        if token:
            try:
                num = int(token)
            except:
                num = None
        if num is not None and num > 0:
            out[address] = num
    return out


def update_location_for_user_today_by_tag(user_tag: str, new_location: str) -> bool:
    """
    Ищет в листе 'Ответы' строку по:
      - 'Тег' == user_tag (без @, регистронезависимо)
      - 'Дата выхода' == сегодня
      - 'Статус' == 'Заполнен'
    и обновляет столбец 'Локация' на new_location.
    Возвращает True, если обновили запись.
    """
    tag_norm = (user_tag or "").strip().lstrip("@").lower()
    today = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")

    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    rows = ws_get_all_values(ws)
    if not rows:
        return False
    header = rows[0]

    def col_idx(name: str, default=None):
        try:
            return header.index(name)
        except ValueError:
            return default

    i_tag = col_idx("Тег", 2)          # по умолчанию как в vvod_grafica
    i_date = col_idx("Дата выхода", 3)
    i_status = col_idx("Статус", 5)
    i_loc = col_idx("Локация", 4)

    if None in (i_tag, i_date, i_status, i_loc):
        return False

    # 1-based индексы для update_cell
    i_loc_1b = i_loc + 1

    for r_idx, row in enumerate(rows[1:], start=2):
        raw_tag = (row[i_tag] if len(row) > i_tag else "").strip()
        tag_val = raw_tag.lstrip("@").lower()
        date_val = (row[i_date] if len(row) > i_date else "").strip()
        status_val = (row[i_status] if len(row) > i_status else "").strip()


        if tag_val == tag_norm and date_val == today and status_val == "Заполнен":
            ws_update_cell(ws, r_idx, i_loc_1b, new_location)
            return True
    return False

def fetch_best_and_good_ids_for_today_with_threshold() -> tuple[list[int], list[int], float]:
    """
    Считывает лист «График исполнителей»:
      - берёт порог рейтинга из второй строки столбца «Рейтинг» (формат "13,5" поддерживается)
      - находит колонку сегодняшней даты
      - выбирает строки, где в колонке с сегодняшней датой непусто (исполнитель стоит на смене)
      - по 'Тег' (без '@') бьёт в API и получает chat_id
      - делит на BEST/GOOD по считанному порогу

    Возвращает (best_ids, good_ids, threshold).
    """

    def _parse_float(s) -> float:
        try:
            return float(str(s).replace(",", "."))
        except Exception:
            return 0.0

    # 0) Дата и коннект
    today_str = date.today().strftime("%d.%m.%Y")

    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    data = ws_get_all_values(ws)

    if not data or len(data) < 2:
        return [], [], 13.0  # дефолт

    headers = [h.strip() for h in data[0]]

    # индексы базовых колонок
    try:
        idx_fio = headers.index("Исполнители")
    except ValueError:
        idx_fio = None
    try:
        idx_tag = headers.index("Тег")
    except ValueError:
        idx_tag = None
    try:
        idx_rating = headers.index("Рейтинг")
    except ValueError:
        idx_rating = None

    # индекс колонки сегодняшней даты
    try:
        idx_today = headers.index(today_str)
    except ValueError:
        return [], [], 13.0

    # 1) Порог из второй строки столбца «Рейтинг»
    threshold = 13.0
    if idx_rating is not None and len(data) >= 2 and len(data[1]) > idx_rating:
        threshold = _parse_float(data[1][idx_rating])

    best_ids: list[int] = []
    good_ids: list[int] = []
    seen: set[int] = set()

    # 2) Идём по строкам, начиная с третьей (0 — заголовки, 1 — «свободных лотов…»)
    for row in data[2:]:
        if not row:
            continue
        # есть ли отметка в колонке сегодняшней даты?
        if len(row) <= idx_today or not str(row[idx_today]).strip():
            continue

        # тэг и рейтинг
        tag_val = (row[idx_tag] if idx_tag is not None and len(row) > idx_tag else "").strip()
        if not tag_val:
            continue
        username = tag_val.lstrip("@")

        rating_val = (row[idx_rating] if idx_rating is not None and len(row) > idx_rating else "")
        rating = _parse_float(rating_val)

        # получаем chat_id по API
        try:
            resp = requests.get(URL_GET_USER_ID_BY_FIO, params={"tg_username": username}, timeout=7)
            resp.raise_for_status()
            payload = resp.json() or {}
            raw_id = payload.get("id") or payload.get("chat_id")
            uid = int(raw_id)
        except Exception:
            continue

        if uid in seen:
            continue
        seen.add(uid)

        if rating > threshold:
            best_ids.append(uid)
        else:
            good_ids.append(uid)

    return best_ids, good_ids, threshold

def read_today_locations_with_capacity_and_district() -> dict[str, tuple[str, int]]:
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Актуальные локации на смену")
    values = ws_get_all_values(ws)
    if not values:
        return {}

    today_str = date.today().strftime("%d.%m.%Y")
    header = [c.strip() for c in values[0]]

    # ищем колонку сегодняшней даты в 1-й или 2-й строке
    date_col = None
    for head_row in (0, 1):
        if head_row >= len(values):
            break
        try:
            date_col = [c.strip() for c in values[head_row]].index(today_str)
            break
        except ValueError:
            continue
    if date_col is None:
        return {}

    # ищем колонку "Округ" (в 1-й или 2-й строке)
    okrug_col = None
    for head_row in (0, 1):
        if head_row >= len(values):
            break
        row = [c.strip() for c in values[head_row]]
        try:
            okrug_col = row.index("Округ")
            break
        except ValueError:
            continue

    out: dict[str, tuple[str, int]] = {}
    for row in values[1:]:
        if not row or len(row) <= date_col:
            continue
        address = (row[0] or "").strip()
        if not address:
            continue

        okrug = (row[okrug_col].strip() if okrug_col is not None and len(row) > okrug_col else "")

        cap_cell = (row[date_col] or "").strip()
        token = ""
        for ch in cap_cell:
            if ch.isdigit():
                token += ch
            else:
                break
        if not token:
            continue
        left = int(token)
        if left > 0:
            out[address] = (okrug, left)
    return out

def performers_for_today_flag1_with_district() -> list[tuple[str, str, str]]:
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    data = ws_get_all_values(ws)

    if not data or len(data) < 2:
        return []

    headers = [h.strip() for h in data[0]]
    today_str = date.today().strftime("%d.%m.%Y")

    try:
        idx_fio   = headers.index("Исполнители")
        idx_tag   = headers.index("Тег")
        idx_today = headers.index(today_str)
    except ValueError:
        return []
    try:
        idx_okrug = headers.index("Округ")
    except ValueError:
        idx_okrug = None

    out: list[tuple[str, str, str]] = []
    for row in data[2:]:  # с 3-й строки, 2-я — служебная
        if len(row) <= idx_today:
            continue
        if (row[idx_today] or "").strip() != "1":
            continue
        fio = (row[idx_fio] or "").strip()
        if not fio:
            continue
        username = ((row[idx_tag] or "").strip().lstrip("@") if len(row) > idx_tag else "")
        okrug = ((row[idx_okrug] or "").strip() if idx_okrug is not None and len(row) > idx_okrug else "")
        out.append((fio, username, okrug))
    return out

def update_location_for_user_today_by_tag_or_fio(user_tag: str, new_location: str, fio_fallback: str = "") -> bool:
    """Сначала ищем по тегу (как сейчас). Если не нашли — ищем по ФИО за сегодня со статусом 'Заполнен';
    при успехе обновляем и 'Локацию', и 'Тег' в найденной строке на актуальный."""
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    rows = ws_get_all_values(ws)

    if not rows:
        return False
    header = rows[0]
    def col_idx(name: str, default=None):
        try: return header.index(name)
        except ValueError: return default
    i_tag   = col_idx("Тег", 2)
    i_date  = col_idx("Дата выхода", 3)
    i_status= col_idx("Статус", 5)
    i_loc   = col_idx("Локация", 4)
    i_fio   = col_idx("ФИО", 1)
    if None in (i_tag, i_date, i_status, i_loc, i_fio):
        return False
    today = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")
    tag_norm = (user_tag or "").strip().lstrip("@").lower()
    # 1) обычный путь — по тегу
    for r_idx, row in enumerate(rows[1:], start=2):
        tag_val   = (row[i_tag] if len(row) > i_tag else "").strip().lstrip("@").lower()
        date_val  = (row[i_date] if len(row) > i_date else "").strip()
        status_val= (row[i_status] if len(row) > i_status else "").strip()
        if tag_val == tag_norm and date_val == today and status_val == "Заполнен":
            ws_update_cell(ws, r_idx, i_loc + 1, new_location)
            return True
    # 2) фолбэк — по ФИО
    fio_norm = (fio_fallback or "").strip()
    if not fio_norm:
        return False
    for r_idx, row in enumerate(rows[1:], start=2):
        fio_val   = (row[i_fio] if len(row) > i_fio else "").strip()
        date_val  = (row[i_date] if len(row) > i_date else "").strip()
        status_val= (row[i_status] if len(row) > i_status else "").strip()
        if fio_val == fio_norm and date_val == today and status_val == "Заполнен":
            updates = [
                {"range": gspread.utils.rowcol_to_a1(r_idx, i_loc + 1), "values": [[new_location]]},
                {"range": gspread.utils.rowcol_to_a1(r_idx, i_tag + 1), "values": [[f"@{tag_norm}"]]},
            ]
            ws_batch_update(ws, updates)
            return True
    return False

from typing import Optional

def find_answers_row_for_today_by_tag_or_fio(user_tag: str, fio_fallback: str = "") -> Optional[int]:
    """
    Ищет в листе 'Ответы' СЕГОДНЯШНЮЮ запись исполнителя и возвращает
    1-based индекс строки. Приоритет: по тегу (без @), иначе по ФИО.
    Если несколько строк, берём ту, где 'Локация' пуста, иначе первую подходящую.
    """
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    rows = ws_get_all_values(ws)
    if not rows:
        return None

    header = [h.strip() for h in rows[0]]
    def col_idx(name: str, default=None):
        try: return header.index(name)
        except ValueError: return default

    i_tag   = col_idx("Тег", 2)
    i_date  = col_idx("Дата выхода", 3)
    i_status= col_idx("Статус", 5)
    i_loc   = col_idx("Локация", 4)
    i_fio   = col_idx("ФИО", 1)
    if None in (i_tag, i_date, i_status, i_loc, i_fio):
        return None

    today = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")
    tag_norm = (user_tag or "").strip().lstrip("@").lower()
    fio_norm = (fio_fallback or "").strip()

    candidates = []
    for r_idx, row in enumerate(rows[1:], start=2):
        date_val   = (row[i_date]   if len(row) > i_date   else "").strip()
        status_val = (row[i_status] if len(row) > i_status else "").strip()
        if date_val != today or status_val != "Заполнен":
            continue
        raw_tag = (row[i_tag] if len(row) > i_tag else "").strip().lstrip("@").lower()
        raw_fio = (row[i_fio] if len(row) > i_fio else "").strip()
        if tag_norm and raw_tag == tag_norm:
            candidates.append((r_idx, (row[i_loc] if len(row) > i_loc else "").strip()))
            continue
        if fio_norm and raw_fio == fio_norm:
            candidates.append((r_idx, (row[i_loc] if len(row) > i_loc else "").strip()))

    if not candidates:
        return None
    # Сначала те, где локация пустая, затем остальные
    candidates.sort(key=lambda x: (x[1] != "", x[0]))
    return candidates[0][0]

def update_location_in_answers_by_row(row_idx: int, new_location: str) -> bool:
    """
    Обновляет 'Локация' в листе 'Ответы' по конкретному индексу строки (1-based).
    """
    if not row_idx or row_idx < 2:
        return False
    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    header = [h.strip() for h in ws_row_values(ws, 1)]
    try:
        i_loc = header.index("Локация") + 1
    except ValueError:
        i_loc = 5  # как в vvod_grafica по умолчанию
    ws_update_cell(ws, row_idx, i_loc, new_location)
    return True

from typing import Optional, Tuple
def find_today_assignment_row_and_loc_by_tag(user_tag: str) -> Tuple[Optional[int], str]:
    """
    Возвращает (row_idx, current_location) для листа 'Ответы' на СЕГОДНЯ
    по тегу (без @), где Статус == 'Заполнен'. Если не найдено — (None, "").
    Если несколько, берём первую, у которой 'Локация' непустая, иначе просто первую.
    """

    tag_norm = (user_tag or "").strip().lstrip("@").lower()
    if not tag_norm:
        return None, ""

    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "Ответы")
    rows = ws_get_all_values(ws)
    if not rows:
        return None, ""

    hdr = [h.strip() for h in rows[0]]
    def idx(name, default=None):
        try: return hdr.index(name)
        except ValueError: return default
    i_tag = idx("Тег", 2); i_date = idx("Дата выхода", 3)
    i_status = idx("Статус", 5); i_loc = idx("Локация", 4)
    if None in (i_tag, i_date, i_status, i_loc):
        return None, ""

    today = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")
    candidates = []
    for r_idx, row in enumerate(rows[1:], start=2):
        raw_tag = (row[i_tag] if len(row) > i_tag else "").strip().lstrip("@").lower()
        if raw_tag != tag_norm:
            continue
        if (row[i_date] if len(row) > i_date else "").strip() != today:
            continue
        if (row[i_status] if len(row) > i_status else "").strip() != "Заполнен":
            continue
        loc = (row[i_loc] if len(row) > i_loc else "").strip()
        candidates.append((r_idx, loc))
    if not candidates:
        return None, ""
    # предпочитаем те, где локация непуста
    candidates.sort(key=lambda x: (x[1] == "", x[0]))
    return candidates[0]

def tag_exists_in_schedule(user_tag: str) -> bool:
    """
    Проверяет, есть ли в листе 'График исполнителей' совпадение по тегу.
    user_tag может быть с '@' или без — функция сама нормализует.
    Возвращает True/False.
    """
    tag_norm = (user_tag or "").strip().lstrip("@").lower()
    if not tag_norm:
        return False

    ws = _open_ws(URL_GOOGLE_SHEETS_ANSWERS, "График исполнителей")
    rows = ws_get_all_values(ws) or []
    if not rows:
        return False

    header = [h.strip() for h in rows[0]]
    try:
        i_tag = header.index("Тег")
    except ValueError:
        return False

    for row in rows[1:]:
        if len(row) <= i_tag:
            continue
        raw_tag = (row[i_tag] or "").strip().lstrip("@").lower()
        if raw_tag == tag_norm:
            return True

    return False