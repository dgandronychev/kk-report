# -*- coding: utf-8 -*-
import asyncio
import gspread
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any, Optional

from aiogram import types
from gspread import Client, Spreadsheet, Worksheet

from app.config import (
    gspread_url,
    gspread_url_answer,
    gspread_url_sklad,
    gspread_url_shift,
    gspread_url_gates,
    URL_GOOGLE_SHEETS_CHART,
)

# ────────────────────────────── Константы ──────────────────────────────
_READ_TTL_SEC = 60  # время жизни кэша чтений (сек)

# ────────────────────────────── Инициализация ──────────────────────────
# Один раз инициализируем клиента и книги
gc: Client = gspread.service_account("app/creds.json")
SH_MAIN: Spreadsheet = gc.open_by_url(gspread_url)
SH_ANS: Spreadsheet = gc.open_by_url(gspread_url_answer)
SH_SKL: Spreadsheet = gc.open_by_url(gspread_url_sklad)
SH_SHIFT: Spreadsheet = gc.open_by_url(gspread_url_shift)
SH_GATES: Spreadsheet = gc.open_by_url(gspread_url_gates)
SH_LOG: Spreadsheet = gc.open_by_url(URL_GOOGLE_SHEETS_CHART)

# Для обратной совместимости с вашим кодом
sh: Spreadsheet = SH_MAIN

# ────────────────────────────── Кэши ──────────────────────────────
_ws_cache: Dict[Tuple[str, str], Worksheet] = {}
# ключ -> (timestamp, data)
_read_cache: Dict[Tuple[Any, ...], Tuple[float, List[List[str]]]] = {}
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

def _get_ws(spreadsheet: Spreadsheet, title: str) -> Worksheet:
    """Кэшируем объекты Worksheet, чтобы не делать повторные запросы метаданных."""
    key = (spreadsheet.id, title)
    ws = _ws_cache.get(key)
    if ws is None:
        ws = spreadsheet.worksheet(title)
        _ws_cache[key] = ws
    return ws

def _cache_get(key: Tuple[Any, ...], ttl_sec: float) -> Optional[List[List[str]]]:
    item = _read_cache.get(key)
    if not item:
        return None
    ts, data = item
    if time.time() - ts <= ttl_sec:
        return data
    _read_cache.pop(key, None)
    return None

def _cache_set(key: Tuple[Any, ...], data: List[List[str]]) -> None:
    _read_cache[key] = (time.time(), data)

def _cache_invalidate(prefix: Tuple[Any, ...]) -> None:
    """Инвалидация всех ключей чтений, начинающихся с prefix."""
    for k in list(_read_cache.keys()):
        if k[: len(prefix)] == prefix:
            _read_cache.pop(k, None)

def _load_sheet_cached(
    spreadsheet: Spreadsheet,
    title: str,
    ttl_sec: int = _READ_TTL_SEC,
    rng: Optional[str] = None,
    drop_header: bool = True,
) -> List[List[str]]:
    """
    Чтение листа с кэшем. По умолчанию отбрасывает заголовок (первая строка).
    """
    key = ("get", spreadsheet.id, title, rng or "ALL", "nohdr" if drop_header else "withhdr")
    cached = _cache_get(key, ttl_sec)
    if cached is not None:
        return cached
    ws = _get_ws(spreadsheet, title)
    values = ws.get(rng) if rng else ws.get_all_values()
    data = values[1:] if (values and drop_header) else (values or [])
    _cache_set(key, data)
    return data

# ────────────────────────────── Публичные API ──────────────────────────

# Эти глобальные списки вы используете далее в коде
list_rez_st: List[List[str]] = []
list_rez_ya: List[List[str]] = []
list_rez_blk: List[List[str]] = []

async def update_all_sheets():
    """
    Прогреваем кэш справочников.
    TTL задан глобально (_READ_TTL_SEC = 60).
    """
    titles = ["Резина Сити", "Резина ЯД", "Резина Белка"]
    loop = asyncio.get_running_loop()

    def _bulk_load():
        return {
            t: _load_sheet_cached(SH_MAIN, t, ttl_sec=_READ_TTL_SEC, rng=None, drop_header=True)
            for t in titles
        }

    data = await loop.run_in_executor(None, _bulk_load)
    global list_rez_st, list_rez_ya, list_rez_blk
    list_rez_st = data["Резина Сити"]
    list_rez_ya = data["Резина ЯД"]
    list_rez_blk = data["Резина Белка"]

def _load_sheet(spreadsheet: Spreadsheet, title: str) -> List[List[str]]:
    """Оставлено для совместимости — теперь просто использует кэш."""
    return _load_sheet_cached(spreadsheet, title, ttl_sec=_READ_TTL_SEC)

# ────────────────────────────── ФУНКЦИИ СКЛАДА ─────────────────────────

def get_max_nomer_sborka():
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    list_of_lists = ws_answer.get_all_values()[1:]  # тут нужен реальный актуальный список
    tlist: List[int] = []
    for record in list_of_lists:
        if len(record) > 13 and record[13]:
            str_nomer = re.sub(r"[^0-9]", "", record[13])
            if str_nomer:
                tlist.append(int(str_nomer))
    return max(tlist) if tlist else 0

def write_soberi_in_google_sheets_rows(tlist: List[List[Any]]):
    ws = _get_ws(SH_SKL, "Заявка на сборку")
    ws.append_rows(
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )
    _cache_invalidate(("get", SH_SKL.id, "Заявка на сборку"))

def loading_bz_znaniya(company: str) -> List[List[str]]:
    return _load_sheet_cached(SH_MAIN, company, ttl_sec=_READ_TTL_SEC, rng=None, drop_header=True)

def get_number_util(company: str, nomer: int) -> str:
    ws_answer = _get_ws(SH_ANS, "Выгрузка ремонты/утиль")
    list_of_lists = ws_answer.get_all_values()[1:]  # требуются актуальные значения
    list_number: List[str] = []
    for tlist in list_of_lists:
        if len(tlist) > nomer and tlist[nomer]:
            list_number.append(tlist[nomer])

    if company == "СитиДрайв":
        filtered = [s for s in list_number if s.startswith("su")]
        prefix = "su"
    elif company == "Яндекс":
        filtered = [s for s in list_number if s.startswith("yu")]
        prefix = "yu"
    else:
        filtered = [s for s in list_number if s.startswith("blk")]
        prefix = "blk"

    nums = [int(re.sub(r"[^0-9]", "", x)) for x in filtered if re.sub(r"[^0-9]", "", x)]
    next_num = (max(nums) + 1) if nums else 1
    return f"{prefix}{next_num}"

def write_in_answers_ras_shift(tlist, name_sheet) -> str:
    """
    Записывает строку в Google Sheets.
    Возвращает строку с длительностью смены при action="Окончание смены", иначе пустую строку.
    """
    ws = _get_ws(SH_SHIFT, name_sheet)

    dt_str, fio, action, username, message_link = tlist
    # Парсим дату
    dt_end = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
        try:
            dt_end = datetime.strptime(dt_str, fmt)
            break
        except ValueError:
            continue
    if dt_end is None:
        raise ValueError(f"Не удалось распарсить дату: '{dt_str}'")

    duration = ""
    if action == "Окончание смены":
        all_rows = ws.get_all_values()  # ожидаем свежие данные
        for row in reversed(all_rows):
            if len(row) >= 3 and row[1] == fio and row[2] == "Начало смены":
                start_dt = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
                    try:
                        start_dt = datetime.strptime(row[0].lstrip("'"), fmt)
                        break
                    except ValueError:
                        continue
                if start_dt is None:
                    continue
                delta = (dt_end - start_dt) if dt_end >= start_dt else (dt_end + timedelta(days=1) - start_dt)
                if delta < timedelta(hours=24):
                    h, rem = divmod(int(delta.total_seconds()), 3600)
                    m, s = divmod(rem, 60)
                    duration = f"{h:02d}:{m:02d}:{s:02d}"
                else:
                    duration = "Нет данных"
                break

    # Форматируем дату как текст с апострофом
    text_date = dt_end.strftime("%d.%m.%Y %H:%M:%S")
    row_to_append = [f"'{text_date}", fio, action, username, message_link, duration or "Нет данных"]
    ws.append_row(row_to_append, value_input_option="USER_ENTERED", insert_data_option="INSERT_ROWS")
    _cache_invalidate(("get", SH_SHIFT.id, name_sheet))
    return duration

def write_in_answers_ras(tlist, name_sheet):
    ws = _get_ws(SH_ANS, name_sheet)
    ws.append_row(
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )
    _cache_invalidate(("get", SH_ANS.id, name_sheet))

from datetime import datetime, timedelta  # у тебя уже есть наверху

# ...

def write_open_gate_row(fio: str, car_plate: str, company: str, message_link: str) -> None:
    ws = _get_ws(SH_GATES, "Выгрузка Техники")
    now_msk = datetime.now() + timedelta(hours=3)
    row = [
        now_msk.strftime("%d.%m.%Y"),   # Дата
        now_msk.strftime("%H:%M:%S"),   # Время
        fio,
        car_plate,
        company,
        message_link,
    ]
    ws.append_row(row, value_input_option="USER_ENTERED", table_range="A1", insert_data_option="INSERT_ROWS")

def write_in_answers_ras_nomen(tlist, name_sheet):
    ws = _get_ws(SH_MAIN, name_sheet)
    ws.append_row(tlist, value_input_option="USER_ENTERED")
    _cache_invalidate(("get", SH_MAIN.id, name_sheet))

def write_soberi_in_google_sheets(tlist):
    ws = _get_ws(SH_SKL, "Заявка на сборку")
    ws.append_row(
        tlist,
        value_input_option="USER_ENTERED",
        table_range="A1",
        insert_data_option="INSERT_ROWS",
    )
    _cache_invalidate(("get", SH_SKL.id, "Заявка на сборку"))

def process_transfer_record(record) -> None:
    # Открываем листы (кэшируются)
    ws_boy = _get_ws(SH_ANS, "Онлайн остатки Бой")
    ws_transfer = _get_ws(SH_ANS, "Выгрузка передача")

    # Получаем данные из "Онлайн остатки Бой"
    boy_data = ws_boy.get_all_values()
    if not boy_data or len(boy_data) < 2:
        print("Нет данных в листе 'Онлайн остатки Бой'")
        return

    # Формируем критерии для сравнения (столбцы 5–12 -> индексы 4–11)
    criteria_boy = (
        record[1].strip(),  # Компания
        record[4].strip(),  # Модель авто
        record[5].strip(),  # Радиус
        record[6].strip(),  # Размер
        record[7].strip(),  # Марка резины
        record[8].strip(),  # Модель резины
        record[9].strip(),  # Сезон
        record[10].strip(),  # Диск
    )

    boy_row_index = None
    row_boy = None  # первые 15 столбцов найденной строки
    for i, row in enumerate(boy_data[1:], start=2):
        if len(row) < 15:
            continue
        row_criteria = tuple(cell.strip() for cell in row[4:12])
        if criteria_boy == row_criteria:
            boy_row_index = i
            row_boy = row[:15]
            break

    if boy_row_index is None:
        print("Строка по заданным критериям не найдена в 'Онлайн остатки Бой'")
        return

    # Получаем данные из листа "Выгрузка передача"
    transfer_data = ws_transfer.get_all_values()
    if not transfer_data or len(transfer_data) < 2:
        print("Нет данных в листе 'Выгрузка передача'")
        return

    transfer_row_index = None
    for j, t_row in enumerate(transfer_data[1:], start=2):
        if len(t_row) < 15:
            continue
        t_row_first15 = tuple(cell.strip() for cell in t_row[:15])
        if tuple(cell.strip() for cell in row_boy) == t_row_first15:
            transfer_row_index = j
            break

    if transfer_row_index is None:
        print("Соответствующая строка не найдена в 'Выгрузка передача'")
    else:
        current_time = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
        # заменили два update_cell на один batch_update
        ws_transfer.batch_update(
            [
                {"range": f"P{transfer_row_index}:Q{transfer_row_index}",
                 "values": [[current_time, record[18].strip()]]}
            ]
        )
        _cache_invalidate(("get", SH_ANS.id, "Выгрузка передача"))

    ws_boy.delete_rows(boy_row_index)
    _cache_invalidate(("get", SH_ANS.id, "Онлайн остатки Бой"))

def get_record_sklad():
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    # здесь нам подходят данные без заголовка
    list_of_lists = ws_answer.get_all_values()[1:]
    zayavka: List[List[str]] = []
    for tlist in list_of_lists:
        if len(tlist) > 15 and not tlist[15] and len(tlist) > 2 and tlist[2]:
            zayavka.append(tlist[3:14])
    return zayavka

def nomer_sborka(
    company, radius, razmer, marka_rez, model_rez, sezon, marka_ts, type_disk, type_kolesa
) -> List[str]:
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    list_of_lists = ws_answer.get_all_values()

    unique_numbers = set()
    for tlist in list_of_lists:
        if len(tlist) < 14:
            continue
        if (
            str(tlist[3]) == str(company)
            and str(tlist[4]) == str(marka_ts)
            and str(tlist[5]) == str(radius)
            and str(tlist[6]) == str(razmer)
            and str(tlist[7]) == str(marka_rez)
            and str(tlist[8]) == str(model_rez)
            and str(tlist[9]) == str(sezon)
            and str(tlist[10]) == str(type_disk)
            and str(tlist[11]) == str(type_kolesa)
        ):
            unique_numbers.add(str(tlist[13]))

    for tlist in list_of_lists:
        if len(tlist) < 14:
            continue
        if (
            str(tlist[3]) == str(company)
            and str(tlist[4]) == str(marka_ts)
            and str(tlist[5]) == str(radius)
            and str(tlist[6]) == str(razmer)
            and str(tlist[9]) == str(sezon)
            and str(tlist[11]) == str(type_kolesa)
        ):
            unique_numbers.add(str(tlist[13]))

    return list(unique_numbers)

def update_data_sborka(marka_rez, model_rez, type_disk, type_kolesa, nomer_sborka):
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    list_of_lists = ws_answer.get_all_values()

    for j, tlist in enumerate(list_of_lists):
        if len(tlist) < 14:
            continue
        if (str(tlist[13]) == str(nomer_sborka) and str(tlist[11]) == str(type_kolesa)):
            # было два update -> заменили на один batch_update
            ws_answer.batch_update(
                [
                    {"range": f"K{j + 1}:K{j + 1}", "values": [[str(type_disk)]]},
                    {"range": f"H{j + 1}:I{j + 1}", "values": [[str(marka_rez), str(model_rez)]]},
                ]
            )
            _cache_invalidate(("get", SH_SKL.id, "Заявка на сборку"))
            return

def nomer_sborka_ko(
    company, radius, razmer, marka_rez, model_rez, sezon, marka_ts, type_disk, type_kolesa
):
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    list_of_lists = ws_answer.get_all_values()

    komplekts = set()  # уникальные значения row[13]
    axes: List[str] = []
    matching_rows: List[List[str]] = []

    # Полный критерий
    for tlist in list_of_lists:
        if len(tlist) < 17:
            continue
        if (
            str(tlist[3]) == str(company)
            and str(tlist[4]) == str(marka_ts)
            and str(tlist[5]) == str(radius)
            and str(tlist[6]) == str(razmer)
            and str(tlist[7]) == str(marka_rez)
            and str(tlist[8]) == str(model_rez)
            and str(tlist[9]) == str(sezon)
            and str(tlist[10]) == str(type_disk)
            and str(tlist[16]) == ""
        ):
            matching_rows.append(tlist)

    # Упрощённый критерий (если ничего не нашли)
    if not matching_rows:
        for tlist in list_of_lists:
            if len(tlist) < 17:
                continue
            if (
                str(tlist[3]) == str(company)
                and str(tlist[4]) == str(marka_ts)
                and str(tlist[5]) == str(radius)
                and str(tlist[6]) == str(razmer)
                and str(tlist[9]) == str(sezon)
                and str(tlist[16]) == ""
            ):
                matching_rows.append(tlist)

    # Группировка по значению в столбце 13
    groups: Dict[str, List[List[str]]] = {}
    for row in matching_rows:
        key = row[13]
        groups.setdefault(key, []).append(row)

    # Если в группе ровно 4 строки, это "Комплект"
    for key, rows in groups.items():
        if len(rows) == 4:
            komplekts.add(key)

    # Обработка "Ось": 2 строки с разными сторонами
    if len(matching_rows) >= 2:
        sides = {row[11] for row in matching_rows}
        if "Левое" in sides and "Правое" in sides:
            axes.append(matching_rows[0][13])

    if type_kolesa == "Ось":
        return axes
    elif type_kolesa == "Комплект":
        return list(komplekts)
    else:
        return []

def update_record_sborka(
    company,
    username,
    radius,
    razmer,
    marka_rez,
    model_rez,
    sezon,
    marka_ts,
    type_disk,
    type_kolesa,
    message_link,
    nomer_sborka,
):
    ws_answer = _get_ws(SH_SKL, "Заявка на сборку")
    list_of_lists = ws_answer.get_all_values()
    now = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    current_time = str(now)

    pos_value = type_kolesa
    matching_indexes: List[Tuple[int, List[str]]] = []

    # База для совпадений (без учёта позиции)
    for j, tlist in enumerate(list_of_lists):
        if len(tlist) < 17:
            continue
        if (
            str(tlist[3]) == str(company)
            and str(tlist[4]) == str(marka_ts)
            and str(tlist[5]) == str(radius)
            and str(tlist[6]) == str(razmer)
            and str(tlist[7]) == str(marka_rez)
            and str(tlist[8]) == str(model_rez)
            and str(tlist[9]) == str(sezon)
            and str(tlist[10]) == str(type_disk)
            and str(tlist[13]) == str(nomer_sborka)
            and str(tlist[16]) == ""
        ):
            matching_indexes.append((j, tlist))

    if not matching_indexes:
        for j, tlist in enumerate(list_of_lists):
            if len(tlist) < 17:
                continue
            if (
                str(tlist[3]) == str(company)
                and str(tlist[4]) == str(marka_ts)
                and str(tlist[5]) == str(radius)
                and str(tlist[6]) == str(razmer)
                and str(tlist[9]) == str(sezon)
                and str(tlist[13]) == str(nomer_sborka)
                and str(tlist[16]) == ""
            ):
                matching_indexes.append((j, tlist))

    updates: List[Dict[str, Any]] = []

    if pos_value not in ("Комплект", "Ось"):
        # ищем строку с точным совпадением по стороне/позиции
        for j, tlist in matching_indexes:
            if str(tlist[11]) == str(pos_value):
                data = [current_time, "Собрано", str(message_link), str(username)]
                updates.append({"range": f"P{j + 1}:S{j + 1}", "values": [data]})
                break
    else:
        # Комплект — по 2 на сторону; Ось — по 1 на сторону
        required_count = 2 if pos_value == "Комплект" else 1
        sides_updated = {"Правое": 0, "Левое": 0}
        for j, tlist in matching_indexes:
            side = str(tlist[11])
            if side in sides_updated and sides_updated[side] < required_count:
                data = [current_time, "Собрано", str(message_link), str(username)]
                updates.append({"range": f"P{j + 1}:S{j + 1}", "values": [data]})
                sides_updated[side] += 1

    if updates:
        ws_answer.batch_update(updates)
        _cache_invalidate(("get", SH_SKL.id, "Заявка на сборку"))


def find_logistics_rows(limit_hours: int = 12) -> tuple[list[str], list[str]]:
    ws = _get_ws(SH_LOG, "Логисты выход на смену")
    rows = ws.get_all_values()
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
        if direction != "ВШМ":
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