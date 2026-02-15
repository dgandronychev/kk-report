import logging
import gspread
from gspread import Client, Spreadsheet
from typing import List, Tuple, Optional, Any
from datetime import datetime, timedelta, time
from app.config import URL_GOOGLE_SHEETS_CHART, GSPREAD_URL_MAIN, GSPREAD_URL_ANSWER
from gspread.exceptions import APIError

EXCEL_EPOCH = datetime(1899, 12, 30)

logger = logging.getLogger(__name__)

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