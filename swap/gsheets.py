import gspread
from datetime import datetime, timedelta
from typing import Dict, List

import app.config as cfg
from app.config import URL_GOOGLE_SHEETS_EXTRA_WASH


def _open_spreadsheet():
    gc = gspread.service_account("app/creds.json")
    return gc.open_by_url(URL_GOOGLE_SHEETS_EXTRA_WASH)


def _group_by_city(records, value_key: str) -> Dict[str, List[str]]:
    """
    Универсальный группировщик:
    records — список словарей от get_all_records()
    value_key — имя колонки со значением ('Адрес мойки' или 'Услуга')
    """
    by_city: dict[str, set[str]] = {}

    for rec in records:
        city = str(rec.get("Город", "")).strip()
        value = str(rec.get(value_key, "")).strip()
        if not city or not value:
            continue
        by_city.setdefault(city, set()).add(value)

    # сортируем значения по алфавиту
    return {city: sorted(values) for city, values in by_city.items()}


def get_extra_wash_addresses_by_city() -> Dict[str, List[str]]:
    """
    Читает лист 'БД Адреса':
        Город | Адрес мойки
    и возвращает {город: [адрес1, адрес2, ...]}.
    """
    sh = _open_spreadsheet()
    ws = sh.worksheet("БД Адреса")
    records = ws.get_all_records()
    return _group_by_city(records, "Адрес мойки")


def get_extra_wash_services_by_city() -> Dict[str, List[str]]:
    """
    Читает лист 'БД Услуги':
        Город | Услуга
    и возвращает {город: [услуга1, услуга2, ...]}.
    """
    sh = _open_spreadsheet()
    ws = sh.worksheet("БД Услуги")
    records = ws.get_all_records()
    return _group_by_city(records, "Услуга")


def load_extra_wash_locations() -> None:
    """
    Загружает из Google Sheets:
    - EXTRA_WASH_ADDRESSES_BY_CITY  (БД Адреса)
    - EXTRA_WASH_SERVICES_BY_CITY   (БД Услуги)
    """
    addr_by_city = get_extra_wash_addresses_by_city()
    srv_by_city = get_extra_wash_services_by_city()

    cfg.EXTRA_WASH_ADDRESSES_BY_CITY = addr_by_city
    cfg.EXTRA_WASH_SERVICES_BY_CITY = srv_by_city



def record_extra_wash_report(
    username: str,
    service: str,
    plate: str,
    address: str,
    ticket: str,
    message_link: str,
    sity: str
) -> None:
    # Авторизация в Google Sheets
    gc = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(URL_GOOGLE_SHEETS_EXTRA_WASH)
    ws = sh.worksheet("Данные")

    # Формируем данные строки
    timestamp = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    row = [
        timestamp,
        username,
        plate,
        service,
        address,
        ticket,
        message_link,
        sity,
    ]

    # Добавляем строку в конец листа
    ws.append_row(row, value_input_option="USER_ENTERED")
