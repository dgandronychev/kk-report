import logging
from datetime import datetime, timedelta
import gspread
from app.config import URL_GOOGLE_SHEETS_SKLAD, URL_GOOGLE_SHEETS_ORDER, URL_GOOGLE_SHEETS_LOC_SHM
import app.config as cfg

logger = logging.getLogger(__name__)

# Кэш для данных "Склад"
cache_sklad_data = None
cache_sklad_timestamp = None
CACHE_DURATION = 60  # секунд

def load_sklad_data():
    global cache_sklad_data, cache_sklad_timestamp
    if (cache_sklad_data is None) or ((datetime.now() - cache_sklad_timestamp).total_seconds() > CACHE_DURATION):
        try:
            gc = gspread.service_account("app/creds.json")
            sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
            ws = sh.worksheet("Склад")
            data = ws.get_all_values()

            if not data:
                cache_sklad_data = []
            else:
                header = data[0]          # первая строка с заголовками
                filtered = [header]

                for row in data[1:]:
                    # нужна хотя бы колонка C
                    if len(row) <= 2:
                        continue

                    qty_str = row[2].strip()
                    if not qty_str:
                        continue

                    # поддержка "1,5" и "1.5"
                    qty_norm = qty_str.replace(",", ".")
                    try:
                        qty = float(qty_norm)
                    except ValueError:
                        # если в ячейке не число, на всякий случай НЕ фильтруем
                        filtered.append(row)
                        continue

                    if qty != 0:
                        filtered.append(row)

                cache_sklad_data = filtered

            cache_sklad_timestamp = datetime.now()

        except Exception as e:
            logger.exception("Ошибка при загрузке данных склада: %s", e)
            cache_sklad_data = []
            return []

    return cache_sklad_data


def get_material_names() -> list[str]:
    """
    Возвращает отсортированный уникальный список всех Наименований (колонка B),
    без повторов, для построения клавиатуры.
    """
    data = load_sklad_data()
    if not data:
        return []
    # B — индекс 1
    names = [row[1].strip() for row in data[1:] if len(row) > 1 and row[1].strip()]
    # убираем дубликаты и сортируем по регистронезависимо
    return sorted(set(names), key=lambda s: s.lower())

def get_material_quantity(material_name: str) -> str:
    """
    Возвращает общую величину остатка из первой подходящей строки (C — индекс 2).
    Обычно нужен для простых сценариев, если ячейка одна.
    """
    data = load_sklad_data()
    if not data:
        return ""
    for row in data[1:]:
        if len(row) > 1 and row[1].strip() == material_name:
            return row[2].strip() if len(row) > 2 else ""
    return ""

def get_recipient_names() -> list:
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Справочник")
        names = ws.col_values(1)[1:]
        return [name.strip() for name in names if name.strip()]
    except Exception as e:
        logger.exception("Ошибка при загрузке получателей: %s", e)
        return []
def get_material_cells(material_name: str) -> list[dict]:
    """
    Возвращает список всех строк листа "Склад", где колонка B==material_name.
    Каждая строка — словарь {"cell": D, "quantity": C}.
    """
    data = load_sklad_data()
    result = []
    for row in data[1:]:
        if len(row) > 4 and row[1].strip() == material_name:
            result.append({
                "cell": row[4].strip(),
                "quantity": row[2].strip() or "0"
            })
    return result

def write_row_to_sheet(worksheet_name: str, row: list):
    """
    Общая функция записи строки в указанный лист.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet(worksheet_name)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Ошибка записи строки в таблицу %s: %s", worksheet_name, e)

def write_arrival_row(arrival_items: list, user_tag: str, message_link: str):
    """
    Дописывает в лист "Передача/поступление" каждое поступление с указанием ячейки.
    В конец добавляется значение ячейки (если оно есть).
    """
    current_dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    for item in arrival_items:
        row = [
            current_dt,
            "Поступление",
            "",
            item.get("name", ""),
            item.get("quantity", ""),
            "",  # колонка «Получатель» или пусто для поступлений
            user_tag,
            message_link,
            item.get("cell", "")  # новый столбец «Ячейка»
        ]
        write_row_to_sheet("Передача/поступление", row)

def write_transfer_row(transfer_items: list, recipient: str, user_tag: str, message_link: str):
    """
    Аналогично для выдачи: в конец каждой строки добавляем ячейку.
    """
    current_dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    for item in transfer_items:
        row = [
            current_dt,
            "Выдача",
            "",
            item.get("name", ""),
            item.get("quantity", ""),
            recipient,
            user_tag,
            message_link,
            item.get("cell", "")
        ]
        write_row_to_sheet("Передача/поступление", row)

def return_cell_to_free(cell: str):
    """
    Если остаток в выбранной ячейке стал 0, очищаем D-колонку в листе "Склад"
    и добавляем эту ячейку в первый свободный ряд листа "Склад2" (колонка A).
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws_sklad = sh.worksheet("Склад")
        ws_free  = sh.worksheet("Склад2")
        # 1) Очистить ячейку в основном листе
        all_vals = ws_sklad.get_all_values()
        for i, row in enumerate(all_vals, start=1):
            if len(row) > 4 and row[4].strip() == cell:
                ws_sklad.update(f"E{i}", "")  # стираем кол‐во ячейки
                break
        # 2) Добавить в список свободных
        ws_free.append_row([cell], value_input_option="USER_ENTERED")
    except Exception as e:
        logger.exception("Ошибка возврата ячейки в Склад2: %s", e)


def get_free_cells() -> list[str]:
    """
    Возвращает все непустые значения из 4-го столбца (D) листа 'Склад2',
    пропуская первую (заголовочную) строку.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Справочник ячеек")
        data = ws.get_all_values()
        free = []
        for row in data[1:]:
            if row[0].strip():
                free.append(row[0].strip())
        return free
    except Exception as e:
        logger.exception("Ошибка загрузки свободных ячеек: %s", e)
        return []

def remove_free_cell(cell: str):
    """
    Ищет в листе 'Склад2' первую строку (начиная со второй),
    у которой в 4-м столбце (D) стоит именно `cell`, и чистит её.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Склад2")
        data = ws.get_all_values()
        # начинаем нумерацию строк с 2, потому что data[0] — это заголовок (D1)
        for idx, row in enumerate(data[1:], start=2):
            if len(row) >= 5 and row[4].strip() == cell:
                # update_cell принимает (row, col, value)
                ws.update_cell(idx, 5, "")
                return
    except Exception as e:
        logger.exception("Ошибка удаления свободной ячейки: %s", e)


def load_data_rez_disk() -> None:
    data = get_data_order("Сити Диски сити new")
    cfg.BAZA_DISK_SITY = data
    data = get_data_order("ЯД Диски")
    cfg.BAZA_DISK_YNDX = data
    data = get_data_order("СИТИ Лето РФ new")
    cfg.BAZA_REZN_SITY = data
    data = get_data_order("ЯД Лето РФ new")
    cfg.BAZA_REZN_YNDX = data

def get_data_order(name: str) -> list[str]:
    gc = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(URL_GOOGLE_SHEETS_ORDER)
    ws = sh.worksheet(name)
    records = ws.get_all_records()
    return records

def get_shm_locations_by_company(company_ui: str) -> list[str]:
    """
    Загружает адреса из листа 'Локации СШМ' (URL_GOOGLE_SHEETS_LOC_SHM),
    фильтруя по колонке 'Каршеринг'.
    company_ui: 'Ситидрайв' | 'Яндекс'
    """
    mapping = {"Ситидрайв": "СИТИ", "Яндекс": "ЯНДЕКС", "Белка": "БЕЛКА"}
    key = mapping.get(company_ui)
    if not key:
        return []

    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_LOC_SHM)
        ws = sh.worksheet("Локации СШМ")
        data = ws.get_all_values()
    except Exception as e:
        logger.exception("Ошибка загрузки 'Локации СШМ': %s", e)
        return []

    if not data:
        return []

    header = data[0]

    try:
        idx_addr = header.index("Адрес")
        idx_car  = header.index("Каршеринг")
    except ValueError:
        logger.error("Нет нужных колонок в листе 'Локации СШМ'")
        return []

    addrs = []
    for row in data[1:]:
        if len(row) <= max(idx_addr, idx_car):
            continue
        if row[idx_car].strip().upper() == key:
            addr = row[idx_addr].strip()
            if addr:
                addrs.append(addr)

    return sorted(set(addrs), key=lambda s: s.lower())

def get_next_request_number() -> str:
    """
    Возвращает следующий номер заявки в формате 'zvN'
    из листа 'Реестр заказ ТМЦ' (колонка B).
    Если столбец пустой или произошла ошибка — вернёт 'zv1'.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Реестр заказ ТМЦ")

        # B-колонка (Номер заявки), пропускаем заголовок
        values = ws.col_values(2)[1:]
        max_num = 0
        for v in values:
            v = v.strip()
            if not v:
                continue
            # ожидаем формат zv123
            v_lower = v.lower()
            if not v_lower.startswith("zv"):
                continue
            num_part = v[2:]
            try:
                n = int(num_part)
            except ValueError:
                continue
            if n > max_num:
                max_num = n

        next_num = max_num + 1 if max_num > 0 else 1
        return f"zv{next_num}"
    except Exception as e:
        logger.exception("Ошибка получения номера заявки ТМЦ: %s", e)
        return "zv1"


def write_request_tmc_rows(
    request_number: str,
    fio: str,
    tag: str,
    department: str,
    items: list[dict],
    message_link: str,
):
    """
    Записывает строки в лист 'Реестр заказ ТМЦ' по каждой позиции ТМЦ
    одним запросом append_rows().
    """
    current_dt = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Реестр заказ ТМЦ")

        # Подготавливаем строки ОДНИМ списком
        rows = []
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

        ws.append_rows(
            rows,
            value_input_option="USER_ENTERED"
        )

    except Exception as e:
        logger.exception("Ошибка записи заявки ТМЦ в реестр: %s", e)

# --- НОВОЕ: работа с листом "Реестр заказ ТМЦ" --------------------------
def get_open_request_numbers() -> list[str]:
    """
    Возвращает уникальный отсортированный список номеров заявок (колонка
    'Номер заявки') из листа 'Реестр заказ ТМЦ', у которых статус == 'Новая'.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Реестр заказ ТМЦ")

        data = ws.get_all_values()
        if not data:
            return []

        header = data[0]
        try:
            idx_num = header.index("Номер заявки")
            idx_status = header.index("Статус")
        except ValueError:
            logger.error("Нет колонок 'Номер заявки' или 'Статус' в 'Реестр заказ ТМЦ'")
            return []

        nums: set[str] = set()
        for row in data[1:]:
            if len(row) <= max(idx_num, idx_status):
                continue
            num = row[idx_num].strip()
            status = row[idx_status].strip().lower()
            if not num:
                continue
            if status == "новая":
                nums.add(num)

        return sorted(nums)

    except Exception as e:
        logger.exception("Ошибка получения открытых заявок ТМЦ: %s", e)
        return []


def get_request_items(request_number: str) -> list[dict]:
    """
    Возвращает список позиций для заявки `request_number` из листа
    'Реестр заказ ТМЦ'.

    Каждый элемент:
    {
        "name": <Вид ТМЦ>,
        "quantity": <Кол-во>
    }
    """
    items: list[dict] = []
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Реестр заказ ТМЦ")

        data = ws.get_all_values()
        if not data:
            return []

        header = data[0]
        try:
            idx_num = header.index("Номер заявки")
            idx_name = header.index("Вид ТМЦ")
            idx_qty = header.index("Кол-во")
        except ValueError:
            logger.error("Нет нужных колонок в 'Реестр заказ ТМЦ'")
            return []

        for row in data[1:]:
            if len(row) <= max(idx_num, idx_name, idx_qty):
                continue
            if row[idx_num].strip() != request_number:
                continue
            name = row[idx_name].strip()
            qty = row[idx_qty].strip()
            if not name:
                continue
            items.append({"name": name, "quantity": qty})

    except Exception as e:
        logger.exception("Ошибка чтения позиций заявки ТМЦ: %s", e)

    return items


def update_request_status(request_number: str, status: str, message_link: str | None = None):
    """
    Обновляет статус (и при необходимости ссылку на отчёт) для всех строк
    заявки `request_number` в листе 'Реестр заказ ТМЦ'.
    """
    try:
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_SKLAD)
        ws = sh.worksheet("Реестр заказ ТМЦ")

        data = ws.get_all_values()
        if not data:
            return

        header = data[0]
        try:
            idx_num = header.index("Номер заявки")
            idx_status = header.index("Статус")
            idx_link = header.index("Ссылка на отчет")
        except ValueError:
            logger.error("Нет нужных колонок в 'Реестр заказ ТМЦ' для обновления статуса")
            return

        updates = []
        col_status = chr(ord("A") + idx_status)
        col_link = chr(ord("A") + idx_link)

        for row_idx, row in enumerate(data[1:], start=2):
            if len(row) <= idx_num:
                continue
            if row[idx_num].strip() != request_number:
                continue

            if message_link is None:
                rng = f"{col_status}{row_idx}"
                updates.append({"range": rng, "values": [[status]]})
            else:
                rng = f"{col_status}{row_idx}:{col_link}{row_idx}"
                updates.append({"range": rng, "values": [[status, message_link]]})

        if updates:
            ws.batch_update(updates)

    except Exception as e:
        logger.exception("Ошибка обновления статуса заявки ТМЦ: %s", e)

