import logging
import asyncio
import json
import requests
import traceback
import time
import subprocess
import os
from datetime import datetime, timedelta
from pathlib import Path

# aiogram
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ParseMode, ContentType
from aiogram.utils import executor

# aiogram FSM
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# gspread
import gspread
from gspread import Client, Spreadsheet

# ------------------------------------------------------------
# Глобальные переменные и настройки
# ------------------------------------------------------------
gspread_url_ras = "https://docs.google.com/spreadsheets/d/1iH5IeurStoNQB9FKwdxehTF5hB_1QcX9LFYjpVqsxvo/edit?pli=1&gid=0#gid=0"
gspread_url_info = "https://docs.google.com/spreadsheets/d/1EkakbixukbAI56XqvQfB-NQ-sQjmhb5Lr1McHfQ3BYg/edit?gid=0#gid=0"
# Токен и айди чатов
# Рабочий вариант
TOKEN_BOT = "7706049825:AAHEDd34vspO14bJZfwJnrwyD6BCk8HNa5Y"
idCleanCar = -1001594522274
idCleanService = -1002005014821
idCleanLogistic = -1002334797298
#idTest = -1002444764866

# Тестовый вариант
#TOKEN_BOT = "7413900981:AAHCsVFC2RMQmnboDs6qA5PiIkvMsxvhQoY"
#idCleanCar = -1002444764866
#idCleanService = -1002444764866
#idCleanLogistic = -1002444764866
#idTest = -1002444764866

# URL
urlSendMediaGroup = (f"https://api.telegram.org/{TOKEN_BOT}/sendMediaGroup")

# Прочие данные
tegCleanCar = "@kazminabuh"
tegCleanCarLog = "@Anna_econo"
tegSoglasovanie = "@Anastasiya_CleanCar"

BOT_URL = f"https://api.telegram.org/bot{TOKEN_BOT}/getMe"
CHECK_INTERVAL = 5

key_empty = []
key_sity = ["Москва", "Санкт-Петербург", "Нижний Новгород", "Другое"]
key_company = ["КлинКар", "КлинКар Сервис", "КлинКар Логистика"]
key_pay = ["Бизнес-карта", "Наличные <> Перевод <> Личная карта", "Счёт", "Отчетные документы(УПД/акты и тд.)", "Другое"]
key_pay_sub = ["Подача на возмещение(свои деньги) + 6%", "Отчёт из подочётных"]

# Получаем путь к текущему скрипту и поднимаемся на уровень выше
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCKER_COMPOSE_PATH = os.path.join(BASE_DIR, "docker-compose.yml")

# ------------------------------------------------------------
# Инициализируем бота и диспетчер (aiogram)
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN_BOT, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())


# ------------------------------------------------------------
# Функции для записи в Google Sheets
# ------------------------------------------------------------
def write_in_answers_ras(tlist):
    try:
        gc: Client = gspread.service_account("app/creds.json")
        sh: Spreadsheet = gc.open_by_url(gspread_url_ras)
        ws = sh.worksheet("Лист1")
        ws.append_row(tlist, value_input_option='USER_ENTERED',
            table_range='A1', insert_data_option='INSERT_ROWS')
    except Exception as e:
        logger.error(f"Ошибка записи в Google Sheet: {e}")
        traceback.print_exc()

def read_from_answers_ras() -> dict[str, list[str]]:
    try:
        gc: Client = gspread.service_account("app/creds.json")
        sh: Spreadsheet = gc.open_by_url(gspread_url_info)
        ws = sh.worksheet("Справочник")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return {}

        headers = rows[0]
        data_rows = rows[1:]

        guide: dict[str, list[str]] = {}
        for col_idx, header in enumerate(headers):
            if not header:
                continue
            col_values: list[str] = []
            for row in data_rows:
                if col_idx < len(row) and row[col_idx].strip():
                    col_values.append(row[col_idx].strip())
            guide[header] = col_values

        return guide

    except Exception as e:
        logger.error(f"Ошибка чтения из Google Sheet: {e}")
        traceback.print_exc()
        return {}

# ------------------------------------------------------------
# Состояния (FSM) для опроса по "Расход" (expense)
# ------------------------------------------------------------
class ExpenseStates(StatesGroup):
    waiting_fio = State()
    waiting_city = State()
    waiting_city_dop = State()
    waiting_direction = State()
    waiting_direction_dop = State()
    waiting_sum = State()
    waiting_org = State()
    waiting_pay = State()
    waiting_pay_dop = State()
    waiting_pay_subcategory = State()
    waiting_reason_category = State()
    waiting_reason_description = State()
    waiting_invoice_org = State()
    waiting_files = State()  # Ожидание загрузки файлов

# ------------------------------------------------------------
# Дополнительная функция:
# Собрать все медиа-файлы (фото/документы и т.д.) из одного сообщения
# ------------------------------------------------------------
@dp.message_handler(
    content_types=[ContentType.PHOTO, ContentType.DOCUMENT, ContentType.VIDEO],
    state=ExpenseStates.waiting_files
)
async def expense_step_files(message: types.Message, state: FSMContext):
    # 1. Определяем file_id и тип
    if message.photo:
        largest_photo = max(message.photo, key=lambda ph: ph.width * ph.height)
        file_id = largest_photo.file_id
        file_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
    else:
        await message.answer("Неизвестный тип файла. Пришлите фото, документ или видео.")
        return

    # 2. Сохраняем в FSM не только file_id, но и file_type
    data = await state.get_data()
    files_data = data.get("files_data", [])

    # Проверяем, не достигли ли лимит
    if len(files_data) >= 4:
        await message.answer("У вас уже 4 файла! Нажмите «Готово» для формирования отчёта.",
                             reply_markup=_keyboard_done_exit())
        return

    files_data.append({"file_id": file_id, "type": file_type})
    await state.update_data(files_data=files_data)

    # 3. Удаляем предыдущее служебное сообщение (если есть)
    last_info_msg_id = data.get("last_info_msg_id")
    if last_info_msg_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=last_info_msg_id)
        except Exception:
            pass

    # 4. Выводим новое «служебное» сообщение
    text = (
        "Файл получен. Пришлите ещё или нажмите «Готово», "
        "чтобы сформировать отчёт."
    )
    new_msg = await message.answer(text, reply_markup=_keyboard_done_exit())
    await state.update_data(last_info_msg_id=new_msg.message_id)


# Кнопка "Готово" + "Выход"
def _keyboard_done_exit():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("Готово")
    kb.add("Выход")
    return kb

# Шаг 2. Отдельный хендлер нажатия «Готово» (или команда /done)
@dp.message_handler(lambda msg: msg.text == "Готово", state=ExpenseStates.waiting_files)
async def finalize_expense(message: types.Message, state: FSMContext):
    # Здесь вызываем вашу финальную функцию
    await _expense_finish_and_send(message, state)

def make_keyboard(items: list[str]) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for it in items:
        kb.add(KeyboardButton(it))
    kb.add(KeyboardButton("Назад"), KeyboardButton("Выход"))
    return kb
# ------------------------------------------------------------
# /expense - начало цепочки
# ------------------------------------------------------------
@dp.message_handler(commands=["expense"], state="*")
async def cmd_expense(message: types.Message, state: FSMContext):
    """
    Старт функции /expense.
    Логируем, что пользователь начал оформление расхода.
    """
    if message.chat.type == 'private':
        # Логируем юзернейм
        logger.info(f"[expense] User @{message.from_user.username} ({message.from_user.id}) начал оформление расхода.")

        # Сбрасываем предыдущее состояние (если было)
        await state.finish()

        # Переходим в состояние ввода ФИО
        await message.answer("Введите ФИО", reply_markup=_keyboard_exit_only())
        await ExpenseStates.waiting_fio.set()
    else:
        await message.answer("Эта команда доступна только в личных сообщениях с ботом.")

# ------------------------------------------------------------
# Хендлеры по шагам (FSM)
# ------------------------------------------------------------
@dp.message_handler(state=ExpenseStates.waiting_fio)
async def expense_step_fio(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    fio = message.text.strip()
    await state.update_data(fio=fio)

    # Предлагаем ввести город
    await message.answer("Укажите Ваш город", reply_markup=_keyboard_back_exit(key_sity))
    await ExpenseStates.waiting_city.set()


@dp.message_handler(state=ExpenseStates.waiting_city)
async def expense_step_city(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возвращаемся к вводу ФИО
        await message.answer("Введите ФИО", reply_markup=_keyboard_exit_only())
        await ExpenseStates.waiting_fio.set()
        return

    if message.text == "Другое":
        await message.answer(
            "Введите название Вашего города",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_city_dop.set()
    else:
        city = message.text.strip()
        await state.update_data(city=city)
        guide = read_from_answers_ras()
        columns = list(guide.keys())
        await message.answer(
            "Укажите направление, для которого производится расход",
            reply_markup=_keyboard_back_exit(columns)
        )
        await ExpenseStates.waiting_direction.set()


@dp.message_handler(state=ExpenseStates.waiting_city_dop)
async def expense_step_city_dop(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возврат к выбору города
        await message.answer("Укажите Ваш город", reply_markup=_keyboard_back_exit(key_sity))
        await ExpenseStates.waiting_city.set()
        return

    city = message.text.strip()
    await state.update_data(city=city)
    guide = read_from_answers_ras()
    columns = list(guide.keys())
    await message.answer(
        "Укажите направление, для которого производится расход",
        reply_markup=_keyboard_back_exit(columns)
    )
    await ExpenseStates.waiting_direction.set()


@dp.message_handler(state=ExpenseStates.waiting_direction)
async def expense_step_direction(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возвращаемся к выбору города
        await message.answer("Укажите Ваш город", reply_markup=_keyboard_back_exit(key_sity))
        await ExpenseStates.waiting_city.set()
        return

    if message.text == "Другое":
        await message.answer(
            "Введите направление",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_direction_dop.set()
    else:
        direction = message.text.strip()
        await state.update_data(direction=direction)
        # Переход к вводу суммы
        await message.answer(
            "Введите сумму с 2 знаками после точки, пример: 5678.91",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_sum.set()


@dp.message_handler(state=ExpenseStates.waiting_direction_dop)
async def expense_step_direction_dop(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        guide = read_from_answers_ras()
        columns = list(guide.keys())
        await message.answer(
            "Укажите направление, для которого производится расход",
            reply_markup=_keyboard_back_exit(columns)
        )
        await ExpenseStates.waiting_direction.set()
        return

    direction = message.text.strip()
    await state.update_data(direction=direction)
    # Переход к вводу суммы
    await message.answer(
        "Введите сумму с 2 знаками после точки, пример: 5678.91",
        reply_markup=_keyboard_back_exit(key_empty)
    )
    await ExpenseStates.waiting_sum.set()


@dp.message_handler(state=ExpenseStates.waiting_sum)
async def expense_step_sum(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        guide = read_from_answers_ras()
        columns = list(guide.keys())
        await message.answer(
            "Укажите направление, для которого производится расход",
            reply_markup=_keyboard_back_exit(columns)
        )
        await ExpenseStates.waiting_direction.set()
        return

    try:
        sum_value = float(message.text.strip())
        await state.update_data(summa=sum_value)
        # Организация
        await message.answer(
            "Укажите компанию, с которой произведен расход",
            reply_markup=_keyboard_back_exit(key_company)
        )
        await ExpenseStates.waiting_org.set()
    except ValueError:
        await message.answer(
            "Неверный формат числа. Введите сумму с 2 знаками после точки, пример: 5678.91",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        return


@dp.message_handler(state=ExpenseStates.waiting_org)
async def expense_step_org(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возвращаемся к сумме
        await message.answer(
            "Введите сумму с 2 знаками после точки, пример: 5678.91",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_sum.set()
        return
    
    organization = message.text.strip()
    if organization in key_company:
        await state.update_data(organization=organization)
        # Способ оплаты
        await message.answer("Способ оплаты", reply_markup=_keyboard_back_exit(key_pay))
        await ExpenseStates.waiting_pay.set()
    else:
        await message.answer(
            "Вы ввели компанию не из предложенного списка.\nУкажите компанию, с которой произведен расход",
            reply_markup=_keyboard_back_exit(key_company)
        )


@dp.message_handler(state=ExpenseStates.waiting_pay)
async def expense_step_pay(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возвращаемся к организации
        await message.answer(
            "Укажите компанию, с которой произведен расход",
            reply_markup=_keyboard_back_exit(key_company)
        )
        await ExpenseStates.waiting_org.set()
        return

    if message.text == "Другое":
        await message.answer(
            "Укажите способ оплаты в произвольной форме",
            reply_markup=_keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_pay_dop.set()
    elif message.text == "Наличные <> Перевод <> Личная карта":
        # Это означает, что надо выбрать подкатегорию
        pay = message.text.strip()
        await state.update_data(pay=pay)
        await message.answer(
            "Выберите из следующих категорий",
            reply_markup=_keyboard_back_exit(key_pay_sub)
        )
        await ExpenseStates.waiting_pay_subcategory.set()
    else:
        pay = message.text.strip()
        await state.update_data(pay=pay)
        data = await state.get_data()
        direction = data.get("direction", "")
        guide = read_from_answers_ras()
        await message.answer(
            "Укажите причину расхода",
            reply_markup=make_keyboard(guide[direction])

        )
        await ExpenseStates.waiting_reason_category.set()


@dp.message_handler(state=ExpenseStates.waiting_pay_dop)
async def expense_step_pay_dop(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возврат к выбору способа оплаты
        await message.answer("Способ оплаты", reply_markup=_keyboard_back_exit(key_pay))
        await ExpenseStates.waiting_pay.set()
        return

    pay = message.text.strip()
    await state.update_data(pay=pay)
    data = await state.get_data()
    direction = data.get("direction", "")
    guide = read_from_answers_ras()
    await message.answer(
        "Укажите причину расхода",
        reply_markup=make_keyboard(guide[direction])

    )
    await ExpenseStates.waiting_reason_category.set()


@dp.message_handler(state=ExpenseStates.waiting_pay_subcategory)
async def expense_step_pay_subcategory(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        # Возврат к выбору способа оплаты
        await message.answer("Способ оплаты", reply_markup=_keyboard_back_exit(key_pay))
        await ExpenseStates.waiting_pay.set()
        return

    subcat_text = message.text.strip()
    if subcat_text == "Подача на возмещение(свои деньги) + 6%":
        cat = 1
    else:
        cat = 2

    await state.update_data(cat=cat)

    data = await state.get_data()
    direction = data.get("direction", "")
    guide = read_from_answers_ras()
    await message.answer(
        "Укажите причину расхода",
        reply_markup=make_keyboard(guide[direction])

    )
    await ExpenseStates.waiting_reason_category.set()


@dp.message_handler(state=ExpenseStates.waiting_reason_category)
async def expense_step_reason_category(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        data = await state.get_data()
        pay = data.get("pay", "")
        if pay == "Наличные <> Перевод <> Личная карта":
            await message.answer(
                "Выберите из следующих категорий",
                reply_markup=_keyboard_back_exit(key_pay_sub)
            )
            await ExpenseStates.waiting_pay_subcategory.set()
        else:
            await message.answer("Способ оплаты", reply_markup=_keyboard_back_exit(key_pay))
            await ExpenseStates.waiting_pay.set()
        return

    reason_category = message.text.strip()

    await state.update_data(reason_category=reason_category)

    await message.answer("Описание причины расхода", reply_markup=_keyboard_back_exit(key_empty))
    await ExpenseStates.waiting_reason_description.set()


@dp.message_handler(state=ExpenseStates.waiting_reason_description)
async def expense_step_reason_description(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    guide = read_from_answers_ras()
    reason_description = message.text.strip()

    if message.text == "Назад":
        data = await state.get_data()
        direction = data.get("direction", "")
        guide = read_from_answers_ras()
        await message.answer(
            "Укажите причину расхода",
            reply_markup=make_keyboard(guide[direction])

        )
        return await ExpenseStates.waiting_reason_category.set()

    await state.update_data(reason_description=reason_description)

    data = await state.get_data()
    pay = data.get("pay", "")

    if pay in ["Счёт", "Отчетные документы(УПД/акты и тд.)", "Другое"]:
         await message.answer(
             "Укажите наименование организации",
             reply_markup = _keyboard_back_exit(key_empty)
         )
         await ExpenseStates.waiting_invoice_org.set()
    else:
        # Иначе сразу собираем файлы
        await message.answer(
            "Загрузите фото чека/счета (от одного до четырёх)",
            reply_markup = _keyboard_back_exit(key_empty)
        )
        await ExpenseStates.waiting_files.set()

@dp.message_handler(state=ExpenseStates.waiting_invoice_org)
async def expense_step_invoice_org(message: types.Message, state: FSMContext):
    # Обработка "Выход"
    if _check_exit(message):
        await state.finish()
        return
    # Обработка "Назад" — возвращаемся к вводу причины
    if message.text == "Назад":
        await message.answer("Описание причины расхода", reply_markup=_keyboard_back_exit(key_empty))
        await ExpenseStates.waiting_reason_description.set()
        return

    # Сохраняем наименование организации и идём собирать файлы
    invoice_org = message.text.strip()
    await state.update_data(invoice_org=invoice_org)
    await message.answer(
        "Загрузите фото чека/счета (от одного до четырёх)",
        reply_markup=_keyboard_back_exit(key_empty)
    )
    await ExpenseStates.waiting_files.set()
# ------------------------------------------------------------
# Принимаем одно сообщение с файлом/файлами (например, фото или документ)
# ------------------------------------------------------------
@dp.message_handler(
    content_types=[
        ContentType.PHOTO,
        ContentType.DOCUMENT,
        ContentType.VIDEO
    ],
    state=ExpenseStates.waiting_files
)

async def _expense_finish_and_send(message: types.Message, state: FSMContext):
    """
    Формируем окончательную строку для отправки, делаем логи.
    Потом отправляем в нужный чат и (если требуется) в гугл-таблицу.
    """
    data = await state.get_data()
    files_data = data.get("files_data", [])
    now = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")

    user_id = message.from_user.id
    username = message.from_user.username

    fio = data.get("fio", "")
    city = data.get("city", "")
    direct = data.get("direction", "")
    summa = data.get("summa", 0)
    organization = data.get("organization", "")
    pay = data.get("pay", "")
    cat = data.get("cat", 0)
    reason_category = data.get("reason_category", "")
    reason_description = data.get("reason_description", "")
    invoice_org = data.get("invoice_org", "")
    file_ids = data.get("file_ids", [])

    # Формируем общий текст
    string_tg = (
        f"#Отчет\nДата и время: {now}\n"
        f"ФИО: {fio}\n"
        f"Тег ТГ: {username}\n"
        f"Город: {city}\n"
        f"Направление: {direct}\n"
        f"Сумма: {str(summa).replace('.', ',')}\n"
    )

    # Если cat = 1, значит +6%
    if cat == 1:
        plus_6 = round(summa / 94 * 100, 2)
        string_tg += (
            f"Сумма +6%: {str(plus_6).replace('.', ',')}\n"
            f"Компания: {organization}\n"
            f"Вид оплаты: Наличные &lt;&gt; Перевод &lt;&gt; Личная карта\n"
            f"Подвид оплаты: Подача на возмещение(свои деньги) + 6%\n"
            f"Причина: {reason_category}\n"
            f"Описание причины: {reason_description}\n"
        )
    elif cat == 2:
        string_tg += (
            f"Компания: {organization}\n"
            f"Вид оплаты: Наличные &lt;&gt; Перевод &lt;&gt; Личная карта\n"
            f"Подвид оплаты: Отчёт из подочётных\n"
            f"Причина: {reason_category}\n"
            f"Описание причины: {reason_description}\n"
        )
    else:
        string_tg += (
            f"Компания: {organization}\n"
            f"Вид оплаты: {pay}\n"
            f"Причина: {reason_category}\n"
            f"Описание причины: {reason_description}\n"
        )
    if invoice_org:
        string_tg += f"Наименование организации по счёту: {invoice_org}\n"

    # Доп. теги
    if pay == "Счёт":
        if organization == "КлинКар Логистика":
            string_tg += f"\n{tegCleanCarLog}, загрузите на оплату, пожалуйста"
        else:
            string_tg += f"\n{tegCleanCar}, загрузите на оплату, пожалуйста"

    if cat == 1:
        string_tg += f"\n{tegSoglasovanie}, cогласуйте, пожалуйста"

    # Формирование списка для GoogleSheets
    # в соответствии с logic manages_lists
    if cat == 1:
        plus_6 = round(summa / 94 * 100, 2)
        tlist = [
            str(now),
            fio,
            username,
            city,
            str(summa).replace(".", ","),
            str(plus_6).replace(".", ","),
            organization,
            direct,
            pay,
            "Подача на возмещение(свои деньги) + 6%",
            reason_category,
            reason_description,
            "",
            "",
            invoice_org
        ]
    elif cat == 2:
        tlist = [
            str(now),
            fio,
            username,
            city,
            str(summa).replace(".", ","),
            "",
            organization,
            direct,
            pay,
            "Отчёт из подочётных",
            reason_category,
            reason_description,
            "",
            "",
            invoice_org
        ]
    else:
        tlist = [
            str(now),
            fio,
            username,
            city,
            str(summa).replace(".", ","),
            "",
            organization,
            direct,
            pay,
            "",
            reason_category,
            reason_description,
            "",
            "",
            invoice_org
        ]

    # Логирование в конце (собранные данные)
    logger.info(f"[expense] Собраны данные: {tlist}")

    # Определяем, в какой чат отправлять
    if organization == "КлинКар":
        chat_id = idCleanCar
    elif organization == "КлинКар Сервис":
        chat_id = idCleanService
    else:
        # по умолчанию
        chat_id = idCleanLogistic

        # Формируем media_group
    media_group = []
    for i, file_info in enumerate(files_data):
        file_id = file_info["file_id"]
        file_type = file_info["type"]

        # В зависимости от типа выбираем класс
        if file_type == "photo":
            input_media = types.InputMediaPhoto(media=file_id)
        elif file_type == "document":
            input_media = types.InputMediaDocument(media=file_id)
        elif file_type == "video":
            input_media = types.InputMediaVideo(media=file_id)
        else:
            # На всякий случай fallback
            input_media = types.InputMediaDocument(media=file_id)

        # Добавим заголовок только к первому элементу
        if i == 0:
            input_media.caption = string_tg

        media_group.append(input_media)

    try:
        # Если у нас действительно есть медиа (если 0 - вылетит ошибка)
        if media_group:
            await bot.send_media_group(chat_id=chat_id, media=media_group)

        # Если способ оплаты не "Отчетные документы(УПД/акты и тд.)",
        # тогда пишем в Google Sheets
        if pay != "Отчетные документы(УПД/акты и тд.)":
            write_in_answers_ras(tlist)

        await message.answer("Ваш запрос успешно сформирован", reply_markup=types.ReplyKeyboardMarkup())
    except Exception as e:
        logger.error(f"Ошибка при отправке медиа-группы или записи: {e}")
        traceback.print_exc()
        await message.answer("При формировании заявки произошла ошибка, обратитесь к разработчикам.")

    # Завершаем состояние
    await state.finish()


# ------------------------------------------------------------
# Клавиатуры
# ------------------------------------------------------------
def _keyboard_exit_only():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("Выход")
    return kb

def _keyboard_back_exit(record_list):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for record in record_list:
        kb.add(record)
    kb.add("Назад", "Выход")
    return kb

def _check_exit(message: types.Message) -> bool:
    """
    Проверяем, не нажал ли пользователь кнопку "Выход".
    Если да — пишем сообщение и возвращаем True, чтобы прервать обработку.
    """
    if message.text and message.text.lower() == "выход":
        # Удаляем клавиатуру
        kb_remove = types.ReplyKeyboardRemove()
        asyncio.create_task(
            message.answer("Оформление заявки завершено.", reply_markup=kb_remove)
        )
        return True
    return False


# ------------------------------------------------------------
# Мониторинг бота и перезапуск
# ------------------------------------------------------------
def is_bot_alive():
    try:
        response = requests.get(BOT_URL, timeout=5)
        if response.status_code == 200:
            return True
    except requests.RequestException:
        pass
    return False
def rebuild_project():
    logger.info(f"Bot is not responding. Rebuilding project...")
    try:
        subprocess.run(["docker-compose", "down"], check=True)
        subprocess.run(["docker-compose", "up", "--build", "-d"], check=True)
        logger.info(f"Project rebuilt and restarted successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during rebuild: {e}")

def monitor_bot():
    """
    Запускается в отдельном потоке, периодически проверяет,
    доступен ли бот. Если нет – пытается его перезапустить.
    """
    while True:
        try:
            if not is_bot_alive():
                rebuild_project()
        except Exception as e:
            logger.error(f"Exception in monitor_bot: {e}")
            traceback.print_exc()
            rebuild_project()
        time.sleep(CHECK_INTERVAL)


@dp.message_handler(commands=["test"])
async def test(message: types.Message):
    print(1/0)
async def main():
    """
    Основная точка входа для асинхронного кода.
    Запускаем мониторинг в отдельном потоке, затем — aiogram-поллинг.
    """
    # Запускаем мониторинг в отдельном потоке
    monitor_thread = None
    if os.getenv("USE_MONITORING", "1") == "1":
        monitor_thread = asyncio.to_thread(monitor_bot)
        asyncio.create_task(monitor_thread)

    # Запускаем поллинг aiogram
    try:
        await dp.start_polling()
    except Exception as e:
        logger.error(f"Исключение при старте polling: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # Можно настроить отдельный лог-файл:
    log_name = f'logs/{datetime.now().strftime("%Y-%m-%d")}.log'
    Path(log_name).parent.mkdir(parents=True, exist_ok=True)

    fh = logging.FileHandler(log_name, mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Запуск
    asyncio.run(main())