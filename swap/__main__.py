import logging
import asyncio
import json
import requests
import traceback
import time
import re
import os
import threading
from collections import defaultdict
from enum import IntEnum
from datetime import datetime, timedelta
from pathlib import Path
from gspread.exceptions import APIError
import random
import asyncio
from app.config import TOKEN_BOT, chat_id_damage_Sity, chat_id_sborka_Sity, chat_id_damage_Yandex,urlSendMediaGroup, LOGS_DIR, thread_id_gates_Yandex, thread_id_gates_Sity
from app.config import chat_id_sborka_Yandex, thread_id_sborka_Yandex, thread_id_sborka_Sity, link_damage_Sity
from app.config import link_sborka_Sity, link_damage_Yandex, link_sborka_Yandex, tablo, list_users, list_users_belka
from app.config import POR_NOMER_REZ, POR_NOMER_DIS, URL_GET_FIO,chat_id_change_work, thread_id_change_work, URL_REGISTRASHION, URL_GET_INFO_TASK
from app.config import chat_id_sborka_Belka, link_sborka_Belka, thread_id_sborka_Belka, chat_id_damage_Belka, link_damage_Belka, thread_id_damage_Belka
from app.utils.gsheets import update_all_sheets, get_max_nomer_sborka, write_soberi_in_google_sheets_rows, loading_bz_znaniya
from app.utils.gsheets import get_number_util, write_in_answers_ras, write_soberi_in_google_sheets, process_transfer_record, update_data_sborka, write_open_gate_row, find_logistics_rows
from app.utils.gsheets import get_record_sklad, nomer_sborka, nomer_sborka_ko, update_record_sborka, write_in_answers_ras_nomen, write_in_answers_ras_shift
from app.states import DamageStates, SoberiStates, DemountingStates, SborkaStates, NomenclatureStates, StartJobShiftStates, EndWorkShiftStates, RegistrationStates, CheckStates, OpenGateStates

# aiogram
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ParseMode, ContentType
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.utils import executor

# aiogram FSM
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

from app.config import GOOGLE_DRIVE_CREDS_JSON, GOOGLE_DRIVE_DAMAGE_BELKA_FOLDER_ID
from app.utils.drive_zip import safe_zip_name, build_zip_from_tg_files, upload_zip_private

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
loop = asyncio.get_event_loop()

update_time = datetime.now() - timedelta(hours=3)

key_type_kolesa = ["В сборе", "Только резина", "Диск"]
key_grz = []
key_company = ["СитиДрайв", "Яндекс", "Белка"]
key_company_small = ["СитиДрайв", "Яндекс"]
key_count = ["1 колесо", "Ось", "Комплект"]
key_radius = ["15", "16", "17", "18", "19", "20"]
key_exit = []
key_approve = ["Подтверждаю"]
key_type_disk = ["Литой оригинальный", "Литой неоригинальный", "Штамп"]
key_type_check = ["Левое колесо","Правое колесо", "Ось", "Комплект"]
key_condition_disk = ["Ок", "Ремонт", "Утиль"]
key_condition_rezina = ["Ок","Ремонт", "Утиль"]
key_reason_ytilia = ["Езда на спущенном", "Износ протектора", "Боковой пробой", "Грыжа", ]
key_reason_remonta = ["Латка", "Грибок", "Замена вентиля", "Герметик борта"]
key_reason_rem_ytilia_diska = ["Искревление ОСИ", "Трещина", "Отколот кусок", "Замена датчика давления"]
key_dir_kolesa = ["Левое", "Правое"]
key_type_sborka = ["Комплект", "Ось"]
key_sogl = ["Да", "Нет"]
key_object = ["Комплект","Ось","Колесо"]
key_chisla = ["0","1","2","3","4","5"]
key_ready = ["Готово"]

# Структура Листа Резина Сити
class GHRezina(IntEnum):
    NOMER = 0
    RADIUS = 1  # Радиус
    RAZMER = 2  # Размер
    SEZON = 3  # Сезон
    MARKA = 4  # Марка
    MODEL = 5  # Модель
    COMPANY = 6  # Компания
    MARKA_TS = 7  # Марка ТС

# Регулярное выражение для проверки государственного номера автомобиля
REGEX_AUTO_NUMBER = r'^[а-я]{1}\d{3}[а-я]{2}\d{2,3}$'

# -------------------------------------------------------------------------------------------------------
def _safe_fullname_from_profile(rep: dict | None, message) -> str:
    """Возвращает ФИО из ответа бекенда или строит его из данных Telegram.
    Никогда не кидает KeyError.
    Приоритет: rep['user']['fullname'] -> rep['fullname'] -> rep['fio'] -> Telegram (first+last) -> @username -> user_id
    """
    fio = None
    used_backend = False
    try:
        if isinstance(rep, dict):
            user_obj = rep.get('user') if isinstance(rep.get('user'), dict) else None
            if user_obj and isinstance(user_obj.get('fullname'), str) and user_obj.get('fullname').strip():
                fio = user_obj.get('fullname').strip(); used_backend = True
            elif isinstance(rep.get('fullname'), str) and rep.get('fullname').strip():
                fio = rep.get('fullname').strip(); used_backend = True
            elif isinstance(rep.get('fio'), str) and rep.get('fio').strip():
                fio = rep.get('fio').strip(); used_backend = True
    except Exception:
        pass
    if not fio:
        try:
            logging.warning("No 'user.fullname' in profile response; fallback to Telegram name for user_id=%s",
                            getattr(message.from_user, 'id', ''))
        except Exception:
            pass
        fn = (getattr(message.from_user, 'first_name', '') or '').strip()
        ln = (getattr(message.from_user, 'last_name', '') or '').strip()
        if fn or ln:
            fio = (fn + ' ' + ln).strip()
        elif getattr(message.from_user, 'username', None):
            fio = '@' + str(message.from_user.username)
        else:
            fio = str(message.from_user.id)
    return fio

_GS_CONCURRENCY = int(os.environ.get("GS_CONCURRENCY", "3"))
_gs_sema = threading.Semaphore(_GS_CONCURRENCY)

def with_sheets_retry(func, *args, max_attempts=6, base_delay=1.0, **kwargs):
    """
    Выполняет func(*args, **kwargs) с повтором при APIError 5xx/429.
    Ограничивает конкурентность до GS_CONCURRENCY потоков (по умолчанию 3).
   """
    for attempt in range(1, max_attempts + 1):
        try:
            with _gs_sema:
                return func(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None)
            if code in (500, 502, 503, 504, 429):
                # 429: уважаем Retry-After если есть
                retry_after = 0.0
                try:
                    retry_after = float(e.response.headers.get("Retry-After", "0"))
                except Exception:
                    retry_after = 0.0
                # экспонента + джиттер
                backoff = base_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0, 0.4 * backoff)
                delay = max(retry_after, backoff + jitter)
                logger.warning(
                    f"[attempt {attempt}] Sheets API {code}, sleeping {delay:.2f}s and retrying..."
                )
                time.sleep(delay)
                continue
            # Остальные ошибки — пробрасываем сразу
            raise RuntimeError(f"Google Sheets API failed after {max_attempts} attempts")
# -------------------------------------------------------------------------------------------------------
@dp.message_handler(commands=["registration"], state="*")
async def cmd_registration(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("Команда доступна только в ЛС")
        return

    # Очищаем предыдущее состояние
    await state.finish()

    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(types.KeyboardButton(text="📱 Поделиться контактом", request_contact=True))
    kb.add(types.KeyboardButton(text="❌ Выход"))

    await message.answer(
        "Убедитесь, что в приложение КЛИНКАР указан номер телефона, который привязан к аккаунту Телеграм в формате 7**********.\n"
        "Нажмите на кнопку 'Поделиться контактом':",
        reply_markup=kb
    )
    await RegistrationStates.WAIT_PHONE.set()

@dp.message_handler(state=RegistrationStates.WAIT_PHONE, content_types=["contact"])
async def process_contact(message: types.Message, state: FSMContext):
    contact = message.contact

    # Проверяем, что пользователь отправил свой контактный номер, а не чужой
    if message.from_user.id != contact.user_id:
        await message.answer("Отправьте свой собственный номер телефона.")
        return

    phone_number = re.sub("[^0-9]", "", contact.phone_number)
    user_id = message.from_user.id
    username = message.from_user.username if message.from_user.username else "не указан"

    json_data = {
        "phone": phone_number,
        "tg_username": username,
        "tg_chat_id": str(user_id)
    }

    try:
        response = requests.post(URL_REGISTRASHION, json=json_data)
        if response.status_code < 400:
            await message.answer("✅ Вы успешно прошли регистрацию!", reply_markup=types.ReplyKeyboardRemove())
        else:
            response_data = response.json()
            await message.answer(f"⚠ Ошибка: {response_data.get('result', 'Неизвестная ошибка')}",
                                 reply_markup=types.ReplyKeyboardRemove())
    except Exception as e:
        logging.exception(e)
        await message.answer("❌ Произошла ошибка. Пожалуйста, обратитесь к разработчикам.")

    await state.finish()

@dp.message_handler(state=RegistrationStates.WAIT_PHONE, text="❌ Выход")
async def cancel_registration(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("❌ Регистрация отменена.", reply_markup=types.ReplyKeyboardRemove())
# -------------------------------------------------------------------------------------------------------

@dp.message_handler(commands=["zayavka_sborka"], state="*")
async def cmd_zayavka_sborka(message: types.Message, state: FSMContext):
    global update_time
    update_time = datetime.now()
    await print_zayavka()

async def print_zayavka():
    try:
        # если with_sheets_retry — блокирующая, обёрнём её в to_thread
        tlist = await asyncio.to_thread(with_sheets_retry, get_record_sklad, max_attempts=5, base_delay=1)
    except RuntimeError:
        logger.error("Не удалось получить данные из Google Sheets после 5 попыток")
        return

    if not tlist:
        return

    # Подсчет количества одинаковых записей
    grouped_records = defaultdict(int)
    for record in tlist:
        key = tuple(record[0:])  # Используем всю запись в качестве ключа
        grouped_records[key] += 1

    # Формирование строк для вывода
    str_list = []
    for record, count in grouped_records.items():
        # Формирование базовой части строки
        sb_number = record[10]  # Здесь хранится 'sb12', 'sb13' и т.д.
        string = (
            f"{sb_number} | {count} шт | {record[0]} | {record[1]} | {record[3]}/{record[2]} | "
        )
        if record[4]:
            string += f"{record[4]} {record[5]} | "
        if record[6]:
            string += f"{record[6]} | "
        if record[7]:
            string += f"{record[7]} | "
        if record[8]:
            string += f"{record[8]} | "
        if record[9]:
            string += f"{record[9]}ч"
        if record[16]:
            string += f"| {record[16]}"
        string += "\n---------------------------\n"

        # Проверка на наличие ранее добавленной записи с таким же номером и "2 шт"
        # Если найдём, заменим "2 шт" на "комплект" и удалим "Левое | " / "Правое | "
        found_index = -1
        for i, s in enumerate(str_list):
            if s.startswith(f"{sb_number} | 2 шт"):
                new_s = s.replace("2 шт", "комплект")
                new_s = new_s.replace("Левое | ", "")
                new_s = new_s.replace("Правое | ", "")
                str_list[i] = new_s
                found_index = i
                break

        if found_index != -1:
            continue

        # Новая проверка для составления оси:
        # Если для одного номера уже есть запись с "1 шт" и в ней содержится "Левое |"
        # а текущая запись содержит "Правое |" (или наоборот),
        # то объединяем их в одну строку, заменяя "1 шт" на "ось"
        found_index_axis = -1
        for i, s in enumerate(str_list):
            if s.startswith(f"{sb_number} | 1 шт"):
                if ("Левое | " in s and "Правое | " in string) or ("Правое | " in s and "Левое | " in string):
                    new_s = s.replace("1 шт", "ось")
                    new_s = new_s.replace("Левое | ", "")
                    new_s = new_s.replace("Правое | ", "")
                    str_list[i] = new_s
                    found_index_axis = i
                    break
        if found_index_axis != -1:
            continue

        # Если ни одна из проверок не сработала, добавляем строку
        str_list.append(string)


    # Отправка сообщений блоками по 40 записей (или 30, как в оригинальном коде)
    out_str = ""
    len_ = 0
    for record in str_list:
        out_str += record
        len_ += 1
        if len_ > 30:
            # print(out_str)
            # await bot.send_message(tablo, out_str, parse_mode="Markdown")
            await bot.send_message(tablo, out_str)
            out_str = ""
            len_ = 0

    if len_ > 0:
        # print(out_str)
        await bot.send_message(tablo, out_str)
        # await bot.send_message(tablo, out_str, parse_mode="Markdown")


async def periodic_print_zayavka():
    # сразу первый прогон
    await print_zayavka()
    # а затем — ровно через каждый час
    while True:
        await asyncio.sleep(3600)
        await print_zayavka()

@dp.message_handler(commands=["update_data"], state="*")
async def cmd_update_data(message: types.Message, state: FSMContext):
    if message.from_user.id not in list_users:
        return await message.answer("У вас нет прав для вызова данной команды")

    await message.answer("Обновление списков началось")
    try:
        with_sheets_retry(
            loading_rezina_is_Google_Sheets,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось загурзить из Гугл Таблицы данные после 3 попыток")
        return await message.answer(
            "Не удалось  обновить базу данных. Попробуйте чуть позже."
        )

    try:
        with_sheets_retry(
            loading_model_is_Google_Sheets,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось загрузить из Гугл Таблицы данные после 3 попыток")
        return await message.answer(
            "Не удалось  обновить базу данных. Попробуйте чуть позже."
        )
    await message.answer("Обновление завершено")


@dp.message_handler(commands=["damage"], state="*")
async def cmd_damage(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[damage] User @{user.username} ({user.id}) начал оформление повреждения")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)
    await message.answer("Компания:",reply_markup=getKeyboardStep1(key_company))
    await DamageStates.WAIT_COMPANY.set()

@dp.message_handler(state=DamageStates.WAIT_COMPANY)
async def damage_step_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    await state.update_data(company=message.text)

    # Предлагаем ввести город
    await message.answer("Вид колеса:", reply_markup=getKeyboardList(key_type_kolesa))
    await DamageStates.WAIT_VID_KOLESA.set()


@dp.message_handler(state=DamageStates.WAIT_VID_KOLESA)
async def damage_step_vid_kolesa(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Компания:", reply_markup=getKeyboardStep1(key_company))
        await DamageStates.WAIT_COMPANY.set()
        return

    vid_kolesa = message.text
    await state.update_data(vid_kolesa=vid_kolesa)

    if vid_kolesa == "В сборе":
        await message.answer("Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_grz))
        await DamageStates.WAIT_VVOD_GRZ.set()
    elif vid_kolesa == "Только резина":
        data = await state.get_data()
        company = data.get("company", "")
        await message.answer("Радиус:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DamageStates.WAIT_RADIUS.set()
    elif vid_kolesa == "Диск":
        await message.answer("Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_grz))
        await DamageStates.WAIT_VVOD_GRZ.set()

@dp.message_handler(state=DamageStates.WAIT_VVOD_GRZ)
async def damage_step_vvod_grz(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Вид колеса:", reply_markup=getKeyboardList(key_type_kolesa))
        await DamageStates.WAIT_VID_KOLESA.set()
        return

    if message.text == "Грз отсутствует на колесе":
        await damage_step_vvod_grz_2(message, state)
        return

    data = await state.get_data()
    company = data.get("company", "")

    grz_ts = getGRZTs(company, message.text.lower())

    if len(grz_ts):
        await message.answer("Подтвердите ГРЗ из списка:", reply_markup=getKeyboardStep1(sorted(grz_ts)))
        await DamageStates.WAIT_APPROVE_GRZ.set()
    else:
        await state.update_data(grz_no_base=message.text.lower())
        await message.answer("В базе данных нет введенного вами ГРЗ. Подвердите правильность введенного госномера", reply_markup=getKeyboardList(key_approve))
        await DamageStates.WAIT_APPROVE_GRZ.set()


@dp.message_handler(state=DamageStates.WAIT_APPROVE_GRZ)
async def damage_step_approve_grz(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return
    
    if message.text == "Назад":
        await message.answer("Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_grz))
        await DamageStates.WAIT_VVOD_GRZ.set()
        return
    elif message.text == "Подтверждаю":
        data = await state.get_data()
        await state.update_data(grz=data.get("grz_no_base", ""))
    else:
        grz = message.text
        await state.update_data(grz=grz)


    data = await state.get_data()
    company = data.get("company", "")
    grz = data.get("grz", "")

    model = ""
    if company == "СитиДрайв":
        source_list = list_per_st
        MODEL = 2
        NOMER = 0
    elif company == "Яндекс":
        source_list = list_per_ya
        MODEL = 3
        NOMER = 0
    else:
        source_list = list_per_blk
        MODEL = 1
        NOMER = 2

    for row in source_list:
        if str(row[NOMER]) == grz:
            model = str(row[MODEL])
            break

    if not model:
        await message.answer("Номер не найден в базе данных. Введите модель и марку автомобиля вручную",
            reply_markup=getKeyboardList(key_exit))
        await DamageStates.WAIT_VVOD_MARKA_TS.set()
        return

    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton(model))
    kb.add(KeyboardButton("Ввести вручную"))
    kb.add(KeyboardButton("Назад"), KeyboardButton("Выход"))

    await message.answer("Марка автомобиля:", reply_markup=kb)
    await DamageStates.WAIT_VVOD_MARKA_TS.set()

@dp.message_handler(state=DamageStates.WAIT_VVOD_GRZ_2)
async def damage_step_vvod_grz_2(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_grz))
        await DamageStates.WAIT_VVOD_GRZ.set()
        return

    if message.text != "Ввести вручную":
        if message.text == "Грз отсутствует на колесе":
            await state.update_data(grz="б/н")

        else:
            await state.update_data(grz=message.text)

        data = await state.get_data()
        company = data.get("company", "")
        await message.answer("Радиус:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DamageStates.WAIT_RADIUS.set()
    else:
        await message.answer("Введите модель и марку автомобиля вручную",
                         reply_markup=getKeyboardList(key_exit))
        await DamageStates.WAIT_VVOD_MARKA_TS.set()


@dp.message_handler(state=DamageStates.WAIT_VVOD_MARKA_TS)
async def damage_step_vvod_marka_ts(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_grz))
        await DamageStates.WAIT_VVOD_GRZ.set()
        return

    await state.update_data(marka_ts=message.text)


    data = await state.get_data()
    company = data.get("company", "")
    await message.answer("Радиус:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
    await DamageStates.WAIT_RADIUS.set()

@dp.message_handler(state=DamageStates.WAIT_RADIUS)
async def damage_step_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    vid_kolesa = data.get("vid_kolesa", "")
    company = data.get("company", "")
    grz = data.get("grz", "")

    if message.text == "Назад" and vid_kolesa == "В сборе":
        model = ""
        if company == "СитиДрайв":
            source_list = list_per_st
            MODEL = 2
        elif company == "Яндекс":
            source_list = list_per_ya
            MODEL = 3
        else:
            source_list = list_per_blk
            MODEL = 3
        NOMER = 0
        for row in source_list:
            if str(row[NOMER]) == grz:
                model = str(row[MODEL])
                break

        kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add(KeyboardButton(model))
        kb.add(KeyboardButton("Ввести вручную"))
        kb.add(KeyboardButton("Назад"), KeyboardButton("Выход"))

        await message.answer("Марка автомобиля:", reply_markup=kb)
        await DamageStates.WAIT_VVOD_MARKA_TS.set()
        return
    elif message.text == "Назад" and vid_kolesa == "Только резина":
        await message.answer("Компания:", reply_markup=getKeyboardStep1(key_company))
        await DamageStates.WAIT_COMPANY.set()
        return
    elif message.text == "Назад" and vid_kolesa == "Диск":
        await message.answer(
            "Марка автомобиля:",
            reply_markup = getKeyboardList(key_exit)
            )
        await DamageStates.WAIT_VVOD_MARKA_TS.set()
        return
    radius = message.text
    if not check_validation_radius(company, radius):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРадиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DamageStates.WAIT_RADIUS.set()
        return

    await state.update_data(radius=radius)

    if vid_kolesa == "Диск":
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await DamageStates.WAIT_TYPE_DISK.set()
        return

    await message.answer("Размер:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company,radius))))))
    await DamageStates.WAIT_RAZMER.set()


@dp.message_handler(state=DamageStates.WAIT_RAZMER)
async def damage_step_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")

    if message.text == "Назад":
        await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DamageStates.WAIT_RADIUS.set()
        return

    razmer = message.text

    if not check_validation_razmer(company, radius, razmer):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРазмер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await DamageStates.WAIT_RAZMER.set()
        return

    await state.update_data(razmer=razmer)


    await message.answer("Марка резины:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
    await DamageStates.WAIT_MARKA_REZ.set()

@dp.message_handler(state=DamageStates.WAIT_MARKA_REZ)
async def damage_step_marka_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")

    if message.text == "Назад":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await DamageStates.WAIT_RAZMER.set()
        return

    marka_rez = message.text

    if not check_validation_marka(company, radius, razmer, marka_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await DamageStates.WAIT_MARKA_REZ.set()
        return

    await state.update_data(marka_rez=marka_rez)

    await message.answer("Модель резины:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
    await DamageStates.WAIT_MODEL_REZ.set()

@dp.message_handler(state=DamageStates.WAIT_MODEL_REZ)
async def damage_step_model_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")

    if message.text == "Назад":
        await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await DamageStates.WAIT_MARKA_REZ.set()
        return

    model_rez = message.text

    if not check_validation_model(company, radius, razmer, marka_rez, model_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМодель резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await DamageStates.WAIT_MODEL_REZ.set()
        return

    await state.update_data(model_rez=model_rez)

    await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
    await DamageStates.WAIT_SEZON.set()

@dp.message_handler(state=DamageStates.WAIT_SEZON)
async def damage_step_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    vid_kolesa = data.get("vid_kolesa", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await DamageStates.WAIT_MODEL_REZ.set()
        return

    sezon = message.text

    if not check_validation_sezon(company, radius, razmer, marka_rez, model_rez,sezon):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await DamageStates.WAIT_SEZON.set()
        return

    await state.update_data(sezon=sezon)


    if vid_kolesa == "Только резина":
        await message.answer("Состояние резины:",
                         reply_markup=getKeyboardList(key_condition_rezina))
        await DamageStates.WAIT_SOST_REZ.set()
        return
    await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
    await DamageStates.WAIT_TYPE_DISK.set()

@dp.message_handler(state=DamageStates.WAIT_TYPE_DISK)
async def damage_step_type_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await DamageStates.WAIT_SEZON.set()
        return

    await state.update_data(type_disk=message.text)

    if data.get("company", "") == "Яндекс":
        park_ts = get_park_TS_YNDX(data.get("grz", ""))
        if park_ts:
            await message.answer(f"Данное ТС в парке {park_ts}")

    await message.answer("Состояние диска:", reply_markup=getKeyboardList(key_condition_disk))
    await DamageStates.WAIT_SOST_DISK.set()

@dp.message_handler(state=DamageStates.WAIT_SOST_DISK)
async def damage_step_sost_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await DamageStates.WAIT_TYPE_DISK.set()
        return

    sost_disk = message.text
    await state.update_data(sost_disk=sost_disk)

    data = await state.get_data()
    vid_kolesa = data.get("vid_kolesa", "")
    if vid_kolesa == "Диск":
        if sost_disk == "Ок":
            await message.answer(
                "Прикрепите от 2 до 10 фото в одном сообщение. И нажмите на кнопку Готово",
                reply_markup = _keyboard_done_exit()
            )
            await DamageStates.WAIT_FILES.set()
            return
        else:
            await message.answer(
                "Выберите причину ремонта/утиля диска из предложенных вариантов, либо впишите причину вручную:",
                reply_markup = getKeyboardList(key_reason_rem_ytilia_diska)
            )
            await DamageStates.WAIT_SOST_DISK_PRICH.set()
            return

    if sost_disk == "Ок":
        await message.answer("Состояние резины:",
                         reply_markup=getKeyboardList(key_condition_rezina))
        await DamageStates.WAIT_SOST_REZ.set()
    else:
        await message.answer("Выберите причину ремонта/утиля диска из предложенных вариантов, либо впишите причину вручную:",
                         reply_markup=getKeyboardList(key_reason_rem_ytilia_diska))
        await DamageStates.WAIT_SOST_DISK_PRICH.set()

@dp.message_handler(state=DamageStates.WAIT_SOST_DISK_PRICH)
async def damage_step_sost_disk_prich(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Состояние диска:", reply_markup=getKeyboardList(key_condition_disk))
        await DamageStates.WAIT_SOST_DISK.set()
        return

    await state.update_data(sost_disk_prich=message.text)

    data = await state.get_data()
    vid_kolesa = data.get("vid_kolesa", "")
    if vid_kolesa == "Диск":
        await message.answer(
            "Прикрепите от 2 до 10 фото в одном сообщение. И нажмите на кнопку Готово",
            reply_markup = _keyboard_done_exit()
        )
        await DamageStates.WAIT_FILES.set()
        return

    await message.answer("Состояние резины:",
                         reply_markup=getKeyboardList(key_condition_rezina))
    await DamageStates.WAIT_SOST_REZ.set()


@dp.message_handler(state=DamageStates.WAIT_SOST_REZ)
async def damage_step_sost_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    vid_kolesa = data.get("vid_kolesa", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sost_disk = data.get("sost_disk", "")

    if message.text == "Назад":
        if message.text == "Назад" and vid_kolesa == "В сборе":
            if sost_disk == "Ок":
                await message.answer("Сезонность резины:",
                                     reply_markup=getKeyboardList(sorted(
                                         list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
                await DamageStates.WAIT_SEZON.set()
                return
            else:
                await message.answer("Состояние диска:", reply_markup=getKeyboardList(key_condition_disk))
                await DamageStates.WAIT_SOST_DISK.set()
                return
        elif message.text == "Назад" and vid_kolesa == "Только резина":
            await message.answer("Модель резины:",
                                 reply_markup=getKeyboardList(
                                     sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
            await DamageStates.WAIT_MODEL_REZ.set()
            return

    sost_rez=message.text
    await state.update_data(sost_rez=sost_rez)

    if sost_rez == "Ремонт":
        await message.answer("Причина ремонта:", reply_markup=getKeyboardList(key_reason_remonta))
        await DamageStates.WAIT_SOST_REZ_PRICH.set()
    elif sost_rez == "Утиль":
        await message.answer("Выберите причину утиля из предложенных вариантов, либо впишите причину вручную:",
                         reply_markup=getKeyboardList(key_reason_ytilia))
        await DamageStates.WAIT_SOST_REZ_PRICH.set()
    elif sost_rez == "Ок":
        await message.answer("Прикрепите от 2 до 10 фото в одном сообщение. И нажмите на кнопку Готово",
                         reply_markup=_keyboard_done_exit())
        await DamageStates.WAIT_FILES.set()
@dp.message_handler(state=DamageStates.WAIT_SOST_REZ_PRICH)
async def damage_step_sost_rez_prich(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Состояние резины:",
                             reply_markup=getKeyboardList(key_condition_rezina))
        await DamageStates.WAIT_SOST_REZ.set()
        return


    await state.update_data(sost_rez_prich=message.text)

    await message.answer("Прикрепите от 2 до 10 фото в одном сообщение. И нажмите на кнопку Готово",
                         reply_markup=_keyboard_done_exit())
    await DamageStates.WAIT_FILES.set()

# Кнопка "Готово" + "Выход"
def _keyboard_done_exit():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("Готово")
    kb.add("Выход")
    return kb

@dp.message_handler(
    content_types=[ContentType.PHOTO, ContentType.DOCUMENT, ContentType.VIDEO],
    state=DamageStates.WAIT_FILES
)
async def collect_files(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    # Определяем тип и file_id
    if message.photo:
        file_type = 'photo'
        file_id = message.photo[-1].file_id
    elif message.document:
        file_type = 'document'
        file_id = message.document.file_id
    elif message.video:
        file_type = 'video'
        file_id = message.video.file_id
    else:
        return  # неожиданный content_type

    files.append({'type': file_type, 'media': file_id})
    await state.update_data(files=files)


@dp.message_handler(lambda msg: msg.text == "Готово", state=DamageStates.WAIT_FILES)
async def finalize_expense(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    if not 2 <= len(files) <= 10:
        return await message.answer("Нужно прикрепить от 2 до 10 файлов")

    username = data.get("username", "")
    company = data.get("company", "")
    grz = data.get("grz", "")
    type_disk = data.get("type_disk", "")
    marka_ts = data.get("marka_ts", "")
    vid_kolesa = data.get("vid_kolesa", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sezon = data.get("sezon", "")
    sost_disk = data.get("sost_disk", "")
    sost_rez = data.get("sost_rez", "")
    sost_disk_prich = data.get("sost_disk_prich", "")
    sost_rez_prich = data.get("sost_rez_prich", "")
    type = data.get("type", "")

    por_nomer_diska = ""
    por_nomer_rezina = ""

    is_damage_path = not (type == "check" and (sost_disk == "Ок" and sost_rez == "Ок"))

    if company == "Белка" and is_damage_path:
        try:
            zip_name = safe_zip_name(grz)
            zip_path = await build_zip_from_tg_files(bot, files)
            loop = asyncio.get_running_loop()
            file_id = await loop.run_in_executor(
                None,
                upload_zip_private,
                zip_path,
                zip_name,
                GOOGLE_DRIVE_DAMAGE_BELKA_FOLDER_ID,
                GOOGLE_DRIVE_CREDS_JSON
            )

            logger.info("[drive][belka_damage] uploaded zip name=%s file_id=%s", zip_name, file_id)
        except Exception:
            logger.exception("[drive][belka_damage] zip upload failed")

    if sost_disk == "Утиль":
        try:
            por_nomer_diska = with_sheets_retry(
                get_number_util,
                company,
                POR_NOMER_DIS,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось получить порядковый номер утиля после 3 попыток")
            return await message.answer(
                "Не удалось получить порядковый номер утиля. Попробуйте чуть позже."
            )
    if sost_rez == "Утиль":
        try:
            por_nomer_rezina = with_sheets_retry(
                get_number_util,
                company,
                POR_NOMER_REZ,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось получить порядковый номер утиля после 3 попыток")
            return await message.answer(
                "Не удалось получить порядковый номер утиля. Попробуйте чуть позже."
            )

    files[0]['caption'] = generating_report_tg(username,company,grz,type_disk,marka_ts,vid_kolesa,radius,razmer,marka_rez,model_rez,sezon,
                                               sost_disk,sost_rez,sost_disk_prich,sost_rez_prich,por_nomer_diska,por_nomer_rezina,type)
    if type == "check" and (sost_disk == "Ок" and sost_rez == "Ок"):
        if company == "СитиДрайв":
            chatId = chat_id_sborka_Sity
            chat_id_for_link = link_sborka_Sity
            thread_id = thread_id_sborka_Sity
        elif company == "Яндекс":
            chatId = chat_id_sborka_Yandex
            chat_id_for_link = link_sborka_Yandex
            thread_id = thread_id_sborka_Yandex
        else:
            chatId = chat_id_sborka_Belka
            chat_id_for_link = link_sborka_Belka
            thread_id = thread_id_sborka_Belka
    else:
        if company == "СитиДрайв":
            chatId = chat_id_damage_Sity
            chat_id_for_link = link_damage_Sity
            thread_id = None
        elif company == "Яндекс":
            chatId = chat_id_damage_Yandex
            chat_id_for_link = link_damage_Yandex
            thread_id = None
        else:
            chatId = chat_id_damage_Belka
            chat_id_for_link = link_damage_Belka
            thread_id = thread_id_damage_Belka

    dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': thread_id, 'media': json.dumps(files)}
    resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
    data = resp.json()
    message_id = data["result"][0]["message_id"]
    message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
    generating_report_google_sheets(username,company,grz,type_disk,marka_ts,vid_kolesa,radius,razmer,marka_rez,model_rez,sezon,
                                    sost_disk,sost_rez,sost_disk_prich,sost_rez_prich,por_nomer_diska,por_nomer_rezina,message_link,type)
    status_code = resp.status_code
    if status_code < 400:
        if type == "check":
            await message.answer('Ваша заявка на проверку готового колеса сформирована',
                         reply_markup=types.ReplyKeyboardRemove())
        else:
            await message.answer('Ваша заявка на оформление повреждения сформирована',
                         reply_markup=types.ReplyKeyboardRemove())
    else:
        await message.answer('При формировании заявки произошла ошибка')

    if vid_kolesa == "Диск":
        await state.finish()
        return

    if sost_disk == "Ок" and sost_rez == "Утиль":
        await message.answer("Есть покрышка на замену:",reply_markup=getKeyboardStep1(key_sogl))
        await DamageStates.WAIT_ZAMENA_POKRESHKA.set()
        return
    if sost_disk == "Ок" and sost_rez == "Ремонт":
        await message.answer("Уточните какое колесо:",
                             reply_markup=getKeyboardList(key_dir_kolesa))
        await SborkaStates.WAIT_TYPE_KOLESA.set()
        return
    if vid_kolesa == "Только резина":
        await message.answer("Тип диска:", reply_markup=getKeyboardStep1(key_type_disk))
        await DamageStates.GAP2.set()
        return
    if sost_disk == "Ремонт" or sost_disk == "Утиль":
        await message.answer("Есть другой диск на замену:", reply_markup=getKeyboardStep1(key_sogl))
        await DamageStates.GAP3.set()
        return
        bot.register_next_step_handler(message, gap3)
        return
    await state.finish()

def generating_report_tg(username,company,grz,type_disk,marka_ts,vid_kolesa,radius,razmer,marka_rez,model_rez,sezon,
                         sost_disk,sost_rez,sost_disk_prich,sost_rez_prich,por_nomer_diska,por_nomer_rezina,type):
    str_answer = ""
    if type == "check":
        str_answer = "Проверка готового колеса\n\n"
    str_answer = str_answer + "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
    if marka_ts!= "":
        str_answer = str_answer + "🚗 " + str(marka_ts) + "\n"
    if grz!= "":
        str_answer = str_answer + "#️⃣ " + str(grz) + "\n"
    str_answer = str_answer + "👷 @" + str(username) + "\n\n"
    if type != "check":
        str_answer = str_answer + "🛞 " + str(vid_kolesa) + "\n"
    if str(vid_kolesa) == "Диск":
        if radius:
            str_answer = str_answer + "R" + str(radius) + "\n"
    else:
        str_answer = str_answer + str(marka_rez) + " " + str(model_rez) + "\n"
        str_answer = str_answer + str(razmer) + "/" + str(radius) + "\n"
        if str(sezon).split(' ')[0] == "Лето":
            str_answer = str_answer + "☀️ " + str(sezon) + "\n"
        elif str(sezon).split(' ')[0] == "Зима":
            str_answer = str_answer + "❄️ " + str(sezon) + "\n"
        else:
            str_answer = str_answer + str(sezon) + "\n"
    if type_disk!= "":
        str_answer = str_answer + str(type_disk) + "\n\n"
        str_answer = str_answer + "🛞 Состояние диска: \n" + "#" + str(sost_disk).replace(' ', '_') + "\n"
    if sost_disk_prich!= "":
        str_answer = str_answer + "#" + str(sost_disk_prich).replace(' ', '_') + "\n"
    if sost_disk == "Утиль":
        str_answer = str_answer + "#" + str(por_nomer_diska).replace(' ', '_') + "\n"
    if str(vid_kolesa) != "Диск":
        str_answer = str_answer + "\n🛞 Состояние резины: \n#" + str(sost_rez) + "\n"
        str_answer = str_answer + "#" + str(sost_rez_prich).replace(' ', '_') + "\n"
        if sost_rez == "Утиль":
            str_answer = str_answer + "#" + str(por_nomer_rezina).replace(' ', '_') + "\n"
    str_answer = str_answer + "#" + str(company) + "\n"
    return str_answer

def generating_report_google_sheets(username,company,grz,type_disk,marka_ts,vid_kolesa,radius,razmer,marka_rez,model_rez,sezon,
                                        sost_disk,sost_rez,sost_disk_prich,sost_rez_prich,por_nomer_diska,por_nomer_rezina,message_link,type="",type_check=""):
    count = 1
    if type_check == "Ось":
        count = 2
    elif type_check == "Комплект":
        count = 4
    tlist = list()
    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
    tlist.append(company)
    if type == "check":
        tlist.append("Проверка колеса")
    else:
        tlist.append(vid_kolesa)
    tlist.append(grz)
    tlist.append(marka_ts)
    tlist.append(radius)
    tlist.append(razmer)
    tlist.append(marka_rez)
    tlist.append(model_rez)
    tlist.append(sezon)
    tlist.append(type_disk)
    tlist.append(sost_disk)
    tlist.append(sost_disk_prich)
    tlist.append(por_nomer_diska)
    tlist.append(sost_rez)
    tlist.append(sost_rez_prich)
    tlist.append(por_nomer_rezina)
    tlist.append(message_link)  # ссылка на отчет
    tlist.append(username)
    for i in range(count):
        try:
            with_sheets_retry(
                write_in_answers_ras,
                tlist,
                "Выгрузка ремонты/утиль",
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось записать данные в Выгрузка ремонты/утиль после 3 попыток")
        try:
            with_sheets_retry(
                process_transfer_record,
                tlist,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось записать данные в Остатки Бой после 3 попыток")

@dp.message_handler(state=DamageStates.WAIT_ZAMENA_POKRESHKA)
async def gap(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return
    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")

    if message.text == "Нет":
        await message.answer("Завершение формирования заявки",
                         reply_markup=types.ReplyKeyboardRemove())
        await state.finish()
        return
    elif message.text == "Да":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await SborkaStates.WAIT_RAZMER.set()


@dp.message_handler(state=DamageStates.GAP2)
async def gap2(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    await state.update_data(type_disk=message.text,type_sborka="sborka")

    await message.answer("Уточните какое колесо:",
                         reply_markup=getKeyboardList(key_dir_kolesa))
    await SborkaStates.WAIT_TYPE_KOLESA.set()

@dp.message_handler(state=DamageStates.GAP3)
async def gap3(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Нет":
        message.answer("Завершение формирования заявки",
                         reply_markup=types.ReplyKeyboardRemove())
        await state.finish()
        return

    elif message.text == "Да":
        await message.answer("Тип диска:", reply_markup=getKeyboardStep1(key_type_disk))
        await DamageStates.GAP2.set()

@dp.message_handler(commands=["sborka", "sborka_ko"], state="*")
async def cmd_sborka(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[sborka] User @{user.username} ({user.id}) начал оформление сборки")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username,type_sborka=message.text.split()[0][1:])
    await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
    await SborkaStates.WAIT_COMPANY.set()

@dp.message_handler(state=SborkaStates.WAIT_COMPANY)
async def sborka_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    company=message.text
    if str(company) in key_company:
        await state.update_data(company=company)

    else:
        message.answer("Вы ввели компание не из списка. Повторите выбор:",
                         reply_markup=getKeyboardStep1(key_company))
        await SborkaStates.WAIT_COMPANY.set()
        return

    await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
    await SborkaStates.WAIT_TYPE_DISK.set()

@dp.message_handler(state=SborkaStates.WAIT_TYPE_DISK)
async def sborka_type_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
        await SborkaStates.WAIT_COMPANY.set()
        return

    type_disk = message.text
    await state.update_data(type_disk=type_disk)


    await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
    await SborkaStates.WAIT_RADIUS.set()


@dp.message_handler(state=SborkaStates.WAIT_RADIUS)
async def sborka_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await SborkaStates.WAIT_TYPE_DISK.set()
        return

    radius = message.text
    await state.update_data(radius=radius)


    if not check_validation_radius(company,radius):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРадиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await SborkaStates.WAIT_RADIUS.set()
        return

    await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company,radius))))))
    await SborkaStates.WAIT_RAZMER.set()


@dp.message_handler(state=SborkaStates.WAIT_RAZMER)
async def sborka_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")

    if message.text == "Назад":
        await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await SborkaStates.WAIT_RADIUS.set()
        return

    razmer = message.text
    await state.update_data(razmer=razmer)


    if not check_validation_razmer(company, radius, razmer):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРазмер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company,radius))))))

        await SborkaStates.WAIT_RAZMER.set()
        return

    await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
    await SborkaStates.WAIT_MARKA_REZ.set()


@dp.message_handler(state=SborkaStates.WAIT_MARKA_REZ)
async def sborka_marka_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")

    if message.text == "Назад":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await SborkaStates.WAIT_RAZMER.set()
        return

    marka_rez = message.text
    await state.update_data(marka_rez=marka_rez)

    if not check_validation_marka(company, radius, razmer, marka_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SborkaStates.WAIT_MARKA_REZ.set()
        return

    await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
    await SborkaStates.WAIT_MODEL_REZ.set()


@dp.message_handler(state=SborkaStates.WAIT_MODEL_REZ)
async def sborka_model_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")

    if message.text == "Назад":
        await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SborkaStates.WAIT_MARKA_REZ.set()
        return

    model_rez = message.text
    await state.update_data(model_rez=model_rez)

    if not check_validation_model(company, radius, razmer, marka_rez, model_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМодель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await SborkaStates.WAIT_MODEL_REZ.set()
        return

    await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
    await SborkaStates.WAIT_SEZON.set()

@dp.message_handler(state=SborkaStates.WAIT_SEZON)
async def sborka_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await SborkaStates.WAIT_MODEL_REZ.set()
        return

    sezon = message.text
    await state.update_data(sezon=sezon)

    if not check_validation_sezon(company, radius, razmer, marka_rez, model_rez,sezon):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await SborkaStates.WAIT_SEZON.set()
        return

    await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
    await SborkaStates.WAIT_MARKA_TS.set()


@dp.message_handler(state=SborkaStates.WAIT_MARKA_TS)
async def sborka_marka_ts(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    type_sborka = data.get("type_sborka", "")

    if message.text == "Назад":
        await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await SborkaStates.WAIT_SEZON.set()
        return

    marka_ts = message.text
    await state.update_data(marka_ts=marka_ts)

    if not check_validation_marka_ts(company, marka_ts):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SborkaStates.WAIT_MARKA_TS.set()
        return

    if type_sborka == "sborka":
        await message.answer("Уточните какое колесо:",
                         reply_markup=getKeyboardList(key_dir_kolesa))
    else:
        await message.answer("Вид сборки:",
                         reply_markup=getKeyboardList(key_type_sborka))
    await SborkaStates.WAIT_TYPE_KOLESA.set()

@dp.message_handler(state=SborkaStates.WAIT_TYPE_KOLESA)
async def sborka_type_kolesa(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SborkaStates.WAIT_MARKA_TS.set()
        return

    type_kolesa = message.text
    await state.update_data(type_kolesa=type_kolesa)

    await message.answer("Сбор под заявку:", reply_markup=getKeyboardList(key_sogl))
    await SborkaStates.WAIT_ZAYAVKA.set()

@dp.message_handler(state=SborkaStates.WAIT_ZAYAVKA)
async def sborka_zayavka(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sezon = data.get("sezon", "")
    marka_ts = data.get("marka_ts", "")
    type_sborka = data.get("type_sborka", "")
    type_disk = data.get("type_disk", "")
    type_kolesa = data.get("type_kolesa", "")

    if message.text == "Назад":
        if type_sborka == "sborka":
            await message.answer("Уточните какое колесо:",
                                 reply_markup=getKeyboardList(key_dir_kolesa))
        else:
            await message.answer("Вид сборки:",
                                 reply_markup=getKeyboardList(key_type_sborka))
        await SborkaStates.WAIT_TYPE_KOLESA.set()
        return

    zayavka = message.text
    await state.update_data(zayavka=zayavka)

    if str(zayavka) == "Да":
        try:
            if type_sborka == "sborka":
                list_sborki = with_sheets_retry(
                    nomer_sborka,
                    company, radius, razmer, marka_rez,
                    model_rez, sezon, marka_ts, type_disk, type_kolesa,
                    max_attempts=5, base_delay=2
                )
            else:
                list_sborki = with_sheets_retry(
                    nomer_sborka_ko,
                    company, radius, razmer, marka_rez,
                    model_rez, sezon, marka_ts, type_disk, type_kolesa,
                    max_attempts=5, base_delay=2
                )
        except RuntimeError:
            logger.error("Не удалось получить номер сборки после 5 попыток")
            return await message.answer(
                "Не удалось получить номер заявки. Попробуйте чуть позже."
            )
        if not list_sborki:
            await message.answer("Не найдена сборка, подходящая под введенные параметры\nСбор под заявку:", reply_markup=getKeyboardList(key_sogl))
            await SborkaStates.WAIT_ZAYAVKA.set()
            return
        await message.answer("Номера актуальных заказов:",
                             reply_markup=getKeyboardList(list_sborki))
        await SborkaStates.WAIT_NOMER_ZAYAVKA.set()
        return

    await message.answer("Прикрепите от 1 до 4 фото в одном сообщение. И нажмите на кнопку Готово",
                 reply_markup=_keyboard_done_exit())
    await SborkaStates.WAIT_FILES.set()


@dp.message_handler(state=SborkaStates.WAIT_NOMER_ZAYAVKA)
async def sborka_nomer_sborka(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sezon = data.get("sezon", "")
    marka_ts = data.get("marka_ts", "")
    type_disk = data.get("type_disk", "")
    type_kolesa = data.get("type_kolesa", "")
    type_sborka = data.get("type_sborka", "")

    if message.text == "Назад":
        await message.answer("Сбор под заявку:", reply_markup=getKeyboardList(key_sogl))
        await SborkaStates.WAIT_ZAYAVKA.set()
        return

    nomer_sborka_in = message.text

    if type_sborka =="sborka_ko" and str(nomer_sborka_in) in nomer_sborka_ko(company,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa):
        await state.update_data(nomer_sborka=nomer_sborka_in)

        await message.answer("Прикрепите от 1 до 4 фото в одном сообщение. И нажмите на кнопку Готово",
                             reply_markup=getKeyboardList(key_ready))
        await SborkaStates.WAIT_FILES.set()
        return

    if str(nomer_sborka_in) in nomer_sborka(company,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa):
        await state.update_data(nomer_sborka=nomer_sborka_in)

        await message.answer("Прикрепите от 1 до 4 фото в одном сообщение. И нажмите на кнопку Готово",
                             reply_markup=getKeyboardList(key_ready))
        await SborkaStates.WAIT_FILES.set()
        return

    await message.answer("Сбор под заявку:", reply_markup=getKeyboardList(key_sogl))
    await SborkaStates.WAIT_ZAYAVKA.set()

@dp.message_handler(
    content_types=[ContentType.PHOTO, ContentType.DOCUMENT, ContentType.VIDEO],
    state=SborkaStates.WAIT_FILES
)
async def collect_files_sborka(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    # Определяем тип и file_id
    if message.photo:
        file_type = 'photo'
        file_id = message.photo[-1].file_id
    elif message.document:
        file_type = 'document'
        file_id = message.document.file_id
    elif message.video:
        file_type = 'video'
        file_id = message.video.file_id
    else:
        return  # неожиданный content_type

    files.append({'type': file_type, 'media': file_id})
    await state.update_data(files=files)

@dp.message_handler(lambda msg: msg.text == "Назад", state=SborkaStates.WAIT_FILES)
async def files_nazad_sborka(message: types.Message, state: FSMContext):
    await message.answer("Сбор под заявку:", reply_markup=getKeyboardList(key_sogl))
    await SborkaStates.WAIT_ZAYAVKA.set()
    return


@dp.message_handler(lambda msg: msg.text == "Готово", state=SborkaStates.WAIT_FILES)
async def finalize_sborka(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    if not 1 <= len(files) <= 4:
        return await message.answer("Нужно прикрепить от 1 до 4 файлов")

    username = data.get("username", "")
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sezon = data.get("sezon", "")
    marka_ts = data.get("marka_ts", "")
    type_sborka = data.get("type_sborka", "")
    type_disk = data.get("type_disk", "")
    type_kolesa = data.get("type_kolesa", "")
    type_check = data.get("type_kolesa", "")
    nomer_sborka = data.get("nomer_sborka", "")
    zayavka = data.get("zayavka", "")
    type = data.get("type", "")

    if type_sborka == "sborka":
        update_data_sborka(marka_rez, model_rez,type_disk, type_kolesa, nomer_sborka)

    files[0]['caption'] = generating_report_tg_sborka(company,username,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa,nomer_sborka,zayavka, type, type_check)
    if company == "СитиДрайв":
        chatId = chat_id_sborka_Sity
        chat_id_for_link = link_sborka_Sity
        thread_id = thread_id_sborka_Sity
    elif company == "Яндекс":
        chatId = chat_id_sborka_Yandex
        chat_id_for_link = link_sborka_Yandex
        thread_id = thread_id_sborka_Yandex
    else:
        chatId = chat_id_sborka_Belka
        chat_id_for_link = link_sborka_Belka
        thread_id = thread_id_sborka_Belka
    dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(files)}
    resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
    data = resp.json()
    message_id = data["result"][0]["message_id"]
    message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
    status_code = resp.status_code
    if type != "check":
        generating_report_google_sheets_sborka(company,username,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa,message_link,nomer_sborka,zayavka)
    if len(nomer_sborka) and str(nomer_sborka) != "не найден":
        try:
            with_sheets_retry(
                update_record_sborka,
                company,username,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa,message_link,nomer_sborka,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось обновить данные в сборке после 3 попыток")
        await print_zayavka()
    elif str(nomer_sborka) == "не найден":
        await message.answer('По Вашей сборке не найдена заявка')
    if status_code < 400:
        await message.answer('Ваше заявка сформирована')
    else:
        await message.answer('При формировании заявки произошла ошибка')
    await state.finish()


def generating_report_tg_sborka(company,username,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa,nomer_sborka,zayavka, type, type_check=""):
    str_answer = " "
    if type == "check":
        if type_check == "Ось":
            str_answer = str_answer + "Проверка готовой оси\n\n"
        elif type_check == "Комплект":
            str_answer = str_answer + "Проверка готового комплекта\n\n"
        else:
            str_answer = str_answer + "Проверка готового колеса\n\n"
    str_answer = str_answer + "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
    str_answer = str_answer + "👷 @" + str(username) + "\n\n"
    str_answer = str_answer + "🚗 " + str(marka_ts) + "\n\n"
    str_answer = str_answer + "🛞 " + str(marka_rez) + " " + str(model_rez) + "\n\n"
    str_answer = str_answer + str(razmer) + "/" + str(radius) + "\n"
    if str(sezon).split(' ')[0] == "Лето":
        str_answer = str_answer + "☀️ " + str(sezon) + "\n"
    elif str(sezon).split(' ')[0] == "Зима":
        str_answer = str_answer + "❄️ " + str(sezon) + "\n"
    else:
        str_answer = str_answer + str(sezon) + "\n"
    str_answer = str_answer + str(type_disk) + "\n"
    if str(type_kolesa) == "Левое":
        str_answer = str_answer + "⬅️ " + str(type_kolesa) + "\n"
    elif str(type_kolesa) == "Правое":
        str_answer = str_answer + "➡️ " + str(type_kolesa) + "\n"
    elif str(type_kolesa) == "Ось":
        str_answer = str_answer + "↔️ " + str(type_kolesa) + "\n"
    elif str(type_kolesa) == "Комплект":
        str_answer = str_answer + "🔄 " + str(type_kolesa) + "\n"
    str_answer = str_answer + "\n#" + str(company) + "\n"
    str_answer = str_answer + "\n📝 Сбор под заявку: " + str(zayavka) + "\n"
    if len(str(nomer_sborka)) > 1:
        str_answer = str_answer + "\n#️⃣ Номер заявки: " + str(nomer_sborka) + "\n"
    return str_answer



def generating_report_google_sheets_sborka(company,username,radius,razmer,marka_rez,model_rez,sezon,marka_ts,type_disk,type_kolesa,message_link,nomer_sborka,zayavka):
    tlist = []
    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
    tlist.append(company)
    tlist.append(marka_ts)
    tlist.append(radius)
    tlist.append(razmer)
    tlist.append(marka_rez)
    tlist.append(model_rez)
    tlist.append(sezon)
    tlist.append(type_disk)
    tlist.append(type_kolesa)
    tlist.append(zayavka)
    tlist.append(nomer_sborka)
    tlist.append(message_link)  # ссылка на отчёт
    tlist.append(username)

    pos = type_kolesa
    if pos not in ("Комплект", "Ось"):
        try:
            with_sheets_retry(
                write_in_answers_ras,
                tlist,
                "Выгрузка сборка",
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось записать данные в Выгрузка сборка после 3 попыток")
        try:
            with_sheets_retry(
                write_in_answers_ras,
                tlist,
                "Онлайн остатки Хаба",
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось записать данные в Онлайн остатки Хаба после 3 попыток")
    else:
        if pos == "Комплект":
            for side, count in (("Правое", 2), ("Левое", 2)):
                for _ in range(count):
                    new_list = tlist.copy()
                    new_list[9] = side
                    try:
                        with_sheets_retry(
                            write_in_answers_ras,
                            new_list,
                            "Выгрузка сборка",
                            max_attempts=3,
                            base_delay=2
                        )
                    except RuntimeError:
                        logger.error("Не удалось записать данные в Выгрузка сборка после 3 попыток")
                    try:
                        with_sheets_retry(
                            write_in_answers_ras,
                            new_list,
                            "Онлайн остатки Хаба",
                            max_attempts=3,
                            base_delay=2
                        )
                    except RuntimeError:
                        logger.error("Не удалось записать данные в Онлайн остатки Хаба после 3 попыток")
        elif pos == "Ось":
            for side in ("Правое", "Левое"):
                new_list = tlist.copy()
                new_list[9] = side
                try:
                    with_sheets_retry(
                        write_in_answers_ras,
                        new_list,
                        "Выгрузка сборка",
                        max_attempts=3,
                        base_delay=2
                    )
                except RuntimeError:
                    logger.error("Не удалось записать данные в Выгрузка сборка после 3 попыток")
                try:
                    with_sheets_retry(
                        write_in_answers_ras,
                        new_list,
                        "Онлайн остатки Хаба",
                        max_attempts=3,
                        base_delay=2
                    )
                except RuntimeError:
                    logger.error("Не удалось записать данные в Онлайн остатки Хаба после 3 попыток")

@dp.message_handler(commands=["soberi"], state="*")
async def cmd_soberi(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[soberi] User @{user.username} ({user.id}) начал оформление заявки на сборку")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)
    await message.answer("Укажите, что собираем:", reply_markup=getKeyboardStep1(key_object))
    await SoberiStates.WAIT_TYPE_SOBERI.set()

@dp.message_handler(commands=["soberi_belka"], state="*")
async def cmd_soberi_belka(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    # if message.from_user.id not in list_users_belka:
    #     return await message.answer("У вас нет прав для вызова данной команды")

    user = message.from_user
    logger.info(f"[soberi_belka] User @{user.username} ({user.id}) начал оформление заявки на сборку Белка")

    await state.finish()
    # фиксируем компанию и флаг preset_company, чтобы ниже хендлеры знали, что шаг компании пропущен
    await state.update_data(user_id=user.id, username=user.username, company="Белка", preset_company=True)
    await message.answer("Укажите, что собираем:", reply_markup=getKeyboardStep1(key_object))
    await SoberiStates.WAIT_TYPE_SOBERI.set()

@dp.message_handler(state=SoberiStates.WAIT_TYPE_SOBERI)
async def soberi_type_soberi(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    await state.update_data(type_soberi=message.text)
    data = await state.get_data()
    if data.get("preset_company"):
        company = data.get("company", "Белка")
        await message.answer(
            "Марка автомобиля:",
            reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company)))))
        )
        return await SoberiStates.WAIT_MARKA_TS.set()
    # Обычный сценарий — просим выбрать компанию
    await message.answer("Компания:", reply_markup=getKeyboardList(key_company_small))
    await SoberiStates.WAIT_COMPANY.set()

@dp.message_handler(state=SoberiStates.WAIT_COMPANY)
async def soberi_type_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Укажите, что собираем:", reply_markup=getKeyboardStep1(key_object))
        await SoberiStates.WAIT_TYPE_SOBERI.set()
        return

    company = message.text
    await state.update_data(company=company)

    await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
    await SoberiStates.WAIT_MARKA_TS.set()


@dp.message_handler(state=SoberiStates.WAIT_MARKA_TS)
async def soberi_marka_ts(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        data = await state.get_data()
        if data.get("preset_company"):
            await message.answer("Укажите, что собираем:", reply_markup=getKeyboardStep1(key_object))
            await SoberiStates.WAIT_TYPE_SOBERI.set()
            return
        await message.answer("Компания:", reply_markup=getKeyboardList(key_company_small))
        await SoberiStates.WAIT_COMPANY.set()
        return

    marka_ts = message.text
    await state.update_data(marka_ts=marka_ts)

    data = await state.get_data()
    company = data.get("company", "")
    type_soberi = data.get("type_soberi", "")

    if not check_validation_marka_ts(company, marka_ts):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SoberiStates.WAIT_MARKA_TS.set()
        return

    if type_soberi == "Комплект":
        await message.answer("Радиус:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await SoberiStates.WAIT_RADIUS.set()
    else:
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await SoberiStates.WAIT_TYPE_DISK.set()

@dp.message_handler(state=SoberiStates.WAIT_TYPE_DISK)
async def soberi_type_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SoberiStates.WAIT_MARKA_TS.set()
        return

    await state.update_data(type_disk=message.text)

    await message.answer("Радиус:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
    await SoberiStates.WAIT_RADIUS.set()


@dp.message_handler(state=SoberiStates.WAIT_RADIUS)
async def soberi_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    type_soberi = data.get("type_soberi", "")

    if message.text == "Назад" and type_soberi == "Комплект":
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SoberiStates.WAIT_MARKA_TS.set()
        return
    elif message.text == "Назад" and (type_soberi == "Ось" or type_soberi == "Колесо"):
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await SoberiStates.WAIT_TYPE_DISK.set()
        return

    radius = message.text
    await state.update_data(radius=radius)

    if not check_validation_radius(company, radius):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРадиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await SoberiStates.WAIT_RADIUS.set()
        return

    await message.answer("Размер:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
    await SoberiStates.WAIT_RAZMER.set()


@dp.message_handler(state=SoberiStates.WAIT_RAZMER)
async def soberi_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    type_soberi = data.get("type_soberi", "")
    radius = data.get("radius", "")

    if message.text == "Назад" and type_soberi == "Комплект":
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SoberiStates.WAIT_MARKA_TS.set()
        return
    elif message.text == "Назад" and (type_soberi == "Ось" or type_soberi == "Колесо"):
        await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await SoberiStates.WAIT_RADIUS.set()
        return

    razmer = message.text
    await state.update_data(razmer=razmer)

    if not check_validation_razmer(company, radius, razmer):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРазмер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await SoberiStates.WAIT_RAZMER.set()
        return

    if type_soberi == "Колесо":
        await message.answer("Марка резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SoberiStates.WAIT_MARKA_REZ.set()
    else:
        await message.answer("Сезонность резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer,"", "", 1))))))
        await SoberiStates.WAIT_SEZON.set()


@dp.message_handler(state=SoberiStates.WAIT_MARKA_REZ)
async def soberi_marka_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    razmer = data.get("razmer", "")
    radius = data.get("radius", "")

    if message.text == "Назад":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await SoberiStates.WAIT_RAZMER.set()
        return

    marka_rez = message.text
    await state.update_data(marka_rez=marka_rez)

    if not check_validation_marka(company, radius, razmer, marka_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SoberiStates.WAIT_MARKA_REZ.set()
        return
    await message.answer("Модель резины:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
    await SoberiStates.WAIT_MODEL_REZ.set()

@dp.message_handler(state=SoberiStates.WAIT_MODEL_REZ)
async def soberi_model_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    razmer = data.get("razmer", "")
    radius = data.get("radius", "")
    marka_rez = data.get("marka_rez", "")

    if message.text == "Назад":
        await message.answer("Марка резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SoberiStates.WAIT_MARKA_REZ.set()
        return

    model_rez = message.text
    await state.update_data(model_rez=model_rez)

    if not check_validation_model(company, radius, razmer, marka_rez, model_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМодель резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SoberiStates.WAIT_MARKA_REZ.set()
        return

    await message.answer("Сезонность резины:",
                         reply_markup=getKeyboardList(
                             sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
    await SoberiStates.WAIT_SEZON.set()

@dp.message_handler(state=SoberiStates.WAIT_SEZON)
async def soberi_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    type_soberi = data.get("type_soberi", "")
    razmer = data.get("razmer", "")
    radius = data.get("radius", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад" and type_soberi == "Колесо":
        await message.answer("Марка резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await SoberiStates.WAIT_MARKA_REZ.set()
        return
    elif message.text == "Назад" and (type_soberi == "Ось" or type_soberi == "Комплект"):
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await SoberiStates.WAIT_RAZMER.set()
        return

    sezon = message.text
    await state.update_data(sezon=sezon)

    if (type_soberi == "Ось" or type_soberi == "Комплект") and not check_validation_sezon(company, radius, razmer, marka_rez, model_rez, sezon, 1):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                                 reply_markup=getKeyboardList(
                                     sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await SoberiStates.WAIT_SEZON.set()
        return
    if type_soberi == "Колесо" and not check_validation_sezon(company, radius, razmer, marka_rez, model_rez, sezon):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                                 reply_markup=getKeyboardList(
                                     sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await SoberiStates.WAIT_SEZON.set()
        return
    if type_soberi == "Комплект":
        await message.answer("Количество комплектов:", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_1.set()
        return
    elif type_soberi == "Ось":
        await message.answer("Количество осей:", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_1.set()
        return
    else:
        await message.answer("Уточните количество левых колес:", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_1.set()
        return

@dp.message_handler(state=SoberiStates.WAIT_COUNT_1)
async def soberi_count_1(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    type_soberi = data.get("type_soberi", "")
    razmer = data.get("razmer", "")
    radius = data.get("radius", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Сезонность резины:",
                                 reply_markup=getKeyboardList(
                                     sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await SoberiStates.WAIT_SEZON.set()
        return
    try:
        count_1 = int(message.text)
        await state.update_data(count_1=count_1)

    except Exception as e:
        await message.answer("Неверный формат целого числа. Попробуйте снова", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_1.set()
        return

    if type_soberi == "Комплект" or type_soberi == "Ось":
        await _soberi_ask_comment(message, state)
        return
    else:
        await message.answer("Уточните количество правых колес:", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_2.set()


@dp.message_handler(state=SoberiStates.WAIT_COUNT_2)
async def soberi_count_2(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Уточните количество левых колес:", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_1.set()
        return
    try:
        count_2 = int(message.text)
        await state.update_data(count_2=count_2)

    except Exception as e:
        await message.answer("Неверный формат целого числа. Попробуйте снова", reply_markup=getKeyboardList(key_chisla))
        await SoberiStates.WAIT_COUNT_2.set()
        return

    await _soberi_ask_comment(message, state)
    return

def _keyboard_comment_skip_exit():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("Пропустить")
    kb.add("Выход")
    return kb

async def _soberi_ask_comment(message: types.Message, state: FSMContext):
    await message.answer(
        "Комментарий (можно пропустить):",
        reply_markup=_keyboard_comment_skip_exit()
    )
    await SoberiStates.WAIT_COMMENT.set()

@dp.message_handler(state=SoberiStates.WAIT_COMMENT)
async def soberi_comment(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    text = (message.text or "").strip()
    if text.lower() == "пропустить":
        comment = ""
    else:
        comment = text

    await state.update_data(comment=comment)

    # Финализация: запись в GS + уведомление
    await generating_report_soberi(message, state)
    await message.answer(
        "Заявка на сборку оформлена",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await print_zayavka()
    await state.finish()
    return

async def generating_report_soberi(message: types.Message, state: FSMContext):
    sum_list = []
    data = await state.get_data()
    type_soberi = data.get("type_soberi", "")
    count_1 = data.get("count_1", "")
    count_2 = data.get("count_2", "")
    if type_soberi == "Колесо":
        left = count_1
        right = count_2
        try:
            nomer = with_sheets_retry(
                get_max_nomer_sborka,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось получить номер сборки после 3 попыток")
            return await message.answer(
                "Не удалось получить номер сборки . Попробуйте чуть позже."
            )
        nomer = nomer + 1
        while left:
            row = await generating_report_google_sheets_soberi(message, state, "Левое", "sb"+ str(nomer))
            sum_list.append(row)
            nomer = nomer + 1
            left = left - 1
        while right:
            row = await generating_report_google_sheets_soberi(message, state, "Правое", "sb"+ str(nomer))
            sum_list.append(row)
            nomer = nomer + 1
            right = right - 1
    elif type_soberi == "Комплект":
        komplekt = count_1
        try:
            nomer = with_sheets_retry(
                get_max_nomer_sborka,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось получить номер сборки после 3 попыток")
            return await message.answer(
                "Не удалось получить номер сборки . Попробуйте чуть позже."
            )
        nomer = nomer + 1
        while komplekt:
            left = 2
            right = 2
            while left:
                row = await generating_report_google_sheets_soberi(message, state, "Левое", "sb" + str(nomer))
                sum_list.append(row)
                left = left - 1
            while right:
                row = await generating_report_google_sheets_soberi(message, state, "Правое", "sb" + str(nomer))
                sum_list.append(row)
                right = right - 1
            komplekt = komplekt - 1
            nomer = nomer + 1
    else:
        osi = count_1
        try:
            nomer = with_sheets_retry(
                get_max_nomer_sborka,
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось получить номер сборки после 3 попыток")
            return await message.answer(
                "Не удалось получить номер сборки . Попробуйте чуть позже."
            )
        nomer = nomer + 1
        while osi:
            row = await generating_report_google_sheets_soberi(message, state, "Левое", "sb" + str(nomer))
            sum_list.append(row)
            row = await generating_report_google_sheets_soberi(message, state, "Правое", "sb" + str(nomer))
            sum_list.append(row)
            osi = osi - 1
            nomer = nomer + 1
    try:
        with_sheets_retry(
            write_soberi_in_google_sheets_rows,
            sum_list,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось сделать запись в Гугл Таблицу после 3 попыток")
        return await message.answer(
            "Не удалось сделать запись в Гугл Таблицу. Попробуйте чуть позже."
        )


async def generating_report_google_sheets_soberi(message: types.Message, state: FSMContext, position,nomer):
    data = await state.get_data()
    tlist = list()
    tlist.append("")
    tlist.append("")
    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
    tlist.append(data.get("company", ""))
    tlist.append(data.get("marka_ts", ""))
    tlist.append(data.get("radius", ""))
    tlist.append(data.get("razmer", ""))
    tlist.append(data.get("marka_rez", ""))
    tlist.append(data.get("model_rez", ""))
    tlist.append(data.get("sezon", ""))
    tlist.append(data.get("type_disk", ""))
    tlist.append(position)
    tlist.append("")
    tlist.append(nomer)
    tlist.append(data.get("username", ""))
    tlist.append("")
    tlist.append("")
    tlist.append("")
    tlist.append("")
    tlist.append(data.get("comment", ""))
    return tlist

@dp.message_handler(commands=["demounting"], state="*")
async def cmd_demounting(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[demounting] User @{user.username} ({user.id}) начал оформление заявки на демонтаж")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)
    await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
    await DemountingStates.WAIT_COMPANY.set()

@dp.message_handler(state=DemountingStates.WAIT_COMPANY)
async def demounting_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    company = message.text
    if str(company) in key_company:
        await state.update_data(company=company)
    else:
        await message.answer("Вы ввели компание не из списка. Повторите выбор:",
                         reply_markup=getKeyboardStep1(key_company))
        await DemountingStates.WAIT_COMPANY.set()
        return

    await message.answer("Сколько колес демонтируется:",
                             reply_markup=getKeyboardList(key_count))
    await DemountingStates.WAIT_COUNT.set()

@dp.message_handler(state=DemountingStates.WAIT_COUNT)
async def demounting_count(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
        await DemountingStates.WAIT_COMPANY.set()
        return

    count = message.text
    await state.update_data(count=count)

    data = await state.get_data()
    company = data.get("company", "")

    await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
    await DemountingStates.WAIT_MARKA_TS.set()

@dp.message_handler(state=DemountingStates.WAIT_MARKA_TS)
async def demounting_marka_ts(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Сколько колес демонтируется:",
                             reply_markup=getKeyboardList(key_count))
        await DemountingStates.WAIT_COUNT.set()

    marka_ts = message.text
    await state.update_data(marka_ts=marka_ts)

    if not check_validation_marka_ts(company, marka_ts):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await DemountingStates.WAIT_MARKA_TS.set()
        return

    await message.answer("Радиус:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
    await DemountingStates.WAIT_RADIUS.set()


@dp.message_handler(state=DemountingStates.WAIT_RADIUS)
async def demounting_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await DemountingStates.WAIT_MARKA_TS.set()
        return

    radius = message.text
    await state.update_data(radius=radius)

    if not check_validation_radius(company, radius):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРадиус:",
                     reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DemountingStates.WAIT_MARKA_TS.set()
        return

    await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
    await DemountingStates.WAIT_RAZMER.set()

@dp.message_handler(state=DemountingStates.WAIT_RAZMER)
async def demounting_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")

    if message.text == "Назад":
        await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await DemountingStates.WAIT_RADIUS.set()
        return

    razmer = message.text
    await state.update_data(razmer=razmer)

    if not check_validation_razmer(company, radius, razmer):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРазмер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await DemountingStates.WAIT_RAZMER.set()
        return

    await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
    await DemountingStates.WAIT_MARKA_REZ.set()

@dp.message_handler(state=DemountingStates.WAIT_MARKA_REZ)
async def demounting_marka_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")

    if message.text == "Назад":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await DemountingStates.WAIT_RAZMER.set()
        return

    marka_rez = message.text
    await state.update_data(marka_rez=marka_rez)

    if not check_validation_marka(company, radius, razmer, marka_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await DemountingStates.WAIT_MARKA_REZ.set()
        return

    await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
    await DemountingStates.WAIT_MODEL_REZ.set()

#
@dp.message_handler(state=DemountingStates.WAIT_MODEL_REZ)
async def demounting_model_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")

    if message.text == "Назад":
        await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await DemountingStates.WAIT_MARKA_REZ.set()
        return

    model_rez = message.text
    await state.update_data(model_rez=model_rez)

    if not check_validation_model(company, radius, razmer, marka_rez, model_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМодель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await DemountingStates.WAIT_MODEL_REZ.set()
        return

    await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
    await DemountingStates.WAIT_SEZON.set()

@dp.message_handler(state=DemountingStates.WAIT_SEZON)
async def demounting_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await DemountingStates.WAIT_MODEL_REZ.set()
        return

    sezon = message.text
    await state.update_data(sezon=sezon)

    if not check_validation_sezon(company, radius, razmer, marka_rez, model_rez, sezon):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await DemountingStates.WAIT_SEZON.set()
        return

    await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
    await DemountingStates.WAIT_TYPE_DISK.set()


@dp.message_handler(state=DemountingStates.WAIT_TYPE_DISK)
async def demounting_type_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")

    if message.text == "Назад":
        await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await DemountingStates.WAIT_SEZON.set()
        return

    type_disk = message.text
    await state.update_data(type_disk=type_disk)

    await generating_report_google_sheets_gen(message, state)
    await message.answer("Демонтаж оформлен", reply_markup = types.ReplyKeyboardRemove())

async def generating_report_google_sheets_gen(message: types.Message, state: FSMContext):
    tlist = list()
    data = await state.get_data()
    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
    tlist.append(data.get("company", ""))
    tlist.append("")
    tlist.append("")
    tlist.append(data.get("marka_ts", ""))
    tlist.append(data.get("radius", ""))
    tlist.append(data.get("razmer", ""))
    tlist.append(data.get("marka_rez", ""))
    tlist.append(data.get("model_rez", ""))
    tlist.append(data.get("sezon", ""))
    tlist.append(data.get("type_disk", ""))
    tlist.append("")
    tlist.append("")
    tlist.append("")
    tlist.append("Демонтаж")
    tlist.append("")
    tlist.append("")
    tlist.append("")  # ссылка на отчет
    tlist.append(data.get("username", ""))
    count = 0
    if data.get("count", "") == "1 колесо":
        count = 1
    elif data.get("count", "") == "Ось":
        count = 2
    elif data.get("count", "") == "Комплект":
        count = 4
    for  _ in range(count):
        try:
            with_sheets_retry(
                write_in_answers_ras,
                tlist,
                "Выгрузка ремонты/утиль",
                max_attempts=3,
                base_delay=2
            )
        except RuntimeError:
            logger.error("Не удалось записать данные в Выгрузка ремонты/утиль после 3 попыток")

@dp.message_handler(commands=["check"], state="*")
async def cmd_check(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[check] User @{user.username} ({user.id}) начал оформление check")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)
    await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
    await CheckStates.WAIT_COMPANY.set()

@dp.message_handler(state=CheckStates.WAIT_COMPANY)
async def check_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    company=message.text
    if str(company) in key_company:
        await state.update_data(company=company)

    else:
        message.answer("Вы ввели компание не из списка. Повторите выбор:",
                         reply_markup=getKeyboardStep1(key_company))
        await CheckStates.WAIT_COMPANY.set()
        return

    await message.answer("Марка автомобиля:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
    await CheckStates.WAIT_MARKA_TS.set()

@dp.message_handler(state=CheckStates.WAIT_MARKA_TS)
async def check_marka_ts(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
        await CheckStates.WAIT_COMPANY.set()
        return

    marka_ts = message.text
    await state.update_data(marka_ts=marka_ts)
    data = await state.get_data()
    company = data.get("company", "")

    if not check_validation_marka_ts(company, marka_ts):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await SborkaStates.WAIT_MARKA_TS.set()
        return

    await message.answer("Укажите, что проверяем:", reply_markup=getKeyboardList(key_type_check))
    await CheckStates.WAIT_TYPE_CHECK.set()

@dp.message_handler(state=CheckStates.WAIT_TYPE_CHECK)
async def check_type_check(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        data = await state.get_data()
        company = data.get("company", "")
        await message.answer("Марка автомобиля:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_ts(company))))))
        await CheckStates.WAIT_MARKA_TS.set()
        return

    type_check = message.text
    words = type_check.split()
    base = words[0]

    if base != "Левое" and base != "Правое":
        await state.update_data(type_check=type_check, type_kolesa=base, type_sborka="sborka_ko")
    else:
        await state.update_data(type_check=type_check, type_kolesa=base, type_sborka="sborka")

    await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
    await CheckStates.WAIT_TYPE_DISK.set()

@dp.message_handler(state=CheckStates.WAIT_TYPE_DISK)
async def check_type_disk(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Укажите, что проверяем:", reply_markup=getKeyboardList(key_type_check))
        await CheckStates.WAIT_TYPE_CHECK.set()
        return

    type_disk = message.text
    await state.update_data(type_disk=type_disk)


    await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
    await CheckStates.WAIT_RADIUS.set()


@dp.message_handler(state=CheckStates.WAIT_RADIUS)
async def check_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Тип диска:", reply_markup=getKeyboardList(key_type_disk))
        await CheckStates.WAIT_TYPE_DISK.set()
        return

    radius = message.text
    await state.update_data(radius=radius)


    if not check_validation_radius(company,radius):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРадиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await CheckStates.WAIT_RADIUS.set()
        return

    await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company,radius))))))
    await CheckStates.WAIT_RAZMER.set()


@dp.message_handler(state=CheckStates.WAIT_RAZMER)
async def check_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")

    if message.text == "Назад":
        await message.answer("Радиус:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_radius(company))))))
        await CheckStates.WAIT_RADIUS.set()
        return

    razmer = message.text
    await state.update_data(razmer=razmer)


    if not check_validation_razmer(company, radius, razmer):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nРазмер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company,radius))))))

        await CheckStates.WAIT_RAZMER.set()
        return

    await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
    await CheckStates.WAIT_MARKA_REZ.set()


@dp.message_handler(state=CheckStates.WAIT_MARKA_REZ)
async def check_marka_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")

    if message.text == "Назад":
        await message.answer("Размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(company, radius))))))
        await CheckStates.WAIT_RAZMER.set()
        return

    marka_rez = message.text
    await state.update_data(marka_rez=marka_rez)

    if not check_validation_marka(company, radius, razmer, marka_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМарка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await CheckStates.WAIT_MARKA_REZ.set()
        return

    await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
    await CheckStates.WAIT_MODEL_REZ.set()


@dp.message_handler(state=CheckStates.WAIT_MODEL_REZ)
async def check_model_rez(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")

    if message.text == "Назад":
        await message.answer("Марка резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka(company, radius, razmer))))))
        await CheckStates.WAIT_MARKA_REZ.set()
        return

    model_rez = message.text
    await state.update_data(model_rez=model_rez)

    if not check_validation_model(company, radius, razmer, marka_rez, model_rez):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nМодель резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await CheckStates.WAIT_MODEL_REZ.set()
        return

    await message.answer("Сезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
    await CheckStates.WAIT_SEZON.set()

@dp.message_handler(state=CheckStates.WAIT_SEZON)
async def check_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    username = data.get("username", "")
    company = data.get("company", "")
    grz = data.get("grz", "")
    type_disk = data.get("type_disk", "")
    marka_ts = data.get("marka_ts", "")
    vid_kolesa = data.get("vid_kolesa", "")
    radius = data.get("radius", "")
    razmer = data.get("razmer", "")
    marka_rez = data.get("marka_rez", "")
    model_rez = data.get("model_rez", "")
    sost_disk = data.get("sost_disk", "")
    sost_rez = data.get("sost_rez", "")
    sost_disk_prich = data.get("sost_disk_prich", "")
    sost_rez_prich = data.get("sost_rez_prich", "")
    type_check = data.get("type_check", "")
    type="check"

    por_nomer_diska = ""
    por_nomer_rezina = ""
    message_link = ""

    if message.text == "Назад":
        await message.answer("Модель резины:",
                             reply_markup=getKeyboardList(
                                 sorted(list(set(get_list_model(company, radius, razmer, marka_rez))))))
        await CheckStates.WAIT_MODEL_REZ.set()
        return

    sezon = message.text
    await state.update_data(sezon=sezon,type=type)

    if not check_validation_sezon(company, radius, razmer, marka_rez, model_rez,sezon):
        await message.answer("Введенного значения нет в базе. Попробуйте еще\nСезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon(company, radius, razmer, marka_rez, model_rez))))))
        await CheckStates.WAIT_SEZON.set()
        return

    generating_report_google_sheets(username, company, grz, type_disk, marka_ts, vid_kolesa, radius, razmer, marka_rez,
                                    model_rez, sezon,
                                    sost_disk, sost_rez, sost_disk_prich, sost_rez_prich, por_nomer_diska,
                                    por_nomer_rezina, message_link, type, type_check)

    await message.answer("Сбор под заявку:", reply_markup=getKeyboardList(key_sogl))
    await SborkaStates.WAIT_ZAYAVKA.set()



@dp.message_handler(commands=["start_job_shift"], state="*")
async def cmd_start_job_shift(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[start_job_shift] User @{user.username} ({user.id}) начал оформление заявки на start_job_shift")

    await state.finish()
    await state.update_data(user_id=user.id, username=user.username)
    try:
        data = {'chat_id': str(message.from_user.id)}
        resp = requests.get(URL_GET_FIO, data=data)
        rep = resp.json()
        await state.update_data(fio=_safe_fullname_from_profile(rep, message))
    except Exception as e:
        logging.exception(e)
        await message.answer("Возникла ошибка чтения Ваших данных из базы КК. Убедитесь, что Вы успешно прошли регистрацию и повторите попытку оформления заявки. При повторном возникновении ошибки обратитесь к разработчикам")

    await message.answer("Прикретите фото. И нажмите на кнопку Готово", reply_markup=_keyboard_done_exit())
    await StartJobShiftStates.WAIT_FILES.set()

@dp.message_handler(
    content_types=[ContentType.PHOTO, ContentType.DOCUMENT, ContentType.VIDEO],
    state=StartJobShiftStates.WAIT_FILES
)
async def collect_files_start_job_shift(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    # Определяем тип и file_id
    if message.photo:
        file_type = 'photo'
        file_id = message.photo[-1].file_id
    elif message.document:
        file_type = 'document'
        file_id = message.document.file_id
    elif message.video:
        file_type = 'video'
        file_id = message.video.file_id
    else:
        return  # неожиданный content_type

    files.append({'type': file_type, 'media': file_id})
    await state.update_data(files=files)

@dp.message_handler(lambda msg: msg.text == "Готово", state=StartJobShiftStates.WAIT_FILES)
async def finalize_start_job_shift(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    if not files:
        return await message.answer("Нужно прикрепить как минимум 1 фото")

    fio = data.get("fio", "")
    username = data.get("username", "")

    # Генерируем подпись и отправляем медиа
    caption = generate_tg_caption("Начало смены", fio, username)
    files[0]['caption'] = caption
    data_media = {
        'chat_id': str(chat_id_change_work),
        'message_thread_id': str(thread_id_change_work),
        'media': json.dumps(files)
    }
    resp = requests.post(urlSendMediaGroup, data=data_media)
    resp_json = resp.json()
    message_id = resp_json["result"][0]["message_id"]
    message_link = f"https://t.me/c/{str(chat_id_change_work)[4:]}/{message_id}"

    # Запись в Google Sheets и получение duration
    tlist = [
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        fio,
        "Начало смены",
        username,
        message_link
    ]

    try:
        duration = with_sheets_retry(
            write_in_answers_ras_shift,
            tlist,
            "Лист1",
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось записать данные в Google Sheets после 3 попыток")
        duration = None

    # Если duration получена, дописываем в подпись
    if duration:
        new_caption = f"{caption}\n⏱ Длительность смены: {duration}"
        await bot.edit_message_caption(
            chat_id=chat_id_change_work,
            message_id=message_id,
            caption=new_caption
        )

    # Ответ пользователю
    if resp.status_code < 400:
        await message.answer('Ваша заявка сформирована', reply_markup=types.ReplyKeyboardRemove())
    else:
        await message.answer('Возникли проблемы с оформлением заявки. Обратитесь к разработчикам',
                             reply_markup=types.ReplyKeyboardRemove())

def generate_tg_caption(action, fio, username):
    ts = datetime.now() + timedelta(hours=3)
    return (
        f"⌚️ {ts.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"👷 @{username}\n\n"
        f"{fio}\n\n"
        f"{action}\n\n"
    )
@dp.message_handler(lambda msg: msg.text == "Выход", state=EndWorkShiftStates.WAIT_FILES)
async def exit_work_shift(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

@dp.message_handler(commands=["end_work_shift"], state="*")
async def cmd_end_work_shift(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[end_work_shift] User @{user.username} ({user.id}) начал оформление заявки на end_work_shift")

    await state.finish()
    await state.update_data(user_id=user.id, username=user.username)
    try:
        data = {'chat_id': str(message.from_user.id)}
        resp = requests.get(URL_GET_FIO, data=data)
        rep = resp.json()
        await state.update_data(fio=_safe_fullname_from_profile(rep, message))
    except Exception as e:
        logging.exception(e)
        await message.answer("Возникла ошибка чтения Ваших данных из базы КК. Убедитесь, что Вы успешно прошли регистрацию и повторите попытку оформления заявки. При повторном возникновении ошибки обратитесь к разработчикам")
        return

    await message.answer("Прикретите фото. И нажмите на кнопку Готово", reply_markup=_keyboard_done_exit())
    await EndWorkShiftStates.WAIT_FILES.set()

@dp.message_handler(
    content_types=[ContentType.PHOTO, ContentType.DOCUMENT, ContentType.VIDEO],
    state=EndWorkShiftStates.WAIT_FILES
)
async def collect_files_end_work_shift(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    # Определяем тип и file_id
    if message.photo:
        file_type = 'photo'
        file_id = message.photo[-1].file_id
    elif message.document:
        file_type = 'document'
        file_id = message.document.file_id
    elif message.video:
        file_type = 'video'
        file_id = message.video.file_id
    else:
        return  # неожиданный content_type

    files.append({'type': file_type, 'media': file_id})
    await state.update_data(files=files)

@dp.message_handler(lambda msg: msg.text == "Готово", state=EndWorkShiftStates.WAIT_FILES)
async def finalize_end_work_shift(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])

    if not files:
        return await message.answer("Нужно прикрепить как минимум 1 фото")

    fio = data.get("fio", "")
    username = data.get("username", "")

    # Генерируем подпись и отправляем медиа
    caption = generate_tg_caption("Окончание смены", fio, username)
    files[0]['caption'] = caption
    data_media = {
        'chat_id': str(chat_id_change_work),
        'message_thread_id': str(thread_id_change_work),
        'media': json.dumps(files)
    }
    resp = requests.post(urlSendMediaGroup, data=data_media)
    resp_json = resp.json()
    message_id = resp_json["result"][0]["message_id"]
    message_link = f"https://t.me/c/{str(chat_id_change_work)[4:]}/{message_id}"

    # Запись в Google Sheets и получение duration
    tlist = [
        (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"),
        fio,
        "Окончание смены",
        username,
        message_link
    ]

    try:
        duration = with_sheets_retry(
            write_in_answers_ras_shift,
            tlist,
            "Лист1",
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось записать данные в Google Sheets после 3 попыток")
        duration = None

    # Если duration получена, дописываем в подпись
    if duration:
        new_caption = f"{caption}⏱ Длительность смены: {duration}"
        await bot.edit_message_caption(
            chat_id=chat_id_change_work,
            message_id=message_id,
            caption=new_caption
        )

    # Ответ пользователю
    if resp.status_code < 400:
        await message.answer('Ваша заявка сформирована', reply_markup=types.ReplyKeyboardRemove())
    else:
        await message.answer('Возникли проблемы с оформлением заявки. Обратитесь к разработчикам', reply_markup=types.ReplyKeyboardRemove())


@dp.message_handler(commands=["nomenclature"], state="*")
async def cmd_nomenclature(message: types.Message, state: FSMContext):
    if message.from_user.id not in list_users:
        return await message.answer("У вас нет прав для вызова данной команды")

    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[nomenclature] User @{user.username} ({user.id}) начал оформление заявки на nomenclature")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)
    await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
    await NomenclatureStates.WAIT_COMPANY.set()

@dp.message_handler(state=NomenclatureStates.WAIT_COMPANY)
async def nomenclature_company(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    company = message.text
    if str(company) in key_company:
        await state.update_data(company=company)
    else:
        await message.answer("Вы ввели компание не из списка. Повторите выбор:",
                         reply_markup=getKeyboardStep1(key_company))
        await NomenclatureStates.WAIT_COMPANY.set()
        return

    await message.answer("Введите радиус:",
                             reply_markup=getKeyboardList(key_radius))
    await NomenclatureStates.WAIT_RADIUS.set()

@dp.message_handler(state=NomenclatureStates.WAIT_RADIUS)
async def nomenclature_radius(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Компания:", reply_markup=getKeyboardList(key_company))
        await NomenclatureStates.WAIT_COMPANY.set()
        return

    radius = message.text
    await state.update_data(radius=radius)

    data = await state.get_data()
    company = data.get("company", "")

    await message.answer("Введите размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer_rez(company))))))
    await NomenclatureStates.WAIT_RAZMER.set()


@dp.message_handler(state=NomenclatureStates.WAIT_RAZMER)
async def nomenclature_razmer(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        await message.answer("Введите радиус:",
                             reply_markup=getKeyboardList(key_radius))
        await NomenclatureStates.WAIT_RADIUS.set()
        return

    razmer = message.text
    await state.update_data(razmer=razmer)

    data = await state.get_data()
    company = data.get("company", "")

    await message.answer("Введите мaрку резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_marka_rez(company))))))
    await NomenclatureStates.WAIT_MARKA_REZ.set()

@dp.message_handler(state=NomenclatureStates.WAIT_MARKA_REZ)
async def nomenclature_marka(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    if message.text == "Назад":
        data = await state.get_data()
        company = data.get("company", "")

        await message.answer("Введите размер:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_razmer_rez(company))))))
        await NomenclatureStates.WAIT_RAZMER.set()
        return

    marka = message.text
    await state.update_data(marka=marka)

    await message.answer("Введите модель резины:",
                         reply_markup=getKeyboardList(key_exit))
    await NomenclatureStates.WAIT_MODEL_REZ.set()

@dp.message_handler(state=NomenclatureStates.WAIT_MODEL_REZ)
async def nomenclature_model(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":

        await message.answer("Введите мaрку резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_marka_rez(company))))))
        await NomenclatureStates.WAIT_MARKA_REZ.set()
        return

    model = message.text
    await state.update_data(model=model)

    await message.answer("Введите сезонность резины:",
                         reply_markup=getKeyboardList(sorted(list(set(get_list_sezon_rez(company))))))
    await NomenclatureStates.WAIT_SEZON.set()

@dp.message_handler(state=NomenclatureStates.WAIT_SEZON)
async def nomenclature_sezon(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Введите модель резины:",
                             reply_markup=getKeyboardList(key_exit))
        await NomenclatureStates.WAIT_MODEL_REZ.set()
        return

    sezon = message.text
    await state.update_data(sezon=sezon)

    if company == "СитиДрайв":
        await message.answer("Введите АЛ:",
                             reply_markup=getKeyboardList(key_exit))
        await NomenclatureStates.WAIT_AL.set()
    else:
        await generating_report_google_sheets_nomen(message, state)
        await message.answer("Добавление новой номенклатуры выполнено", reply_markup=types.ReplyKeyboardRemove())
        await cmd_update_data(message, state)

@dp.message_handler(state=NomenclatureStates.WAIT_AL)
async def nomenclature_al(message: types.Message, state: FSMContext):
    if _check_exit(message):
        await state.finish()
        return

    data = await state.get_data()
    company = data.get("company", "")

    if message.text == "Назад":
        await message.answer("Введите сезонность резины:",
                             reply_markup=getKeyboardList(sorted(list(set(get_list_sezon_rez(company))))))
        await NomenclatureStates.WAIT_SEZON.set()
        return

    al = message.text
    await state.update_data(al=al)

    await generating_report_google_sheets_nomen(message, state)
    await message.answer("Добавление новой номенклатуры выполнено", reply_markup=types.ReplyKeyboardRemove())
    await cmd_update_data(message, state)

async def generating_report_google_sheets_nomen(message: types.Message, state: FSMContext):
    tlist = list()
    data = await state.get_data()
    if data.get("company", "") == "СитиДрайв":
        company = "Сити"
        sheet = "Резина Сити"
    elif data.get("company", "") == "Яндекс":
        company = "Яндекс"
        sheet = "Резина ЯД"
    else:
        company = "Белка"
        sheet = "Резина Белка"
    tlist.append("")
    tlist.append(data.get("radius", ""))
    tlist.append(data.get("razmer", ""))
    tlist.append(data.get("sezon", ""))
    tlist.append(data.get("marka", ""))
    tlist.append(data.get("model", ""))
    tlist.append(company)
    tlist.append(data.get("al", ""))
    try:
        with_sheets_retry(
            write_in_answers_ras_nomen,
            tlist,
            sheet,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось записать данные в базу знаний после 3 попыток")

@dp.message_handler(commands=["open_gate"], state="*")
async def cmd_open_gate(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    user = message.from_user
    logger.info(f"[open_gate] User @{user.username} ({user.id}) начал оформление открытие ворот")

    await state.finish()
    await state.update_data(user_id=user.id,username=user.username)

    try:
        # 1) Получаем ФИО
        resp = requests.get(
            URL_GET_FIO,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()
        fio = (rep.get("user") or {}).get("fullname") or "—"
        await state.update_data(fio=fio)

    except Exception as e:
        logger.exception("Ошибка при запросе задач для open_gate: %s", e)
        await message.answer("Не удалось получить информацию по задачам. Попробуйте позже")
        return

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("Подтвердить открытие"))
    kb.add(KeyboardButton("Выход"))

    text = (
        f"ФИО: {fio}\n"
        "Подтвердите открытие ворот склада"
    )

    sent = await message.answer(text, reply_markup=kb)
    await state.update_data(prompt_msg_id=sent.message_id)
    await OpenGateStates.WAIT_CONFIRM.set()

@dp.message_handler(state=OpenGateStates.WAIT_CONFIRM)
async def open_gate_confirm(message: types.Message, state: FSMContext):
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")

    if prompt_msg_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
        except Exception as e:
            logger.exception("Не удалось удалить сообщение с подтверждением: %s", e)


    text = message.text.strip()

    if text.lower() == "выход":
        await message.answer("Операция отменена", reply_markup=ReplyKeyboardRemove())
        await state.finish()
        return

    if text.lower() == "подтвердить открытие":
        data = await state.get_data()

        fio = data.get("fio", "—")
        plate = data.get("car_plate", "")
        company = data.get("company", "")

        _teg, _fi = find_logistics_rows()
        if not _fi:
            logist = ""
        else:
            logist = " , ".join(f"{name} ({teg})" for name, teg in zip(_fi, _teg))

        now_msk = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
        send_text = (
            f"#Открытие_Склада\n\n"
            f"{now_msk}\n"
            f"ФИО: {fio}\n"
            f"Откройте, пожалуйста, ворота\n{logist}"
        )

        CHAT_ID = chat_id_sborka_Sity
        THREAD_ID = thread_id_gates_Sity
        try:
            sent = await bot.send_message(
                chat_id=CHAT_ID,
                text=send_text,
                message_thread_id=THREAD_ID
            )

        except Exception as e:
            logger.exception("Ошибка отправки в складской чат: %s", e)
            await message.answer("Ошибка отправки в складской чат", reply_markup=ReplyKeyboardRemove())
            await state.finish()
            return

        message_link = f"https://t.me/c/{str(CHAT_ID)[4:]}/{THREAD_ID}/{sent.message_id}"

        try:
            write_open_gate_row(
                fio=fio,
                car_plate=plate,
                company=company,
                message_link=message_link)
        except Exception as e:
            logger.exception("Ошибка отправки в write_open_gate_row: %s", e)

        await message.answer(f"Сообщение отправлено логисту {logist}", reply_markup=ReplyKeyboardRemove())
        await state.finish()
        return

    await message.answer("Выберите действие: Подтвердить открытие / Выход")

# -----------------------------------------------------------------------------------------------------
# Keyboar
# -----------------------------------------------------------------------------------------------------
def getKeyboardList(record_list):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for record in record_list:
        keyboard.row(types.KeyboardButton(text=str(record)))
    keyboard.row(types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Выход"))
    return keyboard


def getKeyboardStep1(record_list):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for record in record_list:
        keyboard.row(types.KeyboardButton(text=str(record)))
    keyboard.row(types.KeyboardButton(text="Выход"))
    return keyboard


# -----------------------------------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------------------------------
def get_list_radius(company):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_radius_bz_znan(lst)



def get_list_radius_bz_znan(bz_znan):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        tlist.append(int(rez[GHRezina.RADIUS]))
    return tlist


def get_list_razmer(company,radius):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_razmer_bz_znan(lst,radius)


def get_list_razmer_bz_znan(bz_znan,radius):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip():
            tlist.append(str(rez[GHRezina.RAZMER]).strip())
    return tlist


def get_list_marka(company, radius, razmer):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_marka_bz_znan(lst, radius, razmer)


def get_list_marka_bz_znan(bz_znan, radius, razmer):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip() and \
                str(razmer) == str(rez[GHRezina.RAZMER]).strip():
            tlist.append(str(rez[GHRezina.MARKA]).strip())
    return tlist


def get_list_model(company, radius, razmer, marka_rez):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_model_bz_znan(lst, radius, razmer, marka_rez)



def get_list_model_bz_znan(bz_znan, radius, razmer, marka_rez):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                rez[GHRezina.RAZMER]).strip() and str(marka_rez) == str(rez[GHRezina.MARKA]).strip():
            tlist.append(str(rez[GHRezina.MODEL]).strip())
    return tlist


def get_list_sezon(company, radius, razmer, marka_rez, model_rez, short = 0):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_sezon_bz_znan(lst, radius, razmer, marka_rez, model_rez, short)



def get_list_sezon_bz_znan(bz_znan, radius, razmer, marka_rez, model_rez, short):
    tlist = list()
    if not short:
        for i, rez in enumerate(bz_znan):
            if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                    rez[GHRezina.RAZMER]).strip() and str(marka_rez) == str(rez[GHRezina.MARKA]).strip() and str(model_rez) == str(rez[GHRezina.MODEL]).strip():
                tlist.append(str(rez[GHRezina.SEZON]).strip())
    else:
        for i, rez in enumerate(bz_znan):
            if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                    rez[GHRezina.RAZMER]).strip():
                tlist.append(str(rez[GHRezina.SEZON]).strip())
    return tlist


def get_list_marka_ts(company):
    if company == "СитиДрайв":
        lst = list_per_st
        count = 2
    elif company == "Яндекс":
        lst = list_per_ya
        count = 3
    else:
        lst = list_per_blk
        count = 1
    return get_list_marka_ts_bz_znan(lst, count)


def get_list_marka_ts_bz_znan(bz_znan, nomer):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        tlist.append(str(rez[nomer]))
    return tlist


def loading_rezina_is_Google_Sheets():
    global list_rez_st
    list_rez_st = loading_bz_znaniya("Резина Сити")
    global list_rez_ya
    list_rez_ya = loading_bz_znaniya("Резина ЯД")
    global list_rez_blk
    list_rez_blk = loading_bz_znaniya("Резина Белка")


def loading_model_is_Google_Sheets():
    global list_per_st
    list_per_st = loading_bz_znaniya("Перечень ТС Сити")
    global list_per_ya
    list_per_ya = loading_bz_znaniya("Перечень ТС Яд")
    global list_per_blk
    list_per_blk = loading_bz_znaniya("Перечень ТС Белка")

def get_list_razmer_rez(company):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    tlist = list()
    for i, rez in enumerate(lst):
        tlist.append(str(rez[2]))
    return tlist

def get_list_marka_rez(company):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    tlist = list()
    for i, rez in enumerate(lst):
        tlist.append(str(rez[4]))
    return tlist

def get_list_sezon_rez(company):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    tlist = list()
    for i, rez in enumerate(lst):
        tlist.append(str(rez[3]))
    return tlist

def getGRZTs(company, input_grz):
    grz = list()
    if company == "СитиДрайв":
        lst = list_per_st
        ind = 0
    elif company == "Яндекс":
        lst = list_per_ya
        ind = 0
    else:
        lst = list_per_blk
        ind = 2
    grz_ts = list()
    for i,string in enumerate(lst):
        grz_ts.append(string[ind])
    for i,string in enumerate(grz_ts):
        if string.startswith(input_grz):
            grz.append(string)
    return grz

def get_park_TS_YNDX(grz: str):
    grz = str(grz).strip().lower()
    for row in list_per_ya:
        if str(row[0]).strip().lower() == grz:
            return row[2]

    return None

def check_validation_radius(company, radius):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_radius(lst, radius)


def check_radius(bz_znan, radius):
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip():
            return 1
    return 0


def check_validation_razmer(company, radius, razmer):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_razmer(lst, radius, razmer)


def check_razmer(bz_znan, radius, razmer):
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                rez[GHRezina.RAZMER]).strip():
            return 1
    return 0


def check_validation_marka(company, radius, razmer, marka_rez):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_marka(lst, radius, razmer, marka_rez)


def check_marka(bz_znan, radius, razmer, marka_rez):
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                rez[GHRezina.RAZMER]).strip() and str(marka_rez) == str(rez[GHRezina.MARKA]).strip():
            return 1
    return 0


def check_validation_model(company,  radius, razmer, marka_rez, model_rez):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_model(lst, radius, razmer, marka_rez, model_rez)


def check_model(bz_znan, radius, razmer, marka_rez, model_rez):
    for i, rez in enumerate(bz_znan):
        if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                rez[GHRezina.RAZMER]).strip() and str(marka_rez) == str(rez[GHRezina.MARKA]).strip() and str(model_rez) == str(rez[GHRezina.MODEL]).strip():
            return 1
    return 0


def check_validation_sezon(company, radius, razmer, marka_rez, model_rez, sezon, short = 0):
    if company == "СитиДрайв":
        lst = list_rez_st
    elif company == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_sezon(lst, radius, razmer, marka_rez, model_rez, sezon, short)


def check_sezon(bz_znan, radius, razmer, marka_rez, model_rez, sezon, short):
    if not short:
        for i, rez in enumerate(bz_znan):
            if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(
                    rez[GHRezina.RAZMER]).strip() and str(marka_rez) == str(rez[GHRezina.MARKA]).strip() and str(
                    model_rez) == str(rez[GHRezina.MODEL]).strip() and str(sezon) == str(
                    rez[GHRezina.SEZON]).strip():
                return 1
    else:
        for i, rez in enumerate(bz_znan):
            if str(radius) == str(rez[GHRezina.RADIUS]).strip() and str(razmer) == str(rez[GHRezina.RAZMER]).strip() and str(sezon) == str(rez[GHRezina.SEZON]).strip():
                return 1
    return 0


def check_validation_marka_ts(company, marka_ts):
    return check_marka_ts(company, marka_ts)


def check_marka_ts(company, marka_ts):
    tlist = sorted(list(set(get_list_marka_ts(company))))
    for i, rez in enumerate(tlist):
        if str(marka_ts).strip() == str(rez).strip():
            return 1
    return 0

async def debug_fsm_context(message: types.Message, state: FSMContext):
    """
    Выводит текущее state и все пары ключ:значение из FSMContext
    """
    # получаем имя текущего состояния
    current_state = await state.get_state()
    # получаем все данные, которые накопились в FSMContext
    data = await state.get_data()

    # формируем текст отчёта
    report_lines = [f"📒 Текущее состояние: {current_state or 'None'}", "🔑 Данные FSMContext:"]
    if data:
        for key, value in data.items():
            report_lines.append(f" • {key}: {value!r}")
    else:
        report_lines.append(" (пусто)")

    report = "\n".join(report_lines)

    # вывод в консоль
    print(report)

# -----------------------------------------------------------------------------------------------------
def _check_exit(message: types.Message) -> bool:
    """
    Проверяем, не нажал ли пользователь кнопку "Выход".
    Если да — пишем сообщение и возвращаем True, чтобы прервать обработку.
    """
    if message.text and message.text.lower() == "выход":
        # Удаляем клавиатуру
        kb_remove = types.ReplyKeyboardRemove()
        asyncio.create_task(
            message.answer("Оформление завершено", reply_markup=kb_remove)
        )
        return True
    return False
# -----------------------------------------------------------------------------------------------------
async def main():
    # Запуск логирования
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    log_name = f'{LOGS_DIR}/{datetime.now().strftime("%Y-%m-%d")}.log'
    file_handler = logging.FileHandler(log_name, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    # Начальная загрузка базы данных
    try:
        with_sheets_retry(
            loading_rezina_is_Google_Sheets,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось загурзить из Гугл Таблицы данные после 3 попыток")

    try:
        with_sheets_retry(
            loading_model_is_Google_Sheets,
            max_attempts=3,
            base_delay=2
        )
    except RuntimeError:
        logger.error("Не удалось загурзить из Гугл Таблицы данные после 3 попыток")

    loop = asyncio.get_event_loop()
    loop.create_task(periodic_print_zayavka())

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling()

if __name__ == '__main__':
    asyncio.run(main())