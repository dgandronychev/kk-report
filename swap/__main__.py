import telebot
import requests
import re
import logging
import threading
import time
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gspread
from gspread import Client, Spreadsheet
from enum import IntEnum
import sqlite3
from telebot.apihelper import ApiTelegramException
from gspread.exceptions import APIError

DB_PATH = "/clean-car-tire-repair-technician-bot/data/xab_messages.db"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Рабочий вариант
chat_id_Yandex = -1001739909868
thread_id_Yandex = 45467
thread_id_Yandex_zapr = 59180
thread_id_Yandex_gates = 166590
link_Yandex = 1739909868
thread_id_Yandex_ras = 45470
thread_id_Yandex_move = 59034
thread_id_Yandex_hab = 59036
chat_id_Sity = -1002188632791
thread_id_Sity =  9
thread_id_Sity_zapr = 13428
thread_id_Sity_gates = 250602
link_Sity = 2188632791
thread_id_Sity_ras = 12
thread_id_Sity_move = 13129
thread_id_Sity_hab = 13142
chat_id_Belka = -1003111588590
thread_id_Belka = 6
thread_id_Belka_zapr = 5
link_Belka = 3111588590
thread_id_Belka_ras = 9
thread_id_Belka_move = 4
thread_id_Belka_hab = 8
chat_id_common = -1002005014821
urlAddUser = "https://app.clean-car.net/api/v1/bots/telegram_information/update/"
urlGetUserInfo = "https://app.clean-car.net/api/v1/bots/accident/retrieve/"
urlSmallDtp = "https://app.clean-car.net/api/v1/bots/accident/retrieve/"
URL_GET_INFO_TASK = "https://app.clean-car.net/api/v1/bots/open_tasks/list/"

# Тестовый вариант
# chat_id_Yandex = -1002371648868
# thread_id_Yandex = 2555
# thread_id_Yandex_zapr = 2555
# thread_id_Yandex_gates = 2555
# link_Yandex = 2371648868
# thread_id_Yandex_ras = 2555
# thread_id_Yandex_move = 2555
# thread_id_Yandex_hab = 2555
# chat_id_Sity = -1002371648868
# thread_id_Sity = 2555
# thread_id_Sity_zapr = 2555
# thread_id_Sity_gates = 2555
# link_Sity = 2371648868
# thread_id_Sity_ras = 2555
# thread_id_Sity_move = 2555
# thread_id_Sity_hab = 2555
# chat_id_Belka = -1002371648868
# thread_id_Belka = 2555
# thread_id_Belka_zapr = 2555
# link_Belka = 2371648868
# thread_id_Belka_ras = 2555
# thread_id_Belka_move = 2555
# thread_id_Belka_hab = 2555
# chat_id_common = -1002371648868
# urlAddUser = "https://stage.app.clean-car.net/api/v1/bots/telegram_information/update/"
# urlGetUserInfo = "https://stage.app.clean-car.net/api/v1/bots/accident/retrieve/"
# urlSmallDtp = "https://stage.app.clean-car.net/api/v1/bots/accident/retrieve/"
# URL_GET_INFO_TASK = "https://stage.app.clean-car.net/api/v1/bots/open_tasks/list/"

bot = telebot.TeleBot(f"{token_bot}")
gspread_url_baza_zn = "https://docs.google.com/spreadsheets/d/1Rk_9eyjx0u5dUGnz84-6GCshd1zLLWOR-QGPZtQTMKg/edit?gid=1510247160#gid=1510247160"
gspread_url_rasxod_shm = "https://docs.google.com/spreadsheets/d/14vrAidePmR78-l9R31tWOTxCADmqvseLIHhR7fY1kR4/edit?gid=729033122#gid=729033122"
gspread_url_peremeshenie = "https://docs.google.com/spreadsheets/d/1p044-xtk5TxFOsPZ9l_kbthc53toqdh3kRXrGbU5Iao/edit?gid=766771694#gid=766771694"
#gspread_url_peremeshenie = "https://docs.google.com/spreadsheets/d/1DQ87q8-qqgjlz8c0ZsqYpNKrYqqNrwil2ApKE23JLLs/edit?gid=0#gid=0"
gspread_url_rasxod = "https://docs.google.com/spreadsheets/d/1iH5IeurStoNQB9FKwdxehTF5hB_1QcX9LFYjpVqsxvo/edit?pli=1&gid=0#gid=0"
gspread_url_gates = "https://docs.google.com/spreadsheets/d/1BH7HDYBS6E-nSoq3ZBQljhA74aAEe8QIOviwOPpAyX4/edit?gid=0#gid=0"
URL_GOOGLE_SHEETS_CHART = "https://docs.google.com/spreadsheets/d/15Kw7bweFKg3Dp0INeA47eki1cuPIgtVgk_o_Ul3LGyM/edit?gid=1647640846#gid=1647640846"

urlSendMediaGroup = f"https://api.telegram.org/bot{token_bot}/sendMediaGroup"

mvrecords = list() # Перемещение
pkrecords = list() # Парковка
zprecords = list() # Заправка
rsrecords = list() # Расход

grz_ts_ya = list()
grz_ts_st = list()
grz_ts_blk = list()
marka_ts_ya = list()
marka_ts_st = list()
marka_ts_blk = list()
list_rez_ya = list()
list_rez_st = list()
list_rez_blk = list()
grz_tech = list()
list_users = [796932736, 1050518459, 547087397, 548446822]
# Глобальные переменные для кэша
global_xab_cache = None
global_cache_usage = 0
# Множество id пользователей, которые уже «захватили» кэш
users_with_cache = {}
last_message_ids = {
    "Яндекс": 0,
    "СитиДрайв": 0,
    "Белка": 0
}

XAB_PER_PAGE = 25
xab_pages = {}

# Структура списка для перемещения
class Mv(IntEnum):
    USER_ID = 0
    USERNAME = 1
    FIO = 2
    GRZ_TECH = 3
    TYPE_ACTION = 4
    GRZ_PEREDACHA = 5
    COMPANY = 6


# Структура списка для парковки
class Pk(IntEnum):
    USER_ID = 0
    USERNAME = 1
    FIO = 2
    GRZ_TECH = 3
    COMPANY = 4
    GRZ_ZADACHA = 5
    TIP_DOCUMENT = 6
    # Фото
    # Ссылка на сообщение

# Структура списка для заправки
class Zp(IntEnum):
    USER_ID = 0
    USERNAME = 1
    FIO = 2
    GRZ_TECH = 3
    COMPANY = 4
    PROBEG = 5
    SUMMA = 6
    TIP_DOCUMENT = 7
    # Фото
    # Ссылка на сообщение

# Структура списка для расхода
class Rs(IntEnum):
    USER_ID = 0
    USERNAME = 1
    FIO = 2
    GRZ_TECH = 3
    COMPANY = 4
    GOROD = 5
    GRZ_ZADACHA = 6
    SUMMA = 7
    OPLATA = 8
    DOP_OPLATA = 9
    PRICIHA = 10
    TIP_DOCUMENT = 11
    # Фото

class Rc(IntEnum):
    MARKA_TS = 0
    RADIUS = 1
    RAZMER = 2
    MARKA_REZ = 3
    MODEL_REZ = 4
    SEZON = 5
    TIP_DISKA = 6
    COUNT_LEFT = 7
    COUNT_RIGHT = 8

class Og(IntEnum):
    USER_ID = 0
    USERNAME = 1
    FIO = 2
    CAR_PLATE = 3
    COMPANY = 4

open_gate_records = []

key_type = ["Комплект", "Ось", "Правое колесо", "Левое колесо"]
key_company = ["СитиДрайв", "Яндекс", "Белка"]
key_exit = []
key_sity = ["Москва", "Санкт-Петербург"]
key_oplata = ["Бизнес-карта", "Наличные <> Перевод <> Личная карта"]
key_oplata_dop = ["Подача на возмещение(свои деньги) + 6%"]
key_action = ["Забираете со склада", "Сдаете бой", "Передаете в техничку"]
key_type_disk = ["Литой оригинальный", "Литой неоригинальный", "Штамп"]
key_sogl = ["Да", "Нет"]
key_corek = ["Заполнено корректно", "Заполнено не корректно"]
key_action_record = ["Добавить запись", "Удалить запись", "Завершить"]
key_chisla = ["0","1","2","3","4","5"]

# Регулярное выражение для проверки государственного номера автомобиля
REGEX_AUTO_NUMBER = r'^[а-я]{1}\d{3}[а-я]{2}\d{2,3}$'

def _normalize_plate(s: str) -> str:
    """Убираем пробелы и приводим к нижнему регистру (для проверки формата)."""
    return re.sub(r'\s+', '', str(s or '')).lower()

def _is_plate_format(s: str) -> bool:
    """Проверка строки на формат ГРЗ (без учёта наличия в списке)."""
    return re.match(REGEX_AUTO_NUMBER, _normalize_plate(s)) is not None

def _safe_delete_message(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
    except ApiTelegramException as e:
        # Частые кейсы: "message can't be deleted", "message to delete not found"
        msg = str(e)
        if ("message can't be deleted" in msg) or ("message to delete not found" in msg):
            # Просто пропускаем, не мусорим лог
            return
        # Остальное можно записать как warning, без трейсбэка
        logger.warning("Can't delete message %s in %s: %s", msg_id, chat_id, e)
    except Exception as e:
        logger.warning("Unexpected error deleting message %s in %s: %s", msg_id, chat_id, e)


# -------------------------------------------------------------------------------------------------------
# Ловит сообщение с фото/документ и переправляет его в соотвествующий обработчик
@bot.message_handler(content_types=['photo', 'document'])
def FilesReception(message):
    global pkrecords
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            if len(record) == Pk.TIP_DOCUMENT:
                record.append(message.content_type)
            if str(record[Pk.TIP_DOCUMENT]) == 'photo':
                record.append(message.photo[-1].file_id)
            elif str(record[Pk.TIP_DOCUMENT]) == 'document':
                record.append(message.document.file_id)
            if len(record) == Pk.TIP_DOCUMENT + 2:
                generating_report_parking(message)
            return
    global zprecords
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == message.from_user.id:
            if len(record) == Zp.TIP_DOCUMENT:
                record.append(message.content_type)
            if len(record) <= Zp.TIP_DOCUMENT + 4:
                if str(record[len(record) - 1]) == str(1):
                    if str(record[Zp.TIP_DOCUMENT]) == 'photo':
                        record[len(record) - 1] = message.photo[-1].file_id
                    elif str(record[Zp.TIP_DOCUMENT]) == 'document':
                        record[len(record) - 1] = message.document.file_id
                    generating_report_zapravka(message)
                    return
            if str(record[Zp.TIP_DOCUMENT]) == 'photo':
                record.append(message.photo[-1].file_id)
            elif str(record[Zp.TIP_DOCUMENT]) == 'document':
                record.append(message.document.file_id)
            if len(record) == Zp.TIP_DOCUMENT + 2:
                generating_report_zapravka(message)
            return
    global rsrecords
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if len(record) == Rs.TIP_DOCUMENT:
                record.append(message.content_type)
            if str(record[Rs.TIP_DOCUMENT]) == 'photo':
                record.append(message.photo[-1].file_id)
            elif str(record[Rs.TIP_DOCUMENT]) == 'document':
                record.append(message.document.file_id)
            if len(record) == Rs.TIP_DOCUMENT + 2:
                generating_report_expense(message)
            return
    global mvrecords
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            number_last_list = get_number_last_list(record)
            if len(record) == 7 + number_last_list:
                record.append(message.content_type)
            if len(record) == 10 + number_last_list:
                if str(record[len(record) - 1]) == str(1):
                    if str(record[7 + number_last_list]) == 'photo':
                        record[len(record) - 1] = message.photo[-1].file_id
                    elif str(record[7 + number_last_list]) == 'document':
                        record[len(record) - 1] = message.document.file_id
                    generating_report_move(message)
                    return
            if str(record[7 + number_last_list]) == 'photo':
                record.append(message.photo[-1].file_id)
            elif str(record[7 + number_last_list]) == 'document':
                record.append(message.document.file_id)
            if len(record) == number_last_list + 9:
                generating_report_move(message)
            return

# -------------------------------------------------------------------------------------------------------
@bot.message_handler(commands=['start'])
def registration(message):
    if int(message.chat.id) > 0:
        keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        keyboard.add(telebot.types.KeyboardButton(text="Поделиться контактом", request_contact=True))
        bot.send_message(message.chat.id, "Убедитесь, что в приложение КЛИНКАР указан номер телефона, который "
                                          "привязан к аккаунту Телеграм в формате 7********** и нажмите на кнопку Поделиться контактом",
                         reply_markup=keyboard)
    else:
        bot.reply_to(message, "Перейдите в личные сообщение с ботом для оформления заявки")

@bot.message_handler(content_types=['contact'])
def read_contact_phone(message):
    phone = message.contact.phone_number
    user_id = message.from_user.id
    username = message.from_user.username
    keyboard = telebot.types.ReplyKeyboardRemove()
    json_data = {"phone": str(re.sub("[^0-9]", "", phone)), "tg_username": str(username), "tg_chat_id": str(user_id)}
    try:
        resp = requests.post(urlAddUser, json=json_data)
        if resp.status_code < 399:
            bot.send_message(message.chat.id, "Вы прошли регистрацию", reply_markup=keyboard)
        else:
            temp_dict = resp.json()
            bot.send_message(message.chat.id, temp_dict["result"], reply_markup=keyboard)
    except Exception as e:
        logging.exception(e)
        bot.send_message(message.chat.id, "Произошла ошибка. Обратитесь к разработчикам")

@bot.message_handler(commands=['update_data'])
def update_data(message):
    count = 0
    for i, record in enumerate(list_users):
        if record == message.from_user.id:
            bot.send_message(message.chat.id, "Обновление списков началось")
            loading_grz_is_Google_Sheets()
            count = count + 1
    if count == 0:
        bot.send_message(message.chat.id, "У вас нет прав для вызова данной команды")

@bot.message_handler(commands=['print_move'])
def print_move(message):
    global users_with_cache

    for idx, user_id in enumerate(users_with_cache):
        user_tag = f"<a href='tg://user?id={user_id}'>Пользователь {idx}</a>"
        bot.send_message(message.chat.id, user_tag, parse_mode="HTML")


@bot.message_handler(commands=['move'])
def move(message):
    global mvrecords, users_with_cache
    global mvrecords
    for i, record in enumerate(mvrecords):
        if record[0] == message.from_user.id:
            mvrecords.remove(record)
            if message.from_user.id in users_with_cache:
                release_cache(message.from_user.id)
    if int(message.chat.id) > 0:
        tlist = list()
        try:
            data = {'chat_id': str(message.from_user.id)}
            resp = requests.get(urlSmallDtp, data=data)
            rep = resp.json()
            tlist.append(message.from_user.id)
            tlist.append(message.from_user.username)
            tlist.append(rep['user']['fullname'])
            mvrecords.append(tlist)
            acquire_cache(user_id=message.from_user.id)
            step1_move(message)
        except Exception as e:
            logging.exception(e)
            bot.send_message(message.chat.id, "Возникла ошибка чтения Ваших данных из базы КК. Убедитесь, что Вы успешно прошли регистрацию и повторите попытку оформления заявки. Регистрация производится по команде /start \nПри повторном возникновении ошибки обратитесь к разработчикам")
    else:
        bot.reply_to(message, "Перейдите в личные сообщение с ботом для оформления заявки",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def step1_move(message):
    global mvrecords
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            bot.send_message(message.chat.id, "ГРЗ технички (можно ввести вручную):", reply_markup=getKeyboardStep1(grz_tech))
            bot.register_next_step_handler(message, step2_move)

def step2_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if not nazad:
                plate_raw = (message.text or "").strip()
                record.append(plate_raw.upper())
                is_known = bool(check_validation_grz_tech(record, Mv.GRZ_TECH))
                is_formatted = _is_plate_format(record[Mv.GRZ_TECH])
                if not (is_known or is_formatted):
                    postpone_build(message, record, 2)
                    return
            bot.send_message(message.chat.id, "Действие:", reply_markup=getKeyboardList(key_action))
            bot.register_next_step_handler(message, step3_move)

def step3_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                mvrecords.remove(record)
                move(message)
                return
            if not nazad:
                record.append(message.text)
            if str(record[Mv.TYPE_ACTION]) == "Передаете в техничку":
                bot.send_message(message.chat.id, "Кому передаете:", reply_markup=getKeyboardList(grz_tech))
                bot.register_next_step_handler(message, step4_move)
            else:
                bot.send_message(message.chat.id, "Компания:", reply_markup=getKeyboardList(key_company))
                bot.register_next_step_handler(message, step5_move)

def step4_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step2_move(message, 1)
                return
            if not nazad:
                plate_raw = (message.text or "").strip()
                record.append(plate_raw.upper())
                is_known = bool(check_validation_grz_tech(record, Mv.GRZ_PEREDACHA))
                is_formatted = _is_plate_format(record[Mv.GRZ_PEREDACHA])
                if not (is_known or is_formatted):
                    postpone_build(message, record, 2)
                    return
            bot.send_message(message.chat.id, "Компания:", reply_markup=getKeyboardList(key_company))
            bot.register_next_step_handler(message, step5_move)

def step5_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0 and (str(record[Mv.TYPE_ACTION]) == "Забираете со склада" or str(record[Mv.TYPE_ACTION]) == "Сдаете бой"):
                record.pop(len(record) - 1)
                step2_move(message, 1)
                return
            if message.text == "Назад" and nazad == 0 and str(record[Mv.TYPE_ACTION]) == "Передаете в техничку":
                record.pop(len(record) - 1)
                step3_move(message, 1)
                return
            if not nazad:
                if str(record[Mv.TYPE_ACTION]) == "Забираете со склада" or str(record[Mv.TYPE_ACTION]) == "Сдаете бой":
                    record.append("")
                record.append(message.text)
            if str(record[Mv.TYPE_ACTION]) == "Забираете со склада":
                bot.send_message(message.chat.id, "Выберите вариант из предложенных:",
                                 reply_markup=getKeyboardList(key_type))
                bot.register_next_step_handler(message, step_xab_move)
            else:
                bot.send_message(message.chat.id, "Марка автомобиля:",
                                 reply_markup=getKeyboardStep1(get_list_marka_ts(record, Mv)))
                bot.register_next_step_handler(message, step7_move)

def step6_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0 and (str(record[Mv.TYPE_ACTION]) == "Забираете со склада" or str(record[Mv.TYPE_ACTION]) == "Сдаете бой"):
                record.pop(len(record) - 1)
                record.pop(len(record) - 1)
                step3_move(message, 1)
                return
            if message.text == "Назад" and nazad == 0 and str(record[Mv.TYPE_ACTION]) == "Передаете в техничку":
                record.pop(len(record) - 1)
                step4_move(message, 1)
                return
            if not nazad:
                record.append(message.text)
            bot.send_message(message.chat.id, "Марка автомобиля:",
                             reply_markup=getKeyboardStep1(get_list_marka_ts(record, Mv)))
            bot.register_next_step_handler(message, step7_move)

def step_xab_move(message, nazad=0):
    global mvrecords, xab_pages
    if check_exit(message, 3):
        return

    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        # Назад
        if message.text == "Назад" and nazad == 0:
            record.pop(len(record) - 1)
            record.pop(len(record) - 1)
            xab_pages.pop(message.from_user.id, None)
            step4_move(message, 1)
            return

        # Пролистывание — «Ещё»
        if message.text == "Ещё" and message.from_user.id in xab_pages:
            xab_pages[message.from_user.id]["page"] += 1
            show_xab_page(message, message.from_user.id, start_over=False)
            bot.register_next_step_handler(message, step_xab_move)
            return

        # Пользователь выбрал КОНКРЕТНУЮ позицию (не тип и не «Ещё»)
        if message.text not in key_type:
            st = xab_pages.get(message.from_user.id)
            chosen_type = st["type"] if st else message.text.split(" ", 1)[0]
            xab_pages.pop(message.from_user.id, None)
            # ВАЖНО: обрабатываем выбор СРАЗУ, а не «на следующее сообщение»
            step_xab_move1(message, chosen_type)
            return

        # Пользователь выбрал тип (Комплект/Ось/Правое/Левое)
        type_selected = message.text.split(" ", 1)[0]
        options = get_xab_koles(record[Mv.COMPANY], type_selected, message.from_user.id)
        if not options:
            bot.send_message(
                message.chat.id,
                "В хабе нет выбранного варианта, выберете другой вариант:",
                reply_markup=getKeyboardList(key_type)
            )
            bot.register_next_step_handler(message, step_xab_move)
            return

        xab_pages[message.from_user.id] = {"type": type_selected, "options": options, "page": 0}
        show_xab_page(message, message.from_user.id, start_over=True)
        bot.register_next_step_handler(message, step_xab_move)
        return


def step_xab_move1(message, type, nazad=0):
    """
    Функция обрабатывает выбор пользователя.
    После получения выбора из списка (строка, состоящая из значений, объединённых через "|")
    определяется ключ группы (в исходном порядке):
      (Марка ТС, Радиус, Размер, Марка резины, Модель резины, Сезонность, Тип диска)
    Затем из кэша удаляются строки согласно выбранному типу.
    После чего данные преобразуются и добавляются в запись пользователя.
    """
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                step5_move(message, 1)
                return

            temp = split_entry(message.text)  # [Марка ТС, Радиус, Размер, Марка резины, Модель резины, Сезонность, Тип диска]

            key = (
                temp[0].strip(),
                temp[1].strip(),
                temp[2].strip(),
                temp[3].strip(),
                temp[4].strip(),
                temp[5].strip(),
                temp[6].strip()
            )

            # Определяем, сколько строк удалить в зависимости от выбранного типа
            removal = {}
            if type in ["Левое", "Правое"]:
                removal[type] = 1
            elif type == "Ось":
                removal = {"Левое": 1, "Правое": 1}
            elif type == "Комплект":
                removal = {"Левое": 2, "Правое": 2}


            # Унифицированная структура: [Марка ТС, Радиус, Размер, Марка резины, Модель резины, Сезон, Тип диска, COUNT_LEFT, COUNT_RIGHT]
            if type == "Левое":
                count_left, count_right = 1, 0
            elif type == "Правое":
                count_left, count_right = 0, 1
            elif type == "Ось":
                count_left, count_right = 1, 1
            elif type == "Комплект":
                count_left, count_right = 2, 2
            else:
                count_left, count_right = 0, 0

            tlist = [
                temp[0], temp[1], temp[2],
                temp[3], temp[4], temp[5],
                temp[6], count_left, count_right
            ]
            record.append(tlist)

            bot.send_message(
                message.chat.id,
                "Требуются еще колеса:",
                reply_markup=getKeyboardStep1(key_sogl)
            )
            bot.register_next_step_handler(message, step15_move)


def step7_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        # Возврат из финального превью к добавлению ещё позиций
        if nazad == 2 and message.text == "Назад":
            bot.send_message(
                message.chat.id,
                get_report_move_str(record, Mv.COMPANY + 1, Mv.COMPANY + get_number_last_list(record) + 1),
                reply_markup=getKeyboardList(key_corek)
            )
            bot.register_next_step_handler(message, step16_move)
            return

        if nazad != 1:
            tlist = [message.text]  # Марка ТС
            record.append(tlist)
            if not check_validation_marka_ts(record, Mv, record[-1]):
                postpone_build(message, record, 2)
                return

        bot.send_message(
            message.chat.id,
            "Радиус:",
            reply_markup=getKeyboardList(sorted(list(set(get_list_radius(record, Mv)))))
        )
        bot.register_next_step_handler(message, step8_move)

def step8_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record.pop()  # убираем tlist
            step6_move(message, 1)
            return

        if not nazad:
            record[-1].append(message.text)  # Радиус
            if not check_validation_radius(record, Mv, record[-1]):
                postpone_build(message, record, 2)
                return

        bot.send_message(
            message.chat.id,
            "Размер:",
            reply_markup=getKeyboardList(sorted(list(set(get_list_razmer(record, Mv, record[-1])))))
        )
        bot.register_next_step_handler(message, step9_move)

def step9_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record[-1].pop()  # убрать Радиус
            step7_move(message, 1)
            return

        if not nazad:
            record[-1].append(message.text)  # Размер
            if not check_validation_razmer(record, Mv, record[-1]):
                postpone_build(message, record, 2)
                return

        bot.send_message(
            message.chat.id,
            "Марка:",
            reply_markup=getKeyboardList(sorted(list(set(get_list_marka(record, Mv, record[-1])))))
        )
        bot.register_next_step_handler(message, step10_move)

def step10_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record[-1].pop()  # убрать Размер
            step8_move(message, 1)
            return

        if not nazad:
            record[-1].append(message.text)  # Марка резины
            if not check_validation_marka(record, Mv, record[-1]):
                postpone_build(message, record, 2)
                return

        bot.send_message(
            message.chat.id,
            "Модель:",
            reply_markup=getKeyboardList(sorted(list(set(get_list_model(record, Mv, record[-1])))))
        )
        bot.register_next_step_handler(message, step11_move)

def step11_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record[-1].pop()  # убрать Марка резины
            step9_move(message, 1)
            return

        if not nazad:
            record[-1].append(message.text)  # Модель резины
            if not check_validation_model(record, Mv, record[-1]):
                postpone_build(message, record, 2)
                return
            # Добавляем сезон
            record[-1].append(get_sezon(record, Mv, record[-1]))

        bot.send_message(
            message.chat.id,
            "Тип диска:",
            reply_markup=getKeyboardList(key_type_disk)
        )
        bot.register_next_step_handler(message, step12_move)

def step12_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            # убрать Сезон (последний) и вернуться к выбору модели
            record[-1].pop()
            step10_move(message, 1)
            return

        if not nazad:
            record[-1].append(message.text)  # Тип диска

        bot.send_message(
            message.chat.id,
            "Сколько левых колес:",
            reply_markup=getKeyboardList(key_chisla)
        )
        bot.register_next_step_handler(message, step13_move)

def step13_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record[-1].pop()  # убрать Тип диска
            step11_move(message, 1)
            return

        try:
            if not nazad:
                count = int(message.text)
                record[-1].append(count)  # COUNT_LEFT
            bot.send_message(
                message.chat.id,
                "Сколько правых колес:",
                reply_markup=getKeyboardList(key_chisla)
            )
            bot.register_next_step_handler(message, step14_move)
        except Exception:
            bot.send_message(
                message.chat.id,
                "Введите целое число",
                reply_markup=getKeyboardList(key_chisla)
            )
            bot.register_next_step_handler(message, step13_move)


def step14_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for record in mvrecords:
        if record[Mv.USER_ID] != message.from_user.id:
            continue

        if message.text == "Назад" and nazad == 0:
            record[-1].pop()  # убрать COUNT_LEFT
            step12_move(message, 1)
            return

        try:
            if not nazad:
                count = int(message.text)
                record[-1].append(count)  # COUNT_RIGHT

            bot.send_message(
                message.chat.id,
                "Требуются еще колеса:",
                reply_markup=getKeyboardList(key_sogl)
            )
            bot.register_next_step_handler(message, step15_move)
        except Exception:
            bot.send_message(
                message.chat.id,
                "Введите целое число",
                reply_markup=getKeyboardList(key_chisla)
            )
            bot.register_next_step_handler(message, step14_move)

def step15_move(message, nazad=0):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                if str(record[Mv.TYPE_ACTION]) != "Забираете со склада":
                    record[len(record)-1].pop(len(record[len(record)-1]) - 1)
                    step13_move(message, 1)
                    return
            if message.text == "Да":
                if str(record[Mv.TYPE_ACTION]) == "Забираете со склада":
                    bot.send_message(message.chat.id, "Выберите вариант из предложенных:",
                                     reply_markup=getKeyboardStep1(key_type))
                    bot.register_next_step_handler(message, step_xab_move)
                else:
                    bot.send_message(message.chat.id, "Марка автомобиля:",
                                     reply_markup=getKeyboardList(get_list_marka_ts(record, Mv)))
                    bot.register_next_step_handler(message, step7_move, 2)
            else:
                bot.send_message(message.chat.id, get_report_move_str(record, Mv.COMPANY + 1, Mv.COMPANY + get_number_last_list(record) + 1), reply_markup=getKeyboardList(key_corek))
                bot.register_next_step_handler(message, step16_move)



def step16_move(message):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if message.text == "Заполнено корректно":
                bot.send_message(message.chat.id, "Прикрепите от 2 до 10 фото", reply_markup=telebot.types.ReplyKeyboardRemove())
            else:
                bot.send_message(message.chat.id, "Удалить:", reply_markup=getKeyboardList(get_report_move_list(record, Mv.COMPANY + 1, Mv.COMPANY + get_number_last_list(record)+1)))
                bot.register_next_step_handler(message, step17_move)

def step17_move(message):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            number_delete = get_number_delete(record, message.text, Mv.COMPANY + 1, Mv.COMPANY + get_number_last_list(record) + 1)
            record.pop(int(Mv.COMPANY) + int(number_delete))
            bot.send_message(message.chat.id, "Действие:", reply_markup=getKeyboardList(key_action_record))
            bot.register_next_step_handler(message, step18_move)

def step18_move(message):
    global mvrecords
    if check_exit(message, 3):
        return
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.from_user.id:
            if str(message.text) == "Завершить":
                bot.send_message(message.chat.id, "Прикрепите от 2 до 10 фото", reply_markup=telebot.types.ReplyKeyboardRemove())
            elif str(message.text) == "Добавить запись":
                if str(record[Mv.TYPE_ACTION]) == "Забираете со склада":
                    bot.send_message(message.chat.id, "Выберите вариант из предложенных:",
                                     reply_markup=getKeyboardStep1(key_type))
                    bot.register_next_step_handler(message, step_xab_move)
                else:
                    bot.send_message(message.chat.id, "Марка автомобиля:", reply_markup=getKeyboardList(get_list_marka_ts(record, Mv)))
                    bot.register_next_step_handler(message, step7_move, 2)
            else:
                lst = get_report_move_list(record, Mv.COMPANY + 1,
                                                                Mv.COMPANY + get_number_last_list(record) + 1)
                bot.send_message(message.chat.id, "Удалить:", reply_markup=getKeyboardList(lst))
                bot.register_next_step_handler(message, step17_move)

def get_report_move_list(record, begin, end):
    """
    Формирует строки отчёта для ветки move.
    Для каждой позиции (одинаковая марка/модель/размер/сезон/тип диска) считает:
      - Комплект: 2 левых + 2 правых  -> "Комплект Nшт"
      - Ось:      1 левый + 1 правый  -> "Ось Nшт"
      - Остатки:  "Левый Xшт" / "Правый Yшт"
    """

    def _to_int(v):
        try:
            s = str(v).strip()
            return int(s) if s else 0
        except Exception:
            return 0

    lines = []
    for mv in record[begin:end]:
        head = (
            f"🛞 {mv[Rc.MARKA_TS]} | "
            f"{mv[Rc.RAZMER]}/{mv[Rc.RADIUS]} | "
            f"{mv[Rc.MARKA_REZ]} {mv[Rc.MODEL_REZ]} | "
            f"{mv[Rc.SEZON]} | {mv[Rc.TIP_DISKA]} | "
        )

        left_count = _to_int(mv[Rc.COUNT_LEFT])
        right_count = _to_int(mv[Rc.COUNT_RIGHT])

        details = []

        # Комплект: 2 левых + 2 правых
        kit_count = min(left_count // 2, right_count // 2)
        if kit_count:
            details.append(f"Комплект {kit_count}шт")
            left_count -= kit_count * 2
            right_count -= kit_count * 2

        # Ось: 1 левый + 1 правый
        axle_count = min(left_count, right_count)
        if axle_count:
            details.append(f"Ось {axle_count}шт")
            left_count -= axle_count
            right_count -= axle_count

        # Остатки по сторонам
        if left_count:
            details.append(f"Левый {left_count}шт")
        if right_count:
            details.append(f"Правый {right_count}шт")

        tail = " | ".join(details) + " |" if details else "|"
        lines.append(head + tail)

    return lines

def get_report_move_str(record, begin, end):
    out_str = ""
    for i, rc in enumerate(get_report_move_list(record, begin, end)):
        out_str = out_str + rc + "\n\n"
    return out_str

def get_number_delete(record, text, begin, end):
    i = 0
    for rc in get_report_move_list(record, begin, end):
        i = i + 1
        if rc.replace(" ", "") == text.replace(" ", ""):
            return i

def get_number_last_list(record):
    max = 0
    for i, rc in enumerate(record):
        if isinstance(rc, list):
            max = max + 1
    return max
def generating_report_move(message):
    time.sleep(2)
    global mvrecords
    media = []
    resp = None
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == message.chat.id:
            # Если пользователь захватывал кэш, освобождаем его один раз
            if message.chat.id in users_with_cache:
                release_cache(message.from_user.id)

            number_last_list = get_number_last_list(record)
            if number_last_list == 0:
                bot.send_message(message.chat.id,
                                 "Не найдено ни одной позиции для отчёта. Добавьте хотя бы одну запись и повторите отправку фото")
                return
            if len(record) < number_last_list + 10:
                record.append(1)
                bot.send_message(message.chat.id, "Прикрепите еще фото")
                return
            for i, img in enumerate(record[number_last_list + 8:]):
                if i < 10:
                    media.append(dict(type=str(record[number_last_list + 7]), media=f'{img}'))
            if not media:
                bot.send_message(message.chat.id, "Нужно прикрепить от 2 до 10 фото одним сообщением")
                return
            if record[Mv.COMPANY] == "СитиДрайв":
                chatId = chat_id_Sity
                chat_id_for_link = link_Sity
                thread_id = thread_id_Sity_move
            elif record[Mv.COMPANY] == "Яндекс":
                chatId = chat_id_Yandex
                chat_id_for_link = link_Yandex
                thread_id = thread_id_Yandex_move
            else:
                chatId = chat_id_Belka
                chat_id_for_link = link_Belka
                thread_id = thread_id_Belka_move
            try:
                len_list = int(number_last_list / 5)
                ost_len_list = number_last_list % 5
                cur_i = 0
                if len_list:
                    for i in range(0,len_list):
                        cur_i = i + 1
                        begin = Mv.COMPANY + 1 + 5 * i
                        end = Mv.COMPANY + 1 + 5 * (i + 1)
                        media[0]['caption'] = generating_report_tg_move(message.chat.id, begin, end, i + 1)
                        dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(media)}
                        #dataSendMediaGroup = {'chat_id': str(chatId),'media': json.dumps(media)}
                        resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                        data = resp.json()
                        time.sleep(0.4)
                        #message_id = 11
                        message_id = data["result"][0]["message_id"]
                        message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
                        record.append(message_link)

                        if str(record[Mv.TYPE_ACTION]) != "Сдаете бой":
                            ok = update_xab_koles_bulk(
                                record[Mv.COMPANY],
                                [record[idx] for idx in range(begin, end)],
                                str(record[Mv.USERNAME]).strip(),
                                str(record[Mv.GRZ_TECH]).strip()
                            )
                            if ok != 1:
                                record.pop(len(record) - 1)
                                bot.send_message(message.chat.id,
                                                 "Не удалось списать позиции из хаба: вероятно, комплект уже забран/недоступен. "
                                                 "Заявка не записана в таблицу. Обновите список и попробуйте снова")
                                warn_text = (
                                    "⚠️ Списание не удалось, заявка НЕ зафиксирована в таблице. "
                                    "Игнорируйте сообщение выше"
                                )
                                try:
                                    bot.send_message(
                                        chat_id = chatId,
                                        message_thread_id = message_thread_id,
                                        text = warn_text
                                    )
                                except Exception as e:
                                    logger.warning("Не удалось отправить предупреждение в сервисный чат: %s", e)
                                return
                        generating_report_gs_move(message.chat.id, begin, end)
                        record.pop(len(record) - 1)
                if ost_len_list:
                    begin_tail = Mv.COMPANY + 1 + 5 * cur_i
                    end_tail = Mv.COMPANY + 1 + 5 * cur_i + ost_len_list
                    media[0]['caption'] = generating_report_tg_move(message.chat.id, begin_tail, end_tail, cur_i + 1)
                    dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(media)}
                    #dataSendMediaGroup = {'chat_id': str(chatId),'media': json.dumps(media)}
                    resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                    data = resp.json()
                    time.sleep(0.4)
                    #message_id = 11
                    message_id = data["result"][0]["message_id"]
                    message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
                    record.append(message_link)

                    if str(record[Mv.TYPE_ACTION]) != "Сдаете бой":
                        ok = update_xab_koles_bulk(
                            record[Mv.COMPANY],
                            [record[idx] for idx in range(begin_tail, end_tail)],
                            str(record[Mv.USERNAME]).strip(),
                            str(record[Mv.GRZ_TECH]).strip()
                        )
                        if ok != 1:
                            record.pop(len(record) - 1)
                            bot.send_message(message.chat.id,
                                             "Не удалось списать позиции из хаба: вероятно, комплект уже забран/недоступен. "
                                             "Заявка не записана в таблицу. Обновите список и попробуйте снова."
                                             )
                            warn_text = (
                                "⚠️ Списание не удалось, заявка НЕ зафиксирована в таблице. "
                                "Игнорируйте сообщение выше"
                            )
                            try:
                                bot.send_message(
                                    chat_id=chatId,
                                    message_thread_id=message_thread_id,
                                    text=warn_text
                                )
                            except Exception as e:
                                logger.warning("Не удалось отправить предупреждение в сервисный чат: %s", e)
                            return
                    generating_report_gs_move(message.chat.id, begin_tail, end_tail)
                    record.pop(len(record) - 1)

                # Если ничего не отправили (ни одной пачки) — корректно сообщаем
                if resp is None:
                    bot.send_message(message.chat.id, 'Не удалось сформировать заявку: нет позиций для выгрузки')
                elif resp.status_code < 400:
                    bot.send_message(message.chat.id, 'Ваша заявка сформирована')
                else:
                    bot.send_message(message.chat.id, 'При формировании заявки произошла ошибка')
                print_google_data(record[Mv.COMPANY])
                mvrecords.remove(record)
                return
            except Exception as e:
                mvrecords.remove(record)
                logging.exception(e)
                bot.send_message(message.chat.id, "Возникла ошибка. Обратитесь к разработчикам",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())


def generating_report_tg_move(from_user_id, begin, end, nomer):
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == from_user_id:
            str_answer = "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
            if nomer > 0:
                str_answer = str_answer + "#️⃣ " + str(nomer) + "\n\n"
            str_answer = str_answer + "🚚Техничка: " + record[Mv.GRZ_TECH] + "\n\n"
            str_answer = str_answer + "📌" + record[Mv.TYPE_ACTION] + "\n\n"
            if str(record[Mv.TYPE_ACTION]) == "Передаете в техничку":
                str_answer = str_answer + "🔀" + record[Mv.GRZ_PEREDACHA] + "\n\n"
            str_answer = str_answer + "👷 @" + str(record[Mv.USERNAME]) + "\n"
            str_answer = str_answer + str(record[Mv.FIO]) + "\n\n"
            str_answer = str_answer + "🏪"+ str(record[Mv.COMPANY]) + "\n\n"
            str_answer = str_answer + get_report_move_str(record, begin, end)
            return str_answer

def generating_report_gs_move(from_user_id, begin, end):
    for i, record in enumerate(mvrecords):
        if record[Mv.USER_ID] == from_user_id:
            sum_list = list()
            for i, mv in enumerate(record[begin:end]):
                count_left = int(mv[Rc.COUNT_LEFT])
                count_right = int(mv[Rc.COUNT_RIGHT])
                while count_left > 0:
                    tlist = list()
                    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
                    tlist.append(record[Mv.GRZ_TECH])
                    tlist.append(record[Mv.TYPE_ACTION])
                    tlist.append(record[Mv.GRZ_PEREDACHA])
                    tlist.append(record[Mv.COMPANY])
                    tlist.append(mv[Rc.MARKA_TS])
                    tlist.append(mv[Rc.RADIUS])
                    tlist.append(mv[Rc.RAZMER])
                    tlist.append(mv[Rc.MARKA_REZ])
                    tlist.append(mv[Rc.MODEL_REZ])
                    tlist.append(mv[Rc.SEZON])
                    tlist.append(mv[Rc.TIP_DISKA])
                    tlist.append("Левое")
                    tlist.append(record[len(record) - 1])
                    tlist.append(record[Mv.USERNAME])
                    sum_list.append(tlist)
                    count_left = count_left - 1
                while count_right > 0:
                    tlist = list()
                    tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
                    tlist.append(record[Mv.GRZ_TECH])
                    tlist.append(record[Mv.TYPE_ACTION])
                    tlist.append(record[Mv.GRZ_PEREDACHA])
                    tlist.append(record[Mv.COMPANY])
                    tlist.append(mv[Rc.MARKA_TS])
                    tlist.append(mv[Rc.RADIUS])
                    tlist.append(mv[Rc.RAZMER])
                    tlist.append(mv[Rc.MARKA_REZ])
                    tlist.append(mv[Rc.MODEL_REZ])
                    tlist.append(mv[Rc.SEZON])
                    tlist.append(mv[Rc.TIP_DISKA])
                    tlist.append("Правое")
                    tlist.append(record[len(record) - 1])
                    tlist.append(record[Mv.USERNAME])
                    sum_list.append(tlist)
                    count_right = count_right - 1
            write_answers_gs_rows(sum_list, "Выгрузка передача", gspread_url_peremeshenie)
            if str(record[Mv.TYPE_ACTION]) == "Сдаете бой":
                write_answers_gs_rows(sum_list, "Онлайн остатки Бой", gspread_url_peremeshenie)

@bot.message_handler(commands=['parking'])
def parking(message):
    global pkrecords
    # чистим старую сессию пользователя
    for i, record in enumerate(pkrecords):
        if record[0] == message.from_user.id:
            pkrecords.remove(record)

    if int(message.chat.id) > 0:
        tlist = list()
        try:
            data = {'chat_id': str(message.from_user.id)}
            resp = requests.get(urlSmallDtp, data=data)
            rep = resp.json()
            tlist.append(message.from_user.id)
            tlist.append(message.from_user.username)
            tlist.append(rep['user']['fullname'])
            pkrecords.append(tlist)
            step1_parking(message)
        except Exception as e:
            logging.exception(e)
            bot.send_message(message.chat.id, "Возникла ошибка чтения Ваших данных из базы КК. Убедитесь, что Вы успешно прошли регистрацию и повторите попытку оформления заявки. Регистрация производится по команде /start \nПри повторном возникновении ошибки обратитесь к разработчикам")
    else:
        bot.reply_to(message, "Перейдите в личные сообщение с ботом для оформления заявки",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def step1_parking(message):
    global pkrecords
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            bot.send_message(message.chat.id, "ГРЗ технички:", reply_markup=getKeyboardStep1(grz_tech))
            bot.register_next_step_handler(message, step2_parking)

def step2_parking(message, nazad=0):
    global pkrecords
    if check_exit(message, 0):
        return
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            record.append(message.text)
            if not check_validation_grz_tech(record, Pk.GRZ_TECH):
                postpone_build(message, record, 0)
                return
            bot.send_message(message.chat.id, "Компания:", reply_markup=getKeyboardList(key_company))
            bot.register_next_step_handler(message, step3_parking)


def step3_parking(message, nazad=0):
    global pkrecords
    if check_exit(message, 0):
        return
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                pkrecords.remove(record)
                parking(message)
                return
            if not nazad:
                record.append(message.text)
            bot.send_message(message.chat.id, "Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_exit))
            bot.register_next_step_handler(message, step4_parking)


def step4_parking(message, nazad=0):
    global pkrecords
    if check_exit(message, 0):
        return
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step2_parking(message)
                return
            grz_ts = getGRZTs(record, (message.text).lower(), Pk)
            if len(grz_ts):
                bot.send_message(message.chat.id, "Подтвердите ГРЗ из списка:", reply_markup=getKeyboardList(sorted(grz_ts)))
                bot.register_next_step_handler(message, step5_parking)
            else:
                bot.send_message(message.chat.id, "В базе данных нет введенного вами ГРЗ. Попробуйте снова", reply_markup=getKeyboardStep1(key_exit))
                bot.register_next_step_handler(message, step4_parking)

def step5_parking(message):
    global pkrecords
    if check_exit(message, 0):
        return
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.from_user.id:
            if message.text == "Назад":
                record.pop(len(record) - 1)
                step3_parking(message)
                return
            record.append(message.text)
            if not check_validation_grz(record, Pk):
                postpone_build(message, record, 0)
                return
            bot.send_message(message.chat.id, "Добавьте скриншот из приложения парковок (от 1 до 2 фото)",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_parking(message):
    time.sleep(2)
    global pkrecords
    media = []
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == message.chat.id:
            try:
                for i, img in enumerate(record[Pk.TIP_DOCUMENT + 1:]):
                    if i < 2:
                        media.append(dict(type=str(record[Pk.TIP_DOCUMENT]), media=f'{img}'))
                media[0]['caption'] = generating_report_tg_parking(message.chat.id)
                if record[Pk.COMPANY] == "СитиДрайв":
                    chatId = chat_id_Sity
                    chat_id_for_link = link_Sity
                    thread_id = thread_id_Sity
                elif record[Pk.COMPANY] == "Яндекс":
                    chatId = chat_id_Yandex
                    chat_id_for_link = link_Yandex
                    thread_id = thread_id_Yandex
                else:
                    chatId = chat_id_Belka
                    chat_id_for_link = link_Belka
                    thread_id = thread_id_Belka
                dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(media)}
                #dataSendMediaGroup = {'chat_id': str(chatId),'media': json.dumps(media)}
                resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                data = resp.json()
                #message_id = 11
                message_id = data["result"][0]["message_id"]
                message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
                record.append(message_link)
                generating_report_gs_parking(message.chat.id)
                if resp.status_code < 400:
                    bot.send_message(message.chat.id, 'Ваша заявка сформирована')
                else:
                    bot.send_message(message.chat.id, 'При формировании заявки произошла ошибка')
                pkrecords.remove(record)
                return
            except Exception as e:
                pkrecords.remove(record)
                logging.exception(e)
                bot.send_message(message.chat.id, "Возникла ошибка. Обратитесь к разработчикам",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_tg_parking(from_user_id):
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == from_user_id:
            str_answer = "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
            str_answer = str_answer + "👷 @" + str(record[Pk.USERNAME]) + "\n"
            str_answer = str_answer + str(record[Pk.FIO]) + "\n\n"
            str_answer = str_answer + str(record[Pk.COMPANY]) + "\n\n"
            str_answer = str_answer + "#️⃣ " + str(record[Pk.GRZ_ZADACHA]) + "\n"
            str_answer = str_answer + "#Парковка\n"
            str_answer = str_answer + "#" + str(record[Pk.GRZ_TECH]) + "\n"
            return str_answer

def generating_report_gs_parking(from_user_id):
    tlist = list()
    for i, record in enumerate(pkrecords):
        if record[Pk.USER_ID] == from_user_id:
            tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
            tlist.append(record[Pk.GRZ_TECH])
            tlist.append(record[Pk.COMPANY])
            tlist.append(record[Pk.GRZ_ZADACHA])
            tlist.append(record[Pk.USERNAME])
            tlist.append(record[Pk.FIO])
            tlist.append(record[len(record) - 1])  # ссылка на отчет
            write_answers_gs(tlist, "Городская парковка",gspread_url_rasxod_shm)

@bot.message_handler(commands=['zapravka'])
def zapravka(message):
    global zprecords
    for i, record in enumerate(zprecords):
        if record[0] == message.from_user.id:
            zprecords.remove(record)

    # команда только в ЛС
    if int(message.chat.id) < 0:
        bot.reply_to(message, "Эта команда доступна только в личных сообщениях с ботом")
        return

    user = message.from_user
    logger.info("[zapravka] start: @%s (%s)", user.username, user.id)

    try:
        # получаем ФИО
        resp = requests.get(
            urlSmallDtp,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()
        fio = (rep.get("user") or {}).get("fullname") or "—"

        # получаем задачи
        resp = requests.get(
            URL_GET_INFO_TASK,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()

        if isinstance(rep, list):
            tasks = rep
        elif isinstance(rep, dict):
            tasks = rep.get("active_tasks") or rep.get("tasks") or []
        else:
            tasks = []

    except Exception as e:
        logger.exception("Ошибка при запросе задач для zapravka: %s", e)
        bot.send_message(message.chat.id, "Не удалось получить информацию по задачам. Попробуйте позже")
        return

    # Если задач нет
    if not tasks:
        if message.chat.id in list_users:
            # тестовый режим
            task = {
                "task_type": "Перегон СШМ",
                "carsharing__name": "Тестовая компания",
                "car_plate": "Т000ТТ000",
                "car_model": "TestCar",
            }
            tasks = [task]
        else:
            bot.send_message(message.chat.id, "У вас нет активной задачи")
            return

    # Если задача одна — старое поведение
    if len(tasks) == 1:
        task = tasks[0]
        tlist = []
        tlist.append(user.id)
        tlist.append(user.username)
        tlist.append(fio)
        tlist.append(task.get("car_plate") or "—")
        tlist.append(task.get("carsharing__name") or "—")
        zprecords.append(tlist)
        step3_zapravka(message)
        return

    # Если задач несколько — предлагаем выбор
    kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons = []

    for task in tasks:
        plate = task.get("car_plate") or "—"
        company = task.get("carsharing__name") or "—"
        btn_text = f"{plate} | {company}"
        buttons.append(btn_text)

    # добавим кнопку выхода / отмены
    cancel_text = "Выход"
    buttons.append(cancel_text)

    # разложим по строкам
    for text in buttons:
        kb.row(text)

    msg = bot.send_message(
        message.chat.id,
        "У вас несколько активных задач.\nВыберите задачу для оформления заправки:",
        reply_markup=kb
    )
    # передаём fio и сами tasks в следующий шаг
    bot.register_next_step_handler(msg, step_select_task_zapravka, fio, tasks)

def step_select_task_zapravka(message, fio, tasks):
    global zprecords

    text = (message.text or "").strip()

    # обработка отмены
    if text.lower() == "выход":
        bot.send_message(
            message.chat.id,
            "Оформление заправки отменено",
            reply_markup=telebot.types.ReplyKeyboardRemove()
        )
        return

    # ищем задачу по тексту кнопки
    selected_task = None
    for task in tasks:
        plate = task.get("car_plate") or "—"
        company = task.get("carsharing__name") or "—"
        btn_text = f"{plate} | {company}"
        if text == btn_text:
            selected_task = task
            break

    # если ввели что-то своё, а не нажали кнопку
    if selected_task is None:
        kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        for task in tasks:
            plate = task.get("car_plate") or "—"
            company = task.get("carsharing__name") or "—"
            kb.row(f"{plate} | {company}")
        kb.row("Выход")

        bot.send_message(
            message.chat.id,
            "Пожалуйста, выберите задачу, нажав на кнопку из списка",
            reply_markup=kb
        )
        bot.register_next_step_handler(message, step_select_task_zapravka, fio, tasks)
        return

    # формируем запись, как раньше
    user = message.from_user
    tlist = []
    tlist.append(user.id)
    tlist.append(user.username)
    tlist.append(fio)
    tlist.append(selected_task.get("car_plate") or "—")
    tlist.append(selected_task.get("carsharing__name") or "—")
    zprecords.append(tlist)

    bot.send_message(
        message.chat.id,
        "Задача выбрана",
        reply_markup=telebot.types.ReplyKeyboardRemove()
    )
    step3_zapravka(message)

def step3_zapravka(message, nazad=0):
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == message.from_user.id:
            bot.send_message(message.chat.id, f"Активная задача:\nГРЗ технички: {record[Zp.GRZ_TECH]}\nКомпания: {record[Zp.COMPANY]}\nУкажите показания одометра:",reply_markup=getKeyboardStep1(key_exit))
            bot.register_next_step_handler(message, step4_zapravka)

def step4_zapravka(message, nazad=0):
    global zprecords
    if check_exit(message, 1):
        return
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == message.from_user.id:
            raw = message.text.strip().replace(",", ".")
            try:
                summa = float(raw)
            except ValueError:
                bot.send_message(
                    message.chat.id,
                    "Введите значение в формате 101.11 или 101,11",
                    reply_markup=getKeyboardList(key_exit)
                )
                bot.register_next_step_handler(message, step4_zapravka)
                return

            record.append(summa)
            bot.send_message(message.chat.id, "Укажите сумму заправки:",reply_markup=getKeyboardList(key_exit))
            bot.register_next_step_handler(message, step5_zapravka)

def step5_zapravka(message, nazad=0):
    global zprecords
    if check_exit(message, 1):
        return
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                zprecords.remove(record)
                zapravka(message)
                return
            if nazad == 0:
                raw = message.text.strip().replace(",", ".")
                try:
                    summa = float(raw)
                except ValueError:
                    bot.send_message(
                        message.chat.id,
                        "Введите значение в формате 101.11 или 101,11",
                        reply_markup=getKeyboardList(key_exit)
                    )
                    bot.register_next_step_handler(message, step5_zapravka)
                    return

                record.append(summa)

            bot.send_message(message.chat.id, "Добавьте скриншот из приложения ППР, фото приборной панели ДО и ПОСЛЕ заправки",
                             reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_zapravka(message):
    time.sleep(2)
    global zprecords
    media = []
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == message.chat.id:
            try:
                if len(record) < Zp.TIP_DOCUMENT + 4:
                    record.append(1)
                    bot.send_message(message.chat.id, "Прикрепите еще фото")
                    return
                for i, img in enumerate(record[Zp.TIP_DOCUMENT + 1:]):
                    if i < 3:
                        media.append(dict(type=str(record[Zp.TIP_DOCUMENT]), media=f'{img}'))
                media[0]['caption'] = generating_report_tg_zapravka(message.chat.id)
                if record[Pk.COMPANY] == "СитиДрайв":
                    chatId = chat_id_Sity
                    chat_id_for_link = link_Sity
                    thread_id = thread_id_Sity_zapr
                elif record[Pk.COMPANY] == "Яндекс":
                    chatId = chat_id_Yandex
                    chat_id_for_link = link_Yandex
                    thread_id = thread_id_Yandex_zapr
                else:
                    chatId = chat_id_Belka
                    chat_id_for_link = link_Belka
                    thread_id = thread_id_Belka_zapr
                dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(media)}
                #dataSendMediaGroup = {'chat_id': str(chatId),'media': json.dumps(media)}
                resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                data = resp.json()
                #message_id = 11
                message_id = data["result"][0]["message_id"]
                message_link = f"https://t.me/c/{chat_id_for_link}/{message_id}"
                record.append(message_link)
                generating_report_gs_zapravka(message.chat.id)
                if resp.status_code < 400:
                    bot.send_message(message.chat.id, 'Ваша заявка сформирована')
                else:
                    bot.send_message(message.chat.id, 'При формировании заявки произошла ошибка')
                zprecords.remove(record)
                return
            except Exception as e:
                zprecords.remove(record)
                logging.exception(e)
                bot.send_message(message.chat.id, "Возникла ошибка. Обратитесь к разработчикам",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_tg_zapravka(from_user_id):
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == from_user_id:
            str_answer = "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
            str_answer = str_answer + "👷 @" + str(record[Zp.USERNAME]) + "\n"
            str_answer = str_answer + str(record[Zp.FIO]) + "\n\n"
            str_answer = str_answer + "#" + str(record[Zp.GRZ_TECH]) + "\n"
            str_answer = str_answer + str(record[Zp.COMPANY]) + "\n\n"
            str_answer = str_answer + str(record[Zp.PROBEG]) + "\n"
            str_answer = str_answer + str(record[Zp.SUMMA]) + "\n"
            str_answer = str_answer + "#Заправка\n"
            return str_answer

def generating_report_gs_zapravka(from_user_id):
    tlist = list()
    for i, record in enumerate(zprecords):
        if record[Zp.USER_ID] == from_user_id:
            tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
            tlist.append(record[Zp.GRZ_TECH])
            tlist.append(record[Zp.COMPANY])
            tlist.append(record[Zp.PROBEG])
            tlist.append(record[Zp.SUMMA])
            tlist.append(record[Zp.USERNAME])
            tlist.append(record[Zp.FIO])
            tlist.append(record[len(record) - 1])  # ссылка на отчет
            write_answers_gs(tlist, "Заправка техничек",gspread_url_rasxod_shm)

# Сценарий "Расход"
@bot.message_handler(commands=['expense'])
def expense(message):
    global rsrecords
    # чистим старую сессию пользователя
    for i, record in enumerate(rsrecords):
        if record[0] == message.from_user.id:
            rsrecords.remove(record)

    # команда только в ЛС
    if int(message.chat.id) < 0:
        bot.reply_to(message, "Эта команда доступна только в личных сообщениях с ботом")
        return

    user = message.from_user
    logger.info("[expense] start: @%s (%s)", user.username, user.id)

    try:
        # получаем ФИО
        resp = requests.get(
            urlSmallDtp,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()
        fio = (rep.get("user") or {}).get("fullname") or "—"

        # получаем задачи
        resp = requests.get(
            URL_GET_INFO_TASK,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()

        if isinstance(rep, list):
            tasks = rep
        elif isinstance(rep, dict):
            tasks = rep.get("active_tasks") or rep.get("tasks") or []
        else:
            tasks = []

    except Exception as e:
        logger.exception("Ошибка при запросе задач для expense: %s", e)
        bot.send_message(message.chat.id, "Не удалось получить информацию по задачам. Попробуйте позже")
        return

    # Если задач нет
    if not tasks:
        if message.chat.id in list_users:
            # тестовый режим
            task = {
                "task_type": "Перегон СШМ",
                "carsharing__name": "Тестовая компания",
                "car_plate": "Т000ТТ000",
                "car_model": "TestCar",
            }
            tasks = [task]
        else:
            bot.send_message(message.chat.id, "У вас нет активной задачи")
            return

    # Если задача одна — старое поведение
    if len(tasks) == 1:
        task = tasks[0]
        tlist = []
        tlist.append(user.id)
        tlist.append(user.username)
        tlist.append(fio)
        tlist.append(task.get("car_plate") or "—")
        tlist.append(task.get("carsharing__name") or "—")
        rsrecords.append(tlist)
        step3_expense(message)
        return

    # Если задач несколько — предлагаем выбор
    kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons = []

    for task in tasks:
        plate = task.get("car_plate") or "—"
        company = task.get("carsharing__name") or "—"
        btn_text = f"{plate} | {company}"
        buttons.append(btn_text)

    cancel_text = "Выход"
    buttons.append(cancel_text)

    for text in buttons:
        kb.row(text)

    msg = bot.send_message(
        message.chat.id,
        "У вас несколько активных задач.\nВыберите задачу для оформления расхода:",
        reply_markup=kb
    )
    # передаём fio и tasks в следующий шаг
    bot.register_next_step_handler(msg, step_select_task_expense, fio, tasks)

def step_select_task_expense(message, fio, tasks):
    global rsrecords

    text = (message.text or "").strip()

    # обработка отмены
    if text.lower() == "выход":
        bot.send_message(
            message.chat.id,
            "Оформление расхода отменено",
            reply_markup=telebot.types.ReplyKeyboardRemove()
        )
        return

    # ищем задачу по тексту кнопки
    selected_task = None
    for task in tasks:
        plate = task.get("car_plate") or "—"
        company = task.get("carsharing__name") or "—"
        btn_text = f"{plate} | {company}"
        if text == btn_text:
            selected_task = task
            break

    # если пользователь ввёл произвольный текст, а не нажал кнопку
    if selected_task is None:
        kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        for task in tasks:
            plate = task.get("car_plate") or "—"
            company = task.get("carsharing__name") or "—"
            kb.row(f"{plate} | {company}")
        kb.row("Выход")

        bot.send_message(
            message.chat.id,
            "Пожалуйста, выберите задачу, нажав на кнопку из списка",
            reply_markup=kb
        )
        bot.register_next_step_handler(message, step_select_task_expense, fio, tasks)
        return

    # формируем запись так же, как раньше
    user = message.from_user
    tlist = []
    tlist.append(user.id)
    tlist.append(user.username)
    tlist.append(fio)
    tlist.append(selected_task.get("car_plate") or "—")
    tlist.append(selected_task.get("carsharing__name") or "—")
    rsrecords.append(tlist)

    # убираем клавиатуру и идём в обычный сценарий
    bot.send_message(
        message.chat.id,
        "Задача выбрана",
        reply_markup=telebot.types.ReplyKeyboardRemove()
    )
    step3_expense(message)


def step3_expense(message, nazad=0):
    global rsrecords
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            bot.send_message(message.chat.id, f"Активная задача:\nГРЗ технички: {record[Zp.GRZ_TECH]}\nКомпания: {record[Zp.COMPANY]}\nВыберите город из списка или введите вручную:",reply_markup=getKeyboardStep1(key_sity))
            bot.register_next_step_handler(message, step4_expense)

def step4_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if not nazad:
                record.append(message.text)
            bot.send_message(message.chat.id, "Начните ввод госномера задачи:", reply_markup=getKeyboardList(key_exit))
            bot.register_next_step_handler(message, step5_expense)

def step5_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                rsrecords.remove(record)
                expense(message)
                return
            grz_ts = getGRZTs(record, (message.text).lower(), Rs)
            if len(grz_ts):
                bot.send_message(message.chat.id, "Подтвердите ГРЗ из списка:", reply_markup=getKeyboardList(sorted(grz_ts)))
                bot.register_next_step_handler(message, step6_expense)
            else:
                bot.send_message(message.chat.id, "В базе данных нет введенного вами ГРЗ. Попробуйте снова", reply_markup=getKeyboardStep1(key_exit))
                bot.register_next_step_handler(message, step5_expense)

def step6_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step4_expense(message, 1)
                return
            record.append(message.text)
            bot.send_message(message.chat.id, "Введите сумму с 2 знаками после точки, пример: 5678.91",
                             reply_markup=getKeyboardList(key_exit))
            bot.register_next_step_handler(message, step7_expense)

def step7_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step4_expense(message, 1)
                return
            try:
                if nazad == 0:
                    summa = float(message.text)
                    record.append(summa)
                bot.send_message(message.chat.id, 'Способ оплаты', reply_markup=getKeyboardList(key_oplata))
                bot.register_next_step_handler(message, step8_expense)
            except Exception as e:
                    bot.send_message(message.chat.id, "Введите сумму с 2 знаками после точки, пример: 5678.91",
                                     reply_markup=getKeyboardList(key_exit))
                    bot.register_next_step_handler(message, step7_expense)

def step8_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step6_expense(message, 1)
                return
            if nazad == 0:
                check = 0
                if message.text in key_oplata:
                    record.append(message.text)
                    check = 1
                if check == 0:
                    bot.send_message(message.chat.id, 'Вы ввели способ оплаты не из предложенного списка\nПопробуйте еще раз:',
                    reply_markup=getKeyboardList(key_oplata))
                    bot.register_next_step_handler(message, step8_expense)
                    return
            if record[Rs.OPLATA] == "Наличные <> Перевод <> Личная карта":
                bot.send_message(message.chat.id, 'Выберите из следующих категорий', reply_markup=getKeyboardList(key_oplata_dop))
                bot.register_next_step_handler(message, step9_expense)
            else:
                bot.send_message(message.chat.id, "Укажите причину расхода", reply_markup=getKeyboardList(key_exit))
                bot.register_next_step_handler(message, step10_expense)

def step9_expense(message, nazad=0):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and nazad == 0:
                record.pop(len(record) - 1)
                step7_expense(message, 1)
                return
            if nazad == 0:
                record.append(message.text)
            bot.send_message(message.chat.id, "Укажите причину расхода", reply_markup=getKeyboardList(key_exit))
            bot.register_next_step_handler(message, step10_expense)

def step10_expense(message):
    global rsrecords
    if check_exit(message, 2):
        return
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.from_user.id:
            if message.text == "Назад" and record[Rs.OPLATA] == "Наличные <> Перевод <> Личная карта":
                record.pop(len(record) - 1)
                step8_expense(message, 1)
                return
            if message.text == "Назад" and record[Rs.OPLATA] == "Бизнес-карта":
                record.pop(len(record) - 1)
                step7_expense(message, 1)
                return
            if record[Rs.OPLATA] == "Бизнес-карта":
                record.append("")
            record.append(message.text)
            bot.send_message(message.chat.id, "Загрузите фото чека/счета (от 1 до 4 фото) в одном сообщении", reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_expense(message):
    time.sleep(2)
    global rsrecords
    media = []
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == message.chat.id:
            try:
                for i, img in enumerate(record[Rs.TIP_DOCUMENT + 1:]):
                    if i < 4:
                        media.append(dict(type=str(record[Rs.TIP_DOCUMENT]), media=f'{img}'))
                media[0]['caption'] = generating_report_tg_expense(message.chat.id)
                if record[Rs.COMPANY] == "СитиДрайв":
                    chatId = chat_id_Sity
                    thread_id = thread_id_Sity_ras
                elif record[Rs.COMPANY] == "Яндекс":
                    chatId = chat_id_Yandex
                    thread_id = thread_id_Yandex_ras
                else:
                    chatId = chat_id_Belka
                    thread_id = thread_id_Belka_ras
                dataSendMediaGroup = {'chat_id': str(chatId), 'message_thread_id': str(thread_id), 'media': json.dumps(media)}
                #dataSendMediaGroup = {'chat_id': str(chatId),'media': json.dumps(media)}
                resp = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                dataSendMediaGroup = {'chat_id': str(chat_id_common),'media': json.dumps(media)}
                resp_common = requests.post(urlSendMediaGroup, data=dataSendMediaGroup)
                generating_report_gs_expense(message.chat.id)
                if resp.status_code < 400 and resp_common.status_code < 400:
                    bot.send_message(message.chat.id, 'Ваша заявка сформирована')
                else:
                    bot.send_message(message.chat.id, 'При формировании заявки произошла ошибка')
                rsrecords.remove(record)
                return
            except Exception as e:
                rsrecords.remove(record)
                logging.exception(e)
                bot.send_message(message.chat.id, "Возникла ошибка. Обратитесь к разработчикам",
                                 reply_markup=telebot.types.ReplyKeyboardRemove())

def generating_report_tg_expense(from_user_id):
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == from_user_id:
            str_answer = "⌚️ " + str((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")) + "\n\n"
            str_answer = str_answer + "👷 @" + str(record[Rs.USERNAME]) + "\n"
            str_answer = str_answer + str(record[Rs.FIO]) + "\n\n"
            str_answer = str_answer + str(record[Rs.GOROD]) + "\n"
            str_answer = str_answer + "ШМ\n"
            str_answer = str_answer + str(record[Rs.SUMMA]) + "\n"
            if str(record[Rs.DOP_OPLATA]) == "Подача на возмещение(свои деньги) + 6%":
                str_answer = str_answer + str(round(record[Rs.SUMMA]/94*100,2)).replace(".", ",") + "\n\n"
            str_answer = str_answer + str(record[Rs.COMPANY]) + "\n"
            str_answer = str_answer + str(record[Rs.OPLATA]) + "\n"
            str_answer = str_answer + str(record[Rs.PRICIHA]) + "\n\n"
            str_answer = str_answer + "#" + str(record[Rs.GRZ_TECH]) + "\n"
            str_answer = str_answer + str(record[Rs.GRZ_ZADACHA]) + "\n"
            if str(record[Rs.DOP_OPLATA]) == "Подача на возмещение(свои деньги) + 6%":
                str_answer = str_answer + "\n@Anastasiya_CleanCar, cогласуйте, пожалуйста"
            return str_answer

def generating_report_gs_expense(from_user_id):
    tlist = list()
    for i, record in enumerate(rsrecords):
        if record[Rs.USER_ID] == from_user_id:
            tlist.append((datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S"))
            tlist.append(record[Rs.FIO])
            tlist.append(record[Rs.USERNAME])
            tlist.append(record[Rs.GOROD])
            tlist.append(record[Rs.SUMMA])
            if str(record[Rs.DOP_OPLATA]) == "Подача на возмещение(свои деньги) + 6%":
                tlist.append(str(round(record[Rs.SUMMA]/94*100,2)).replace(".", ","))
            else:
                tlist.append("")
            tlist.append("КлинКар Сервис")
            tlist.append("ШМ")
            tlist.append(record[Rs.OPLATA])
            tlist.append(record[Rs.DOP_OPLATA])
            tlist.append(record[Rs.PRICIHA])
            tlist.append(record[Rs.GRZ_TECH])
            tlist.append(record[Rs.GRZ_ZADACHA])
            write_answers_gs(tlist, "Лист1",gspread_url_rasxod)
# -----------------------------------------------------------------------------------------------------
# Keyboar
# -----------------------------------------------------------------------------------------------------
def getKeyboardList(record_list):
    keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for record in record_list:
        keyboard.row(telebot.types.KeyboardButton(text=str(record)))
    keyboard.row(telebot.types.KeyboardButton(text="Назад"), telebot.types.KeyboardButton(text="Выход"))
    return keyboard

def show_xab_page(message, user_id, start_over=False):
    state = xab_pages.get(user_id)
    if not state:
        return

    per_page = XAB_PER_PAGE
    n = len(state["options"])
    if n == 0:
        bot.send_message(message.chat.id,
            "В хабе нет выбранного варианта, выберете другой вариант:",
            reply_markup=getKeyboardList(key_type))
        return

    page = 0 if start_over else state.get("page", 0)
    max_page = (n - 1) // per_page
    page = max(0, min(page, max_page))
    state["page"] = page

    start = page * per_page
    end = min(start + per_page, n)
    slice_opts = list(state["options"][start:end])

    # добавляем кнопку «Ещё», если дальше есть позиции
    if end < n:
        slice_opts.append("Ещё")

    kb = getKeyboardList(slice_opts)
    bot.send_message(message.chat.id,
        "Выберите вариант из предложенных:",
        reply_markup=kb)


def getKeyboardStep1(record_list):
    keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for record in record_list:
        keyboard.row(telebot.types.KeyboardButton(text=str(record)))
    keyboard.row(telebot.types.KeyboardButton(text="Выход"))
    return keyboard


# -----------------------------------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------------------------------
def _is_retryable_http_status(status: int) -> bool:
    # 429 Too Many Requests и любые 5xx — пробуем повторить
    return status == 429 or (500 <= status <= 599)

def _sleep_with_jitter(base_delay: float, attempt: int) -> None:
    # экспоненциальный backoff + небольшой джиттер
    delay = base_delay * (2 ** (attempt - 1))
    delay = delay + random.uniform(0, 0.2)  # 0–200 мс джиттер
    time.sleep(delay)

def gspread_open_by_url_with_retry(url: str, max_retries: int = 5) -> Spreadsheet:
    """
    Открывает таблицу с повторами на 429/5xx (APIError).
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            gc: Client = gspread.service_account("app/creds.json")
            return gc.open_by_url(url)
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("open_by_url retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)
                last_exc = e
                continue
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            break
    raise last_exc

def worksheet_get_all_values_with_retry(ws, max_retries: int = 5):
    """
    ws.get_all_values() с повторами на 429/5xx.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return ws.get_all_values()
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("get_all_values retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)
                last_exc = e
                continue
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            break
    raise last_exc

def worksheet_append_rows_with_retry(ws, rows: list, max_retries: int = 5):
    """
    ws.append_rows(...) с повторами на 429/5xx.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            ws.append_rows(rows, value_input_option='USER_ENTERED',
                           table_range='A1', insert_data_option='INSERT_ROWS')
            return
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("append_rows retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)
                last_exc = e
                continue
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            break
    raise last_exc

def worksheet_append_row_with_retry(ws, row: list, max_retries: int = 5):
    """
    ws.append_row(...) с повторами на 429/5xx.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            ws.append_row(row, value_input_option='USER_ENTERED',
                          table_range='A1', insert_data_option='INSERT_ROWS')
            return
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("append_row retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)
                last_exc = e
                continue
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            break
    raise last_exc

def gs_values_batch_update_with_retry(sh: Spreadsheet, data_payload: list, max_retries: int = 5) -> None:
    """
    Обёртка над Spreadsheet.values_batch_update с retry/backoff на 429/5xx.
    """
    for attempt in range(1, max_retries + 1):
        try:
            sh.values_batch_update({
                "valueInputOption": "USER_ENTERED",
                "data": data_payload
            })
            return
        except Exception as e:
            # gspread APIError имеет response с status_code; requests.HTTPError тоже может содержать код
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is None:
                # попробуем вытащить код глубже (иногда gspread его вкладывает внутрь)
                status = getattr(getattr(getattr(e, "response", None), "status", None), "code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("values_batch_update retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)  # 0.5, 1.0, 2.0, ...
                continue
            logger.exception("values_batch_update failed (no retry or maxed out): %s", e)
            raise

def gs_batch_update_with_retry(sh: Spreadsheet, requests_body: list, max_retries: int = 5) -> None:
    """
    Обёртка над Spreadsheet.batch_update с retry/backoff на 429/5xx.
    """
    for attempt in range(1, max_retries + 1):
        try:
            sh.batch_update({"requests": requests_body})
            return
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is None:
                status = getattr(getattr(getattr(e, "response", None), "status", None), "code", None)
            if status is not None and _is_retryable_http_status(int(status)) and attempt < max_retries:
                logger.warning("batch_update retry %d due to HTTP %s", attempt, status)
                _sleep_with_jitter(0.5, attempt)
                continue
            logger.exception("batch_update failed (no retry or maxed out): %s", e)
            raise

def write_answers_gs_rows(tlist, name_gs, url_gs):
    try:
        sh: Spreadsheet = gspread_open_by_url_with_retry(url_gs)
        ws = sh.worksheet(name_gs)
        worksheet_append_rows_with_retry(ws, tlist)
    except Exception as e:
        logger.exception(
            "Ошибка выполнения write_answers_gs_rows: name_gs=%s url_gs=%s tlist=%s",
            name_gs, url_gs, tlist
        )
        try:
            bot.send_message(
                547087397,
                f"write_answers_gs_rows упала:\n{name_gs}\n{url_gs}\n{e}"
            )
        except Exception:
            pass

def write_open_gate_row(fio: str, car_plate: str, company: str, message_link: str) -> None:
    try:
        sh: Spreadsheet = gspread_open_by_url_with_retry(gspread_url_gates)
        ws = sh.worksheet("Выгрузка Техники")
        now_msk = datetime.now() + timedelta(hours=3)
        row = [
            now_msk.strftime("%d.%m.%Y"),  # Дата
            now_msk.strftime("%H:%M:%S"),  # Время
            fio,
            car_plate,
            company,
            message_link,
        ]
        ws.append_row(row, value_input_option='USER_ENTERED', table_range='A1', insert_data_option='INSERT_ROWS')
    except Exception as e:
        logger.exception("Ошибка выполнения write_open_gate_row")

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
    sh: Spreadsheet = gspread_open_by_url_with_retry(URL_GOOGLE_SHEETS_CHART)
    ws = sh.worksheet("Логисты выход на смену")
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

def write_answers_gs(tlist, name_gs, url_gs):
    try:
        sh: Spreadsheet = gspread_open_by_url_with_retry(url_gs)
        ws = sh.worksheet(name_gs)
        worksheet_append_row_with_retry(ws, tlist)
    except Exception as e:
        logger.exception(
            "Ошибка выполнения write_answers_gs: name_gs=%s url_gs=%s tlist=%s",
            name_gs, url_gs, tlist
        )
        try:
            bot.send_message(
                547087397,
                f"write_answers_gs упала:\n{name_gs}\n{url_gs}\n{e}"
            )
        except Exception:
            pass


def getGRZTech():
    sh: Spreadsheet = gspread_open_by_url_with_retry(gspread_url_baza_zn)
    ws = sh.worksheet("Наши технички")
    list_of_lists = worksheet_get_all_values_with_retry(ws)[1:]
    grz = list()
    for tlist in list_of_lists:
        grz.append(str(tlist[1]))
    return grz
def getGRZTs(record, input_grz, Cl):
    grz = list()
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = grz_ts_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = grz_ts_ya
    else:
        lst = grz_ts_blk
    for i,string in enumerate(lst):
        if string.startswith(input_grz):
            grz.append(string)
    return grz


def check_exit(message, typ):
    if message.text == "Выход":
        if typ == 0:
            global pkrecords
            for i, record in enumerate(pkrecords):
                if record[Pk.USER_ID] == message.from_user.id:
                    bot.send_message(message.chat.id, "Оформление заявки завершено",
                                     reply_markup=telebot.types.ReplyKeyboardRemove())
                    pkrecords.remove(record)
                    return 1
        elif typ == 1:
            global zprecords
            for i, record in enumerate(zprecords):
                if record[Zp.USER_ID] == message.from_user.id:
                    bot.send_message(message.chat.id, "Оформление заявки завершено",
                                     reply_markup=telebot.types.ReplyKeyboardRemove())
                    zprecords.remove(record)
                    return 1
        elif typ == 2:
            global rsrecords
            for i, record in enumerate(rsrecords):
                if record[Rs.USER_ID] == message.from_user.id:
                    bot.send_message(message.chat.id, "Оформление заявки завершено",
                                     reply_markup=telebot.types.ReplyKeyboardRemove())
                    rsrecords.remove(record)
                    return 1
        elif typ == 3:
            global mvrecords
            for i, record in enumerate(mvrecords):
                if record[Mv.USER_ID] == message.from_user.id:
                    bot.send_message(message.chat.id, "Оформление заявки завершено",
                                     reply_markup=telebot.types.ReplyKeyboardRemove())
                    mvrecords.remove(record)
                    release_cache( message.from_user.id)
                    return 1

def check_validation_grz(record, Cl):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = grz_ts_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = grz_ts_ya
    else:
        lst = grz_ts_blk
    return check_grz(lst, record, Cl)


def check_grz(bz_znan, record, Cl):
    for i, rez in enumerate(bz_znan):
        if str(record[Cl.GRZ_ZADACHA]).strip() == str(rez).strip():
            return 1

def check_validation_grz_tech(record, number):
    for i, rez in enumerate(grz_tech):
        if str(record[number]) == str(rez).strip():
            return 1
    return 0
def check_validation_marka_ts(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = marka_ts_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = marka_ts_ya
    else:
        lst = marka_ts_blk
    return check_marka_ts(lst, cur_record)


def check_marka_ts(baza_zn, cur_record):
    for i, rez in enumerate(baza_zn):
        if str(cur_record[Rc.MARKA_TS]).strip() == str(rez).strip():
            return 1
    return 0

def check_validation_radius(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_radius(lst, cur_record)

def check_radius(bz_znan, record):
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip():
            return 1


def check_validation_razmer(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_razmer(lst, cur_record)


def check_razmer(bz_znan, record):
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip():
            return 1

def check_validation_marka(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_marka(lst, cur_record)


def check_marka(bz_znan, record):
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip() and str(record[Rc.MARKA_REZ]) == str(rez[4]).strip():
            return 1


def check_validation_model(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return check_model(lst, cur_record)


def check_model(bz_znan, record):
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip() and str(record[Rc.MARKA_REZ]) == str(rez[4]).strip() and str(record[Rc.MODEL_REZ]) == str(rez[5]).strip():
            return 1

def postpone_build(message, record, typ):
    bot.send_message(message.chat.id,"Введён параметр не из предложенных вариантов. Обратитесь к [Сергею](tg://user?id=1050518459) для обновления базы", parse_mode = "Markdown",reply_markup = telebot.types.ReplyKeyboardRemove())
    if typ == 0:
        global pkrecords
        pkrecords.remove(record)
    if typ == 1:
        global rsrecords
        rsrecords.remove(record)
    if typ == 2:
        global mvrecords
        mvrecords.remove(record)


def loading_grz_is_Google_Sheets():
    global grz_ts_st
    grz_ts_st = loading_bz_znaniya_grz("Перечень ТС Сити")
    global grz_ts_ya
    grz_ts_ya = loading_bz_znaniya_grz("Перечень ТС Яд")
    global grz_ts_blk
    grz_ts_blk = loading_bz_znaniya_grz("Перечень ТС Белка")
    global marka_ts_st
    marka_ts_st = loading_bz_znaniya_marka("Перечень ТС Сити", 2)
    global marka_ts_ya
    marka_ts_ya = loading_bz_znaniya_marka("Перечень ТС Яд", 3)
    global marka_ts_blk
    marka_ts_blk = loading_bz_znaniya_marka("Перечень ТС Белка", 1)
    global list_rez_st
    list_rez_st = loading_bz_znaniya_rezina("Резина Сити")
    global list_rez_ya
    list_rez_ya = loading_bz_znaniya_rezina("Резина ЯД")
    global list_rez_blk
    list_rez_blk = loading_bz_znaniya_rezina("Резина Белка")
    global grz_tech
    grz_tech = getGRZTech()

def loading_bz_znaniya_grz(company):
    sh: Spreadsheet = gspread_open_by_url_with_retry(gspread_url_baza_zn)
    ws_direct = sh.worksheet(company)
    list_of_lists = worksheet_get_all_values_with_retry(ws_direct)[1:]
    grz = list()
    if company == "Перечень ТС Белка":
        index = 2
    else:
        index = 0
    for tlist in list_of_lists:
        grz.append(str(tlist[index]))
    return grz

def loading_bz_znaniya_marka(company, nomer):
    sh: Spreadsheet = gspread_open_by_url_with_retry(gspread_url_baza_zn)
    ws_direct = sh.worksheet(company)
    list_of_lists = worksheet_get_all_values_with_retry(ws_direct)[1:]
    marka = list()
    for tlist in list_of_lists:
        marka.append(str(tlist[nomer]))
    return sorted(list(set(marka)))

def loading_bz_znaniya_rezina(company):
    sh: Spreadsheet = gspread_open_by_url_with_retry(gspread_url_baza_zn)
    ws_direct = sh.worksheet(company)
    list_of_lists = worksheet_get_all_values_with_retry(ws_direct)[1:]
    return list_of_lists


def load_xab_cache():
    global global_xab_cache
    # Загрузка кэша из Google Таблицы (как у вас уже реализовано)
    sh = gspread_open_by_url_with_retry(gspread_url_peremeshenie)
    ws_direct = sh.worksheet("Онлайн остатки Хаба")
    list_of_lists = worksheet_get_all_values_with_retry(ws_direct)
    groups = {}
    for row in list_of_lists[1:]:
        if len(row) < 14:
            continue
        key = (
            row[2].strip(),  # Марка ТС
            row[3].strip(),  # Радиус
            row[4].strip(),  # Размер
            row[5].strip(),  # Марка резины
            row[6].strip(),  # Модель резины
            row[7].strip(),  # Сезонность
            row[8].strip()   # Тип диска
        )
        groups.setdefault(key, []).append(row)
    global_xab_cache = groups

def get_xab_koles(company, type, user_id):
    # Если пользователь ещё не захватил кэш, захватываем его и помечаем
    global users_with_cache
    if user_id not in users_with_cache:
        acquire_cache()
    result = []
    # Фильтрация кэша по компании и типу (как у вас ранее)
    for key, rows in global_xab_cache.items():
        filtered_rows = [r for r in rows if r[1].strip() == company]
        if not filtered_rows:
            continue
        if type in ["Правое", "Левое"]:
            filtered = [r for r in filtered_rows if r[9].strip() == type]
            if filtered:
                entry = "|".join(key)
                result.append(entry)
        elif type == "Ось":
            wheels = [r[9].strip() for r in filtered_rows]
            if "Левое" in wheels and "Правое" in wheels:
                entry = "|".join(key)
                result.append(entry)
        elif type == "Комплект":
            wheels = [r[9].strip() for r in filtered_rows]
            if wheels.count("Левое") >= 2 and wheels.count("Правое") >= 2:
                entry = "|".join(key)
                result.append(entry)
    result.sort()
    return result


def remove_from_xab_cache(key, removal_dict):
    """
    Из кэша удаляются строки для группы с заданным ключом.
    removal_dict – словарь, в котором для каждого из вариантов ("Левое", "Правое")
    указывается, сколько строк нужно удалить.

    Например:
      - Для type "Левое" или "Правое": removal_dict = {"Левое": 1} или {"Правое": 1}
      - Для type "Ось": removal_dict = {"Левое": 1, "Правое": 1}
      - Для type "Комплект": removal_dict = {"Левое": 2, "Правое": 2}
    """
    global global_xab_cache
    if key not in global_xab_cache:
        return
    rows = global_xab_cache[key]
    # Для каждого требуемого типа удаляем указанное количество строк
    for wheel_type, count in removal_dict.items():
        removed = 0
        new_rows = []
        for r in rows:
            if r[9].strip() == wheel_type and removed < count:
                removed += 1
                # строка удаляется из выборки
                continue
            new_rows.append(r)
        rows = new_rows
    # Если после удаления остаются строки – обновляем группу, иначе удаляем ключ
    if rows:
        global_xab_cache[key] = rows
    else:
        del global_xab_cache[key]

def acquire_cache(user_id=None):
    global global_cache_usage, global_xab_cache, users_with_cache
    if global_xab_cache is None:
        load_xab_cache()
    global_cache_usage += 1
    if user_id is not None:
        users_with_cache[user_id] = time.time()
    return global_xab_cache

def cleanup_stale_cache_users(timeout_seconds=1800):
    global users_with_cache
    now = time.time()
    to_release = [user_id for user_id, timestamp in users_with_cache.items() if now - timestamp > timeout_seconds]
    for user_id in to_release:
        release_cache(user_id)

def release_cache(chat_id):
    global global_cache_usage, global_xab_cache
    if global_cache_usage > 0:
        global_cache_usage -= 1
        users_with_cache.pop(chat_id, None)
    cleanup_cache_if_unused()

def cleanup_cache_if_unused():
    global global_xab_cache, global_cache_usage, users_with_cache
    if global_cache_usage <= 0 and not users_with_cache:
        global_xab_cache = None

@bot.message_handler(commands=['reset_cache'])
def reset_cache(message):
    global global_xab_cache, global_cache_usage, users_with_cache
    # Полный сброс кэша
    global_xab_cache = None
    # Обнуляем счётчик активных захватов
    global_cache_usage = 0
    # Убираем всех пользователей, которые «захватили» кэш
    users_with_cache.clear()
    bot.send_message(
        message.chat.id,
        "✅ Кэш и все связанные с ним данные были успешно сброшены.",
        reply_markup=telebot.types.ReplyKeyboardRemove()
    )
def update_xab_koles(company, record, username, grz_tech):
    """
    Оптимизированная версия:
      - 1x get_all_values() для "Онлайн остатки Хаба" и 1x для "Выгрузка сборка"
      - 1x values_batch_update() для всех обновлений "Выгрузка сборка"
      - 1x batch_update(deleteDimension[]) для пакетного удаления строк из "Онлайн остатки Хаба"
    Возвращает 1 при успехе, 0 при ошибке.
    """
    try:
        gc: Client = gspread.service_account("app/creds.json")
        sh: Spreadsheet = gc.open_by_url(gspread_url_peremeshenie)

        ws_direct = sh.worksheet("Онлайн остатки Хаба")
        ws_upload = sh.worksheet("Выгрузка сборка")

        # ---- Чтения (2 запроса) ----
        direct_data = ws_direct.get_all_values()  # A:.. (заголовок + данные)
        upload_data = ws_upload.get_all_values()

        # Сколько нужно удалить по сторонам
        need_left = int(record[Rc.COUNT_LEFT])
        need_right = int(record[Rc.COUNT_RIGHT])

        # Подготовим быстрый индекс по "Выгрузка сборка":
        # ключ = tuple первых 14 ячеек (с trim), значение = список индексов строк (1-based)
        upload_index = {}
        for row_idx, row in enumerate(upload_data, start=1):
            if len(row) < 14:
                continue
            key = tuple(str(v).strip() for v in row[:14])
            upload_index.setdefault(key, []).append(row_idx)

        # Ключ для сопоставления группы (как в вашей логике):
        # (Марка ТС, Радиус, Размер, Марка резины, Модель резины, Сезонность, Тип диска)
        key_tuple = (
            str(record[Rc.MARKA_TS]).strip(),
            str(record[Rc.RADIUS]).strip(),
            str(record[Rc.RAZMER]).strip(),
            str(record[Rc.MARKA_REZ]).strip(),
            str(record[Rc.MODEL_REZ]).strip(),
            str(record[Rc.SEZON]).strip(),
            str(record[Rc.TIP_DISKA]).strip(),
        )

        # Список кандидатов на удаление из "Онлайн остатки Хаба" (row_index 1-based с учётом заголовков)
        deletion_row_indexes = []  # только индексы строк для deleteDimension
        # Буфер обновлений в "Выгрузка сборка"
        updates = []  # (row_index, [date, username, grz_tech])

        # Текущая дата для записи в upload-лист (как раньше)
        current_date = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")

        # Пробегаем по данным "Онлайн остатки Хаба"; начинаем со 2-й строки (row_index=2), т.к. 1-я — заголовок
        for row_index, row in enumerate(direct_data[1:], start=2):
            if len(row) < 14:
                continue

            # Сопоставление по компании и ключу (позиции в листе: компания=1, далее 2..8 — ключ)
            if (
                str(row[1]).strip() == str(company).strip()
                and (str(row[2]).strip(), str(row[3]).strip(), str(row[4]).strip(),
                     str(row[5]).strip(), str(row[6]).strip(), str(row[7]).strip(),
                     str(row[8]).strip()) == key_tuple
            ):
                pos = str(row[9]).strip()  # "Левое" / "Правое"

                # Отбираем нужное количество строк под удаление
                if pos == "Левое" and need_left > 0:
                    deletion_row_indexes.append(row_index)
                    need_left -= 1
                elif pos == "Правое" and need_right > 0:
                    deletion_row_indexes.append(row_index)
                    need_right -= 1

        # Если нечего удалять — выходим без ошибок (просто не нашли нужные позиции)
        if not deletion_row_indexes:
            logger.debug("update_xab_koles: nothing to delete for %s key=%s", company, key_tuple)
            return 1

        # Готовим пакет обновлений для "Выгрузка сборка"
        # Для сопоставления берём первые 14 ячеек строки из direct_data и ищем идентичную 14-ячейковую подпись в upload_index
        for row_index in deletion_row_indexes:
            row = direct_data[row_index - 1]  # direct_data индекс 0-based
            candidate_key = tuple(str(v).strip() for v in row[:14])
            rows_in_upload = upload_index.get(candidate_key, [])
            if rows_in_upload:
                # Обычно ожидается единственное совпадение; но если есть несколько — обновим все
                for up_idx in rows_in_upload:
                    updates.append(
                        (up_idx, [current_date, str(username), str(grz_tech)])
                    )

        # ---- Выполняем VALUES batch update (1 запрос) ----
        if updates:
            data_payload = []
            for up_idx, values in updates:
                rng = f"'Выгрузка сборка'!O{up_idx}:Q{up_idx}"
                data_payload.append({"range": rng, "values": [values]})
                # retry/backoff
            gs_values_batch_update_with_retry(sh, data_payload)

        # ---- Пакетное удаление строк из "Онлайн остатки Хаба" (1 запрос) ----
        # Удаляем в убывающем порядке индексов
        deletion_row_indexes.sort(reverse=True)
        requests = []
        sheet_id = ws_direct.id  # gspread Worksheet.id — это sheetId

        for row_idx in deletion_row_indexes:
            # startIndex и endIndex 0-based, endIndex не включительно
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_idx - 1,
                        "endIndex": row_idx
                    }
                }
            })

        if requests:
            gs_batch_update_with_retry(sh, requests)

        return 1

    except Exception as e:
        logger.exception("Ошибка в update_xab_koles (bulk): %s", e)
        return 0

def update_xab_koles_bulk(company: str, records: list, username: str, grz_tech: str) -> int:
    """
    Пакетная версия:
      - 1x get_all_values() для "Онлайн остатки Хаба" и 1x для "Выгрузка сборка"
      - 1x values_batch_update() на все попавшие строки "Выгрузка сборка"
      - 1x batch_update(deleteDimension[]) для пакетного удаления многих строк из "Онлайн остатки Хаба"
    records — список позиционных записей вида record[idx], где каждая — ваш tlist с Rc.* полями.
    """
    try:
        gc: Client = gspread.service_account("app/creds.json")
        sh: Spreadsheet = gc.open_by_url(gspread_url_peremeshenie)

        ws_direct = sh.worksheet("Онлайн остатки Хаба")
        ws_upload = sh.worksheet("Выгрузка сборка")

        # ---- единичные чтения (2 запроса) ----
        direct_data = ws_direct.get_all_values()
        upload_data = ws_upload.get_all_values()

        # Индекс по "Выгрузка сборка": ключ = первые 14 колонок, значение = список строк (1-based)
        # Важно: строки могут дублироваться (2 левых / 2 правых), поэтому храним список индексов.
        upload_index: dict[tuple, list[int]] = {}
        for i, row in enumerate(upload_data[1:], start=2):  # start=2 потому что первая строка - заголовок
            key = tuple(str(v).strip() for v in (row[:14] if len(row) >= 14 else row + [""] * (14 - len(row))))
            upload_index.setdefault(key, []).append(i)

        # Соберём "нужно списать" по ключам
        need_map: dict[tuple, dict[str, int]] = {}
        requested_total = 0

        for rec in records:
            key_tuple = (
                str(rec[Rc.MARKA_TS]).strip(),
                str(rec[Rc.RADIUS]).strip(),
                str(rec[Rc.RAZMER]).strip(),
                str(rec[Rc.MARKA_REZ]).strip(),
                str(rec[Rc.MODEL_REZ]).strip(),
                str(rec[Rc.SEZON]).strip(),
                str(rec[Rc.TIP_DISKA]).strip(),
            )
            d = need_map.setdefault(key_tuple, {"Левое": 0, "Правое": 0})

            left_need = int(rec[Rc.COUNT_LEFT]) if str(rec[Rc.COUNT_LEFT]).strip() else 0
            right_need = int(rec[Rc.COUNT_RIGHT]) if str(rec[Rc.COUNT_RIGHT]).strip() else 0

            d["Левое"] += left_need
            d["Правое"] += right_need
            requested_total += left_need + right_need

        # Найдём строки для удаления в "Онлайн остатки Хаба"
        deletion_row_indexes: list[int] = []  # индексы (1-based) в ws_direct
        current_date = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y")
        updates: list[tuple[int, list[str]]] = []  # (row_index_in_upload, [O,Q])

        for row_index, row in enumerate(direct_data[1:], start=2):
            if len(row) < 14:
                continue
            if str(row[1]).strip() != str(company).strip():
                continue

            key_tuple = (
                str(row[2]).strip(), str(row[3]).strip(), str(row[4]).strip(),
                str(row[5]).strip(), str(row[6]).strip(), str(row[7]).strip(),
                str(row[8]).strip()
            )
            need = need_map.get(key_tuple)
            if not need:
                continue

            pos = str(row[9]).strip()  # "Левое"/"Правое"
            if pos not in need:
                continue
            if need[pos] <= 0:
                continue

            # эта строка пойдёт под удаление
            deletion_row_indexes.append(row_index)
            need[pos] -= 1

            # подготовим обновление "Выгрузка сборка" под ту же 14-ячейковую подпись
            # ВАЖНО: обновляем РОВНО одну строку на каждое списанное колесо
            candidate_key = tuple(str(v).strip() for v in row[:14])
            rows_in_upload = upload_index.get(candidate_key, [])
            if not rows_in_upload:
                logger.warning(
                    "update_xab_koles_bulk: не найдена строка в 'Выгрузка сборка' для списания. company=%s key=%s user=%s grz=%s",
                    company, candidate_key, username, grz_tech
                )
                return 0

            up_idx = rows_in_upload.pop(0)  # «погашаем» только одну строку
            updates.append((up_idx, [current_date, str(username), str(grz_tech)]))

        selected_total = len(deletion_row_indexes)
        updated_total = len(updates)

        # 1) В Хабе должно найтись ровно столько колёс, сколько запросили
        if requested_total > 0 and selected_total < requested_total:
            logger.warning(
                "update_xab_koles_bulk: недостаточно позиций в Хабе для списания. company=%s need=%s got=%s user=%s grz=%s",
                company, requested_total, selected_total, username, grz_tech
            )
            return 0

        # 2) На каждое списанное колесо должна быть подготовлена ровно одна отметка
        if selected_total > 0 and updated_total != selected_total:
            logger.warning(
                "update_xab_koles_bulk: несоответствие количества отметок и списаний. company=%s deleted=%s updated=%s user=%s grz=%s",
                company, selected_total, updated_total, username, grz_tech
            )
            return 0

        # ---- batch values update (1 запрос) ----
        if updates:
            data_payload = []
            for up_idx, values in updates:
                rng = f"'Выгрузка сборка'!O{up_idx}:Q{up_idx}"
                data_payload.append({"range": rng, "values": [values]})
            gs_values_batch_update_with_retry(sh, data_payload)

        # ---- пакетное удаление строк из "Хаба" (1 запрос) ----
        if deletion_row_indexes:
            deletion_row_indexes.sort(reverse=True)
            sheet_id = ws_direct.id
            reqs = []
            for row_idx in deletion_row_indexes:
                reqs.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_idx - 1,
                            "endIndex": row_idx
                        }
                    }
                })
            gs_batch_update_with_retry(sh, reqs)

        return 1

    except Exception as e:
        logger.exception("Ошибка в update_xab_koles_bulk: %s", e)
        return 0

def send_or_update_long_message(company, chat_id, text, reply_to_message_id=None):
    max_length = 4096
    # Разбиваем текст на части, не превышающие max_length символов
    parts = []
    if len(text) <= max_length:
        parts = [text]
    else:
        lines = text.split('\n')
        current_chunk = ""
        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_length:
                parts.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += ("\n" if current_chunk else "") + line
        if current_chunk:
            parts.append(current_chunk)

    global last_message_ids
    old_ids = last_message_ids.get(company)
    if not old_ids or old_ids == 0:
        new_ids = []
        for part in parts:
            sent = safe_send_message(chat_id, part, reply_to_message_id=reply_to_message_id)
            new_ids.append(sent.message_id)
        last_message_ids[company] = new_ids[0] if len(new_ids) == 1 else new_ids
        save_message_ids(company, last_message_ids[company])
        return

    if isinstance(old_ids, int):
        old_ids = [old_ids]

    new_ids = []
    if len(parts) == 1 and len(old_ids) > 1:
        last_id = old_ids[-1]
        try:
            safe_edit_message_text(parts[0], chat_id, last_id)
        except ApiTelegramException as e:
            if "message is not modified" in str(e):
                pass
            else:
                logger.warning("edit_message_text failed: %s", e)
                sent = safe_send_message(chat_id, parts[0], reply_to_message_id=reply_to_message_id)
                last_id = sent.message_id

        new_ids = [last_id]
        for extra_id in old_ids[:-1]:
            try:
                _safe_delete_message(chat_id, extra_id)
            except Exception as e:
                logger.exception("Error deleting extra message: %s", e)
    else:
        for i, part in enumerate(parts):
            if i < len(old_ids):
                try:
                    safe_edit_message_text(part, chat_id, old_ids[i])
                    new_ids.append(old_ids[i])
                except ApiTelegramException as e:
                    if "message is not modified" in str(e):
                        new_ids.append(old_ids[i])
                        continue
                    logger.exception("Error editing message part %d: %s", i, e)
                    sent = safe_send_message(chat_id, part, reply_to_message_id=reply_to_message_id)
                    new_ids.append(sent.message_id)
                    try:
                        _safe_delete_message(chat_id, old_ids[i])
                    except Exception as del_e:
                        logger.exception("Error deleting old message for part %d: %s", i, del_e)
            else:
                sent = safe_send_message(chat_id, part, reply_to_message_id=reply_to_message_id)
                new_ids.append(sent.message_id)

        if len(old_ids) > len(parts):
            for msg_id in old_ids[len(parts):]:
                try:
                    _safe_delete_message(chat_id, msg_id)
                except Exception as e:
                    logger.exception("Error deleting extra message: %s", e)

    last_message_ids[company] = new_ids[0] if len(new_ids) == 1 else new_ids
    save_message_ids(company, last_message_ids[company])


def print_google_data(company: str):
    sh = gspread_open_by_url_with_retry(gspread_url_peremeshenie)
    ws_direct = sh.worksheet("Онлайн остатки Хаба")
    direct_data = worksheet_get_all_values_with_retry(ws_direct)

    if len(direct_data) < 2:
        print("No data found in the spreadsheet")
        return

    # Фильтрация строк по компании (колонка 1: Company)
    filtered_rows = [row for row in direct_data[1:] if len(row) >= 10 and row[1].strip() == company]

    # Группируем строки по ключу:
    # Ключ: (Радиус, Размер, Марка резины, Модель резины, Сезонность, Тип диска)
    groups = {}  # { brand: { subgroup_key: {"Левое": count, "Правое": count} } }
    for row in filtered_rows:
        brand = row[2].strip()
        subgroup_key = (
            row[3].strip(),
            row[4].strip(),
            row[5].strip(),
            row[6].strip(),
            row[7].strip(),
            row[8].strip()
        )
        wheel_pos = row[9].strip()  # Ожидается "Левое" или "Правое"

        if brand not in groups:
            groups[brand] = {}
        if subgroup_key not in groups[brand]:
            groups[brand][subgroup_key] = {"Левое": 0, "Правое": 0}
        if wheel_pos in groups[brand][subgroup_key]:
            groups[brand][subgroup_key][wheel_pos] += 1

    # Формируем итоговое сообщение
    output_lines = []
    for brand in sorted(groups.keys()):
        output_lines.append(f"🚗  {brand}")
        for subgroup in sorted(groups[brand].keys()):
            radius, size, tire_brand, tire_model, season, disk_type = subgroup
            counts = groups[brand][subgroup]
            left_count = counts.get("Левое", 0)
            right_count = counts.get("Правое", 0)

            details = []
            kit_count = 0
            while left_count >= 2 and right_count >= 2:
                kit_count += 1
                left_count -= 2
                right_count -= 2
            if kit_count > 0:
                details.append("Комплект " + f"{kit_count}" + "шт")

            axle_count = 0
            while left_count >= 1 and right_count >= 1:
                axle_count += 1
                left_count -= 1
                right_count -= 1
            if axle_count > 0:
                details.append("Ось " + f"{axle_count}" + "шт")

            if left_count > 0:
                details.append("Левое " + f"{left_count}" + "шт")
            if right_count > 0:
                details.append("Правое " + f"{right_count}" + "шт")

            line = (f"🛞 {radius}/{size} | {tire_brand} {tire_model} | {season} | {disk_type} | " +
                    " | ".join(details) + " |")
            output_lines.append(line)

    current_time = (datetime.now() + timedelta(hours=3)).strftime("%H:%M %d.%m.%Y")
    message_text = current_time + "\n\n" + "\n\n".join(output_lines)

    if company == "Яндекс":
        chat_id = chat_id_Yandex
        thread_id = thread_id_Yandex_hab
    elif company == "СитиДрайв":
        chat_id = chat_id_Sity
        thread_id = thread_id_Sity_hab
    else:
        chat_id = chat_id_Belka
        thread_id = thread_id_Belka_hab

    # Отправляем или обновляем сообщение(я)
    send_or_update_long_message(company, chat_id, message_text, reply_to_message_id=thread_id)

@bot.message_handler(commands=['open_gate'])
def open_gate_start(message):
    # Только ЛС
    if int(message.chat.id) < 0:
        bot.reply_to(message, "Эта команда доступна только в личных сообщениях с ботом")
        return

    user = message.from_user
    logger.info("[open_gate] start: @%s (%s)", user.username, user.id)

    # Чистим старую запись пользователя
    global open_gate_records
    open_gate_records = [r for r in open_gate_records if r[Og.USER_ID] != user.id]

    try:
        # 1) ФИО
        resp = requests.get(
            urlSmallDtp,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()
        fio = (rep.get("user") or {}).get("fullname") or "—"

        # 2) Задачи
        resp = requests.get(
            URL_GET_INFO_TASK,
            json={"chat_id": str(user.id)},
            timeout=5,
        )
        rep = resp.json()

        if isinstance(rep, list):
            tasks = rep
        elif isinstance(rep, dict):
            tasks = rep.get("active_tasks") or rep.get("tasks") or []
        else:
            tasks = []

    except Exception as e:
        logger.exception("Ошибка при запросе задач для open_gate: %s", e)
        bot.send_message(message.chat.id, "Не удалось получить информацию по задачам. Попробуйте позже")
        return

    # Если задач нет
    if not tasks:
        if message.chat.id in list_users:
            # тестовый режим
            task = {
                "task_type": "Перегон СШМ",
                "carsharing__name": "Тестовая компания",
                "car_plate": "Т000ТТ000",
                "car_model": "TestCar",
            }
            tasks = [task]
        else:
            bot.send_message(message.chat.id, "У вас нет активной задачи")
            return

    task = tasks[0]
    car_plate = task.get("car_plate") or "—"
    company = task.get("carsharing__name") or "—"
    # car_model = task.get("car_model") or "—"  # пока не используем
    # task_type = task.get("task_type") or "—"

    # Сохраняем данные в глобальный список
    open_gate_records.append([
        user.id,
        user.username,
        fio,
        car_plate,
        company,
    ])

    kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row(telebot.types.KeyboardButton("Подтвердить открытие"))
    kb.row(telebot.types.KeyboardButton("Выход"))

    text = (
        f"ФИО: {fio}\n"
        f"ГРЗ: {car_plate}\n"
        f"Компания: {company}\n\n"
        "Подтвердите открытие ворот склада"
    )

    bot.send_message(message.chat.id, text, reply_markup=kb)
    bot.register_next_step_handler(message, open_gate_confirm)

def open_gate_confirm(message):
    global open_gate_records

    user = message.from_user
    text = (message.text or "").strip().lower()

    # Ищем запись пользователя
    record = None
    for r in open_gate_records:
        if r[Og.USER_ID] == user.id:
            record = r
            break

    if record is None:
        # Нет активной операции — просто уберём клавиатуру и выйдем
        bot.send_message(message.chat.id, "Операция не найдена", reply_markup=telebot.types.ReplyKeyboardRemove())
        return

    if text == "выход":
        bot.send_message(message.chat.id, "Операция отменена", reply_markup=telebot.types.ReplyKeyboardRemove())
        open_gate_records = [r for r in open_gate_records if r[Og.USER_ID] != user.id]
        return

    if text == "подтвердить открытие":
        fio = record[Og.FIO]
        plate = record[Og.CAR_PLATE]
        company = record[Og.COMPANY]

        # логисты
        _teg, _fi = find_logistics_rows()
        if not _fi:
            logist = ""
        else:
            logist = " , ".join(f"{name} ({teg})" for name, teg in zip(_fi, _teg))

        send_text = (
            f"#Открытие_Склада\n\n"
            f"ФИО: {fio}\n"
            f"ГРЗ: {plate}\n"
            f"Компания: {company}\n"
            f"Откройте, пожалуйста, ворота\n{logist}"
        )

        if company == "Яндекс":
            CHAT_ID = chat_id_Yandex
            THREAD_ID = thread_id_Yandex_gates
        else:
            CHAT_ID = chat_id_Sity
            THREAD_ID = thread_id_Sity_gates

        try:
            sent = bot.send_message(
                chat_id=CHAT_ID,
                text=send_text,
                message_thread_id=THREAD_ID
            )
        except Exception as e:
            logger.exception("Ошибка отправки в складской чат: %s", e)
            bot.send_message(
                message.chat.id,
                "Ошибка отправки в складской чат",
                reply_markup=telebot.types.ReplyKeyboardRemove()
            )
            open_gate_records = [r for r in open_gate_records if r[Og.USER_ID] != user.id]
            return

        # ссылка на сообщение в чате склада
        message_link = f"https://t.me/c/{str(CHAT_ID)[4:]}/{THREAD_ID}/{sent.message_id}"

        try:
            write_open_gate_row(
                fio=fio,
                car_plate=plate,
                company=company,
                message_link=message_link
            )
        except Exception as e:
            logger.exception("Ошибка в write_open_gate_row: %s", e)

        bot.send_message(
            message.chat.id,
            f"Сообщение отправлено логисту {logist}",
            reply_markup=telebot.types.ReplyKeyboardRemove()
        )

        open_gate_records = [r for r in open_gate_records if r[Og.USER_ID] != user.id]
        return

    # Любой другой текст — повторяем шаг подтверждения
    bot.send_message(message.chat.id, "Выберите действие: Подтвердить открытие / Выход")
    bot.register_next_step_handler(message, open_gate_confirm)


def get_list_marka_ts(record, Cl):
    if record[Cl.COMPANY] == "СитиДрайв":
        global marka_ts_st
        return marka_ts_st
    elif record[Cl.COMPANY] == "Яндекс":
        global marka_ts_ya
        return marka_ts_ya
    else:
        global marka_ts_blk
        return marka_ts_blk

def get_list_radius(record, Cl):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_radius_bz_znan(lst)


def get_list_radius_bz_znan(bz_znan):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        tlist.append(int(rez[1]))
    return tlist

def get_list_razmer(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_razmer_bz_znan(lst, cur_record)


def get_list_razmer_bz_znan(bz_znan, record):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip():
            tlist.append(str(rez[2]).strip())
    return tlist


def get_list_marka(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_marka_bz_znan(lst, cur_record)


def get_list_marka_bz_znan(bz_znan, record):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip():
            tlist.append(str(rez[4]).strip())
    return tlist


def get_list_model(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_list_model_bz_znan(lst, cur_record)

def get_list_model_bz_znan(bz_znan, record):
    tlist = list()
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip() and str(record[Rc.MARKA_REZ]) == str(rez[4]).strip():
            tlist.append(str(rez[5]).strip())
    return tlist


def get_sezon(record, Cl, cur_record):
    if record[Cl.COMPANY] == "СитиДрайв":
        lst = list_rez_st
    elif record[Cl.COMPANY] == "Яндекс":
        lst = list_rez_ya
    else:
        lst = list_rez_blk
    return get_sezon_bz_znan(lst, cur_record)


def get_sezon_bz_znan(bz_znan, record):
    for i, rez in enumerate(bz_znan):
        if str(record[Rc.RADIUS]) == str(rez[1]).strip() and str(record[Rc.RAZMER]) == str(rez[2]).strip() and str(record[Rc.MARKA_REZ]) == str(rez[4]).strip() and str(record[Rc.MODEL_REZ]) == str(rez[5]).strip():
            return str(rez[3]).strip()

def _telegram_call_with_retry(func, *args, max_retries: int = 3, base_delay: float = 1.0, **kwargs):
    """
    Универсальный ретрай для вызовов Telegram API (send_message, edit_message_text и т.п.).
    Ловим сетевые ConnectionError и пробуем ещё раз.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.ConnectionError as e:
            # Telegram/сеть оборвали соединение
            if attempt == max_retries:
                logger.warning(
                    "Telegram %s failed after %d attempts: %s",
                    func.__name__, attempt, e
                )
                # После всех попыток — пробрасываем наверх (поймается в worker-е)
                raise
            logger.warning(
                "Telegram %s ConnectionError (attempt %d/%d): %s",
                func.__name__, attempt, max_retries, e
            )
            _sleep_with_jitter(base_delay, attempt)
        except ApiTelegramException as e:
            # Здесь можно добавить отдельную логику под 429 и т.п., если захочешь.
            logger.warning("Telegram %s ApiTelegramException: %s", func.__name__, e)
            raise


def safe_send_message(chat_id, text, **kwargs):
    return _telegram_call_with_retry(bot.send_message, chat_id, text, **kwargs)


def safe_edit_message_text(text, chat_id, message_id, **kwargs):
    return _telegram_call_with_retry(bot.edit_message_text, text, chat_id, message_id, **kwargs)


def split_entry(entry: str) -> list:
    return [part.strip() for part in entry.split("|") if part.strip()]

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_messages (
                company TEXT PRIMARY KEY CHECK (company IN ('Яндекс', 'СитиДрайв', 'Белка')),
                message_id TEXT
            )
        """)
        # Вставляем записи для компаний по умолчанию, если их еще нет
        cursor.execute("INSERT OR IGNORE INTO company_messages (company, message_id) VALUES ('Яндекс', NULL)")
        cursor.execute("INSERT OR IGNORE INTO company_messages (company, message_id) VALUES ('СитиДрайв', NULL)")
        cursor.execute("INSERT OR IGNORE INTO company_messages (company, message_id) VALUES ('Белка', NULL)")
        conn.commit()

def save_message_ids(company, message_ids):
    """
    Сохраняем message_ids как JSON-строку.
    Если message_ids – список, сохраняется список, иначе сохраняется список из одного элемента.
    """
    if isinstance(message_ids, list):
        value = json.dumps(message_ids)
    else:
        value = json.dumps([message_ids])
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO company_messages (company, message_id)
            VALUES (?, ?)
            ON CONFLICT(company) DO UPDATE SET message_id = excluded.message_id
        """, (company, value))
        conn.commit()

def load_message_ids():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT company, message_id FROM company_messages")
        data = cursor.fetchall()
    result = {}
    for company, msg_ids in data:
        if msg_ids:
            try:
                parsed = json.loads(msg_ids)
                # Если список состоит из одного элемента – сохраняем как int, иначе как список
                result[company] = parsed[0] if len(parsed) == 1 else parsed
            except Exception as e:
                result[company] = 0
        else:
            result[company] = 0
    return result

def schedule_print_google_data():
    def worker():
        while True:
            try:
                cleanup_stale_cache_users()
                print_google_data("Яндекс")
                print_google_data("СитиДрайв")
                print_google_data("Белка")
            except requests.exceptions.ConnectionError as e:
                logger.warning("Сетевой сбой при вызове print_google_data (будет повтор через 5 минут): %s", e)
            except Exception as e:
                logger.exception("Неожиданная ошибка при вызове print_google_data: %s", e)
            time.sleep(300)  # 5 минут

    threading.Thread(target=worker, daemon=True).start()

def migrate_company_messages_add_belka():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # Проверим, можно ли вставить 'Белка' — если нельзя, значит старый CHECK
        try:
            cur.execute("""
                INSERT INTO company_messages (company, message_id) VALUES ('Белка', NULL)
                ON CONFLICT(company) DO NOTHING
            """)
            conn.commit()
            return  # всё ок, миграция не нужна
        except sqlite3.IntegrityError:
            pass  # нужен пересоздание таблицы

        logger.info("Migrating company_messages to include 'Белка' in CHECK...")
        cur.execute("PRAGMA foreign_keys=OFF;")

        # Переименуем старую таблицу
        cur.execute("ALTER TABLE company_messages RENAME TO company_messages_old;")

        # Создадим новую с корректным CHECK
        cur.execute("""
            CREATE TABLE company_messages (
                company TEXT PRIMARY KEY CHECK (company IN ('Яндекс', 'СитиДрайв', 'Белка')),
                message_id TEXT
            )
        """)

        # Перенесём существующие записи, с маппингом: если вдруг есть мусорные компании — пропустим
        cur.execute("""
            INSERT OR IGNORE INTO company_messages (company, message_id)
            SELECT company, message_id
            FROM company_messages_old
            WHERE company IN ('Яндекс', 'СитиДрайв', 'Белка')
        """)

        # Добьёмся наличия всех трёх компаний
        for comp in ('Яндекс', 'СитиДрайв', 'Белка'):
            cur.execute("""
                INSERT OR IGNORE INTO company_messages (company, message_id)
                VALUES (?, NULL)
            """, (comp,))

        cur.execute("DROP TABLE company_messages_old;")
        conn.commit()
        cur.execute("PRAGMA foreign_keys=ON;")
        logger.info("Migration completed.")

# -----------------------------------------------------------------------------------------------------
def main():
    # Запуск логирования
    log_name = f'logs/{datetime.now().strftime("%Y-%m-%d")}.log'
    Path(log_name).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.WARNING,
        filename=log_name,
        format='%(asctime)s (%(levelname)s): %(message)s (Line: %(lineno)d) [%(filename)s]',
        filemode="a"
    )

    init_db()
    # migrate_company_messages_add_belka()
    # Загружаем сохраненные id сообщений из базы
    db_message_ids = load_message_ids()
    for company in last_message_ids:
        if company in db_message_ids and db_message_ids[company] is not None:
            last_message_ids[company] = db_message_ids[company]

    loading_grz_is_Google_Sheets()

    print_google_data("Яндекс")
    print_google_data("СитиДрайв")
    print_google_data("Белка")

    schedule_print_google_data()

    # Запуск бота
    bot.polling(none_stop=True)

if __name__ == '__main__':
    main()
