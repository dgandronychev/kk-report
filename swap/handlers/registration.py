import logging
import re
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md

from app.states import RegistrationStates
from app.config import URL_REGISTRASHION, URL_TASK_FIO, TOKEN_BOT, ID_OKRUGA
from app.utils.script import shorten_name
from app.utils.gsheets import input_registration

# Список округов и соответствующие аббревиатуры
okruga = [
    "Центральный",
    "Северный",
    "Северо-Восточный",
    "Восточный",
    "Юго-Восточный",
    "Южный",
    "Юго-Западный",
    "Западный",
    "Северо-Западный",
    "Зеленоградский",
    "Новомосковский",
    "Троицкий"
]
abbr_map = dict(zip(
    okruga,
    ["ЦАО", "САО", "СВАО", "ВАО",
     "ЮВАО", "ЮАО", "ЮЗАО", "ЗАО",
     "СЗАО", "ЗелАО", "НАО", "ТАО"]
))

bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)
logger = logging.getLogger(__name__)

MAP_FILE_ID = None

async def catch_map_photo(message: types.Message):
    global MAP_FILE_ID
    if not message.photo:
        return
    MAP_FILE_ID = message.photo[-1].file_id
    print(MAP_FILE_ID)
    await message.answer("✅ File_id карты сохранён. Теперь бот умеет его использовать.")
async def cmd_start(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer(
            escape_md("Команда доступна только в ЛС"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
        return
    await message.answer(
        escape_md(
            "Для начала работы в телеграм-боте CleanCar пройдите регистрацию по команде /registration"
        ),
        parse_mode=types.ParseMode.MARKDOWN_V2
    )

async def cmd_registration(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer(
            escape_md("Команда доступна только в ЛС"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
        return

    await state.finish()
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(types.KeyboardButton(text="📱 Поделиться контактом", request_contact=True))
    kb.add(types.KeyboardButton(text="❌ Выход"))

    await message.answer(
        escape_md(
            "Убедитесь, что в приложение КЛИНКАР указан номер телефона, который привязан к аккаунту Телеграм в формате 7**********.\n"
            "Нажмите на кнопку 'Поделиться контактом':"
        ),
        reply_markup=kb
    )
    await RegistrationStates.WAIT_PHONE.set()

async def process_contact(message: types.Message, state: FSMContext):
    contact = message.contact
    if message.from_user.id != contact.user_id:
        await message.answer(
            escape_md("Отправьте свой собственный номер телефона"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
        return

    phone_number = re.sub("[^0-9]", "", contact.phone_number)
    username = message.from_user.username or "не указан"

    json_data = {
        "phone": phone_number,
        "tg_username": username,
        "tg_chat_id": str(message.from_user.id)
    }
    try:
        resp = requests.post(URL_REGISTRASHION, json=json_data)
        resp.raise_for_status()
    except Exception as e:
        logger.exception(e)
        await message.answer(
            escape_md("❌ Произошла ошибка при регистрации. Обратитесь к разработчикам"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
        return await state.finish()


    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for okrug in okruga:
        kb.add(types.KeyboardButton(text=okrug))
    kb.add(types.KeyboardButton(text="❌ Выход"))

    # Если file_id сохранен — используем его, иначе попросим админа сохранить
    if ID_OKRUGA:
        await message.answer_photo(
            photo=ID_OKRUGA,
            caption="📍 Выберите ваш административный округ из списка ниже:",
            reply_markup=kb
        )
    else:
        await message.answer(
            "📍 Выберите ваш административный округ из списка ниже:",
            reply_markup=kb
        )
    await RegistrationStates.WAIT_ADRESS.set()

async def input_adrees(message: types.Message, state: FSMContext):
    address = message.text.strip()
    if address == "❌ Выход":
        return await cancel_registration(message, state)

    if address not in abbr_map:
        await message.answer(
            "❗ Пожалуйста, выберите округ из списка кнопок ниже."
        )
        return

    short_code = abbr_map[address]

    # Получаем ФИО
    try:
        r = requests.get(URL_TASK_FIO, data={"chat_id": str(message.from_user.id)})
        r.raise_for_status()
        user_info = r.json().get("user", {})
        fullname = user_info.get("fullname", "БезФИО")
        fio = fullname
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка при получении ФИО. Обратитесь к разработчикам")
        return await state.finish()

    # Запись в Google Sheets
    input_registration(fio, message.from_user.username, short_code)

    await message.answer(escape_md("✅ Регистрация завершена!"), reply_markup=types.ReplyKeyboardRemove())
    await state.finish()

async def cancel_registration(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        escape_md("❌ Регистрация отменена"),
        parse_mode=types.ParseMode.MARKDOWN_V2,
        reply_markup=types.ReplyKeyboardRemove()
    )


def register_handlers(dp: Dispatcher):
    # dp.register_message_handler(catch_map_photo, content_types=["photo"], state="*")
    dp.register_message_handler(cmd_start, commands=["start"], state="*")
    dp.register_message_handler(cmd_registration, commands=["registration"], state="*")
    dp.register_message_handler(process_contact, state=RegistrationStates.WAIT_PHONE, content_types=["contact"])
    dp.register_message_handler(cancel_registration, state=RegistrationStates.WAIT_PHONE, text="❌ Выход")
    dp.register_message_handler(input_adrees, state=RegistrationStates.WAIT_ADRESS)
