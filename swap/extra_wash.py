import logging
import re
import requests
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import ContentType, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from aiogram.utils.markdown import escape_md

from app.config import TOKEN_BOT, CHAT_ID_WASH_MSK, CHAT_ID_WASH_SPB, THREAD_ID_WASH_MSK, THREAD_ID_WASH_SPB
from app.states import ExtraWashStates
from app.utils.gsheets import record_extra_wash_report, load_extra_wash_locations
import app.config as cfg

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

LICENSE_PLATE_PATTERN = re.compile(r"^[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}$", re.IGNORECASE)


# ───────────────────────── вспомогательные функции UI ─────────────────────────

async def _delete_ui_message(state: FSMContext, chat_id: int) -> None:
    """
    Удаляет предыдущее служебное сообщение бота с клавиатурой,
    если его id сохранён в ui_msg_id.
    """
    data = await state.get_data()
    msg_id = data.get("ui_msg_id")
    if not msg_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        # Сообщение уже удалено / изменено — просто игнорируем
        pass


async def _send_step_message(
    state: FSMContext,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: types.ParseMode | None = None,
) -> None:
    """
    Удаляет предыдущее служебное сообщение и отправляет новое,
    сохраняя его id в ui_msg_id.
    """
    await _delete_ui_message(state, chat_id)
    msg = await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    await state.update_data(ui_msg_id=msg.message_id)


# ───────────────────────── служебные клавиатуры ─────────────────────────

def _kb_city() -> InlineKeyboardMarkup:
    """
    Клавиатура выбора города.
    Берём города из кэша, если он заполнен, иначе — дефолт: Москва / Санкт-Петербург.
    """
    kb = InlineKeyboardMarkup(row_width=2)

    cities = sorted(
        set(cfg.EXTRA_WASH_ADDRESSES_BY_CITY.keys()) |
        set(cfg.EXTRA_WASH_SERVICES_BY_CITY.keys())
    )
    if not cities:
        cities = ["Москва", "Санкт-Петербург"]

    buttons = []
    for city in cities:
        buttons.append(InlineKeyboardButton(city, callback_data=f"wash_city:{city}"))
    kb.add(*buttons)
    kb.add(InlineKeyboardButton("❌ Выход", callback_data="wash_exit"))
    return kb


def _kb_service(city: str) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора услуги для конкретного города.
    """
    kb = InlineKeyboardMarkup(row_width=2)

    services = cfg.EXTRA_WASH_SERVICES_BY_CITY.get(city)
    if not services:
        # На случай, если в таблице нет записей — оставим минимальный набор.
        services = ["Комплексная", "Кузов"]

    for idx, name in enumerate(services):
        kb.insert(InlineKeyboardButton(name, callback_data=f"wash_service:{idx}"))

    kb.row(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_city"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit"),
    )
    return kb


def _kb_plate() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_service"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit"),
    )
    return kb


# ───────────────────────── команды ─────────────────────────

async def cmd_update_address(message: types.Message):
    await message.answer("🔄 Обновляем базу адресов и услуг")
    try:
        load_extra_wash_locations()
        await message.answer("✅ База адресов и услуг обновлена")
    except Exception as e:
        logger.exception("Failed to update wash locations/services")
        await message.answer("❌ Не удалось обновить базу адресов и услуг")


async def cmd_fill_wash(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return await message.answer("Эта команда доступна только в личных сообщениях с ботом")

    await state.finish()
    await state.update_data(user_id=message.from_user.id, username=message.from_user.username)

    # Можно на всякий случай подтянуть актуальные данные (быстро).
    load_extra_wash_locations()

    # Первый шаг — выбор города
    await _send_step_message(
        state,
        message.chat.id,
        "🏙 Выберите город:",
        reply_markup=_kb_city(),
    )
    await ExtraWashStates.WAIT_CITY.set()


# ───────────────────────── общий summary ─────────────────────────

async def send_summary(message: types.Message, data: dict, state: FSMContext):
    summary = (
        f"🏙 Город: {data.get('city', '—')}\n"
        f"🧽 Услуга: {data.get('service', 'Не указана')}\n"
        f"🚘 ГРЗ: {data.get('plate', '—')}\n"
        f"🔎 Мойка: {data.get('address', '—')}\n"
        f"📎 Тикет: {data.get('ticket', '—')}\n"
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Все ок", callback_data="wash_confirm"),
        InlineKeyboardButton("✏️ Есть ошибка", callback_data="wash_error"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        message.chat.id,
        escape_md(summary),
        reply_markup=kb,
        parse_mode=types.ParseMode.MARKDOWN_V2,
    )


# ───────────────────────── выбор города ─────────────────────────

async def on_city_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    city = cb.data.split(":", 1)[1]

    await state.update_data(city=city, service=None, address=None)

    # Переходим к выбору услуги для выбранного города
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Выберите услугу мойки",
        reply_markup=_kb_service(city),
    )
    await ExtraWashStates.WAIT_SERVICE.set()


async def on_city_back(cb: types.CallbackQuery, state: FSMContext):
    """
    Возврат с этапа выбора услуги к выбору города.
    """
    await cb.answer()
    await state.update_data(city=None, service=None)
    await _send_step_message(
        state,
        cb.message.chat.id,
        "🏙 Выберите город:",
        reply_markup=_kb_city(),
    )
    await ExtraWashStates.WAIT_CITY.set()


# ───────────────────────── выбор услуги ─────────────────────────

async def on_service_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    idx_str = cb.data.split(":", 1)[1]
    data = await state.get_data()
    city = data.get("city")

    services = cfg.EXTRA_WASH_SERVICES_BY_CITY.get(city)
    if not services:
        services = ["Комплексная", "Кузов"]

    try:
        idx = int(idx_str)
        service = services[idx]
    except (ValueError, IndexError):
        service = services[0]

    editing = data.get("editing_field")

    await state.update_data(service=service)

    # Если мы в режиме редактирования услуги — возвращаемся к summary
    if editing == "service":
        await state.update_data(editing_field=None)
        new_data = await state.get_data()
        await send_summary(cb.message, new_data, state)
        await ExtraWashStates.WAIT_CONFIRMATION.set()
        return

    # Обычный сценарий — переходим к вводу ГРЗ
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Введите ГРЗ",
        reply_markup=_kb_plate(),
    )
    await ExtraWashStates.WAIT_PLATE.set()


async def on_service_back(cb: types.CallbackQuery, state: FSMContext):
    """Возврат с этапа ввода ГРЗ к выбору услуги мойки"""
    await cb.answer()
    data = await state.get_data()
    city = data.get("city")
    await state.update_data(service=None)
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Выберите услугу мойки",
        reply_markup=_kb_service(city or "Москва"),
    )
    await ExtraWashStates.WAIT_SERVICE.set()


# ───────────────────────── ввод ГРЗ ─────────────────────────

async def on_plate_entered(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()
    data = await state.get_data()
    editing = data.get('editing_field')

    if not LICENSE_PLATE_PATTERN.match(text):
        return await message.answer(
            escape_md("Неверный формат ГРЗ. Попробуйте снова (например: А001АА777)"),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )

    await state.update_data(plate=text.upper())

    if editing == 'plate':
        await state.update_data(editing_field=None)
        new_data = await state.get_data()
        await send_summary(message, new_data, state)
        await ExtraWashStates.WAIT_CONFIRMATION.set()
        return

    city = data.get("city")
    addresses_by_city = cfg.EXTRA_WASH_ADDRESSES_BY_CITY or {}
    locations = addresses_by_city.get(city, [])

    if not locations:
        return await message.answer(
            f"Для города *{escape_md(city or '—')}* не найдено адресов мойки. "
            f"Обратитесь к администратору.",
            parse_mode=types.ParseMode.MARKDOWN_V2
        )

    await state.update_data(locations=locations)
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, addr in enumerate(locations):
        kb.insert(InlineKeyboardButton(addr, callback_data=f"wash_loc:{idx}"))
    kb.row(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_plate"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        message.chat.id,
        "Выберите мойку",
        reply_markup=kb,
    )
    await ExtraWashStates.WAIT_LOCATION.set()


async def on_plate_back(cb: types.CallbackQuery, state: FSMContext):
    """Возврат к вводу ГРЗ из выбора мойки"""
    await cb.answer()
    await state.update_data(plate=None)
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Введите ГРЗ",
        reply_markup=_kb_plate(),
    )
    await ExtraWashStates.WAIT_PLATE.set()


# ───────────────────────── выбор мойки ─────────────────────────

async def on_loc_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    idx = int(cb.data.split(':', 1)[1])
    address = data['locations'][idx]
    await state.update_data(address=address)
    editing = data.get('editing_field')

    if editing == 'location':
        await state.update_data(editing_field=None)
        new_data = await state.get_data()
        await send_summary(cb.message, new_data, state)
        await ExtraWashStates.WAIT_CONFIRMATION.set()
        return

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_loc"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Прикрепите ссылку тикета",
        reply_markup=kb,
    )
    await ExtraWashStates.WAIT_TICKET.set()


async def on_loc_back(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    locations = data.get('locations', [])
    kb = InlineKeyboardMarkup(row_width=2)
    for i, addr in enumerate(locations):
        kb.insert(InlineKeyboardButton(addr, callback_data=f"wash_loc:{i}"))
    kb.row(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_plate"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Выберите мойку",
        reply_markup=kb,
    )
    await ExtraWashStates.WAIT_LOCATION.set()


# ───────────────────────── тикет ─────────────────────────

async def on_ticket_entered(message: types.Message, state: FSMContext):
    ticket = message.text.strip()
    data = await state.get_data()
    editing = data.get('editing_field')
    await state.update_data(ticket=ticket)

    if editing == 'ticket':
        await state.update_data(editing_field=None)
        new_data = await state.get_data()
        await send_summary(message, new_data, state)
        await ExtraWashStates.WAIT_CONFIRMATION.set()
        return

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_ticket"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        message.chat.id,
        escape_md("Прикрепите 4 фото после Мойки\n(Где виден ГРЗ)"),
        reply_markup=kb,
        parse_mode=types.ParseMode.MARKDOWN_V2,
    )
    await ExtraWashStates.WAIT_PHOTOS.set()


async def on_ticket_back(cb: types.CallbackQuery, state: FSMContext):
    """Возврат к выбору мойки из этапа ввода тикета"""
    await cb.answer()
    data = await state.get_data()
    locations = data.get('locations', [])
    kb = InlineKeyboardMarkup(row_width=2)
    for i, addr in enumerate(locations):
        kb.insert(InlineKeyboardButton(addr, callback_data=f"wash_loc:{i}"))
    kb.row(
        InlineKeyboardButton("⏪ Назад", callback_data="wash_back_plate"),
        InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
    )
    await _send_step_message(
        state,
        cb.message.chat.id,
        "Выберите мойку",
        reply_markup=kb,
    )
    await ExtraWashStates.WAIT_LOCATION.set()


# ───────────────────────── фото ─────────────────────────

async def on_media_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files: list[str] = data.get('photos', [])
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and message.document.mime_type and message.document.mime_type.startswith('image/'):
        file_id = message.document.file_id
    else:
        return await message.answer("Пожалуйста, пришлите именно изображение — фото или картинку")

    files.append(file_id)
    await state.update_data(photos=files)

    # Требуем ровно 4 фото
    if len(files) != 4:
        return

    new_data = await state.get_data()
    await send_summary(message, new_data, state)
    await ExtraWashStates.WAIT_CONFIRMATION.set()


# ───────────────────────── обработка ошибок / редактирование ─────────────────────────

async def on_error_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔹 Услуга", callback_data="error_edit:service"),
        InlineKeyboardButton("🔹 ГРЗ", callback_data="error_edit:plate"),
        InlineKeyboardButton("🔹 Адрес", callback_data="error_edit:location"),
        InlineKeyboardButton("🔹 Тикет", callback_data="error_edit:ticket"),
        InlineKeyboardButton("🔹 Фото", callback_data="error_edit:photos")
    )
    kb.add(InlineKeyboardButton("❌ Выход", callback_data="wash_exit"))
    await _send_step_message(
        state,
        cb.message.chat.id,
        "✏️ Изменить:",
        reply_markup=kb,
    )


async def on_error_field_selected(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    field = cb.data.split(':', 1)[1]
    data = await state.get_data()
    await state.update_data(editing_field=field)

    if field == 'service':
        await state.update_data(service=None)
        city = data.get("city")
        await _send_step_message(
            state,
            cb.message.chat.id,
            "Выберите услугу мойки",
            reply_markup=_kb_service(city or "Москва"),
        )
        await ExtraWashStates.WAIT_SERVICE.set()

    elif field == 'plate':
        await state.update_data(plate=None)
        await _send_step_message(
            state,
            cb.message.chat.id,
            "Введите ГРЗ",
            reply_markup=_kb_plate(),
        )
        await ExtraWashStates.WAIT_PLATE.set()

    elif field == 'location':
        await state.update_data(address=None)
        locations = data.get('locations')
        await state.update_data(locations=locations)
        kb = InlineKeyboardMarkup(row_width=1)
        for idx, addr in enumerate(locations):
            kb.insert(InlineKeyboardButton(addr, callback_data=f"wash_loc:{idx}"))
        kb.row(
            InlineKeyboardButton("⏪ Назад", callback_data="wash_back_plate"),
            InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
        )
        await _send_step_message(
            state,
            cb.message.chat.id,
            "Выберите мойку",
            reply_markup=kb,
        )
        await ExtraWashStates.WAIT_LOCATION.set()

    elif field == 'ticket':
        await state.update_data(ticket=None)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("⏪ Назад", callback_data="wash_back_loc"),
            InlineKeyboardButton("❌ Выход", callback_data="wash_exit")
        )
        await _send_step_message(
            state,
            cb.message.chat.id,
            "Прикрепите ссылку тикета",
            reply_markup=kb,
        )
        await ExtraWashStates.WAIT_TICKET.set()

    else:  # photos
        await state.update_data(photos=[])
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(InlineKeyboardButton("❌ Выход", callback_data="wash_exit"))
        await _send_step_message(
            state,
            cb.message.chat.id,
            escape_md("Прикрепите 4 фото после Мойки\n(Где виден ГРЗ)"),
            reply_markup=kb,
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )
        await ExtraWashStates.WAIT_PHOTOS.set()


# ───────────────────────── подтверждение / выход ─────────────────────────

async def on_wash_confirm(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()

    # удаляем последнюю summary-карточку с клавиатурой
    await _delete_ui_message(state, cb.message.chat.id)

    time_str = (datetime.now() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M:%S")
    caption = (
        f"#Срочная_мойка\n\n"
        f"🕕 Время: {time_str}\n"
        f"🏙 Город: {data.get('city', '—')}\n"
        f"🧽 Услуга: {data.get('service', 'Не указана')}\n"
        f"🚘 ГРЗ: {data['plate']}\n"
        f"🔎 Мойка: {data['address']}\n"
        f"📎 Тикет: {data['ticket']}\n"
        f"@{data['username']}\n\n"
    )
    media = [
        InputMediaPhoto(
            media=data['photos'][0],
            caption=escape_md(caption),
            parse_mode=types.ParseMode.MARKDOWN_V2
        )
    ]
    if data.get('city') == "Москва":
        CHAT_ID_WASH = CHAT_ID_WASH_MSK
        THREAD_ID_WASH = THREAD_ID_WASH_MSK
    else:
        CHAT_ID_WASH = CHAT_ID_WASH_SPB
        THREAD_ID_WASH = THREAD_ID_WASH_SPB
    for fid in data['photos'][1:4]:
        media.append(InputMediaPhoto(media=fid))
    messages = await bot.send_media_group(chat_id=CHAT_ID_WASH, media=media, message_thread_id=THREAD_ID_WASH)
    first_msg = messages[0]
    if first_msg.chat.username:
        link = f"https://t.me/{first_msg.chat.username}/{first_msg.message_id}"
    else:
        link = f"https://t.me/c/{str(first_msg.chat.id)[4:]}/{first_msg.message_id}"
    record_extra_wash_report(
        username=data['username'],
        service=data.get('service', 'Не указана'),
        plate=data['plate'],
        address=data['address'],
        ticket=data['ticket'],
        message_link=link,
        sity=data.get('city', '—')
    )
    await cb.message.answer(escape_md("Отчет отправлен"), parse_mode=types.ParseMode.MARKDOWN_V2)
    await state.finish()


async def on_wash_exit(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()

    # удаляем последнюю служебную карточку
    await _delete_ui_message(state, cb.message.chat.id)

    await cb.message.answer("Оформление завершено")
    await state.finish()


# ───────────────────────── регистрация хендлеров ─────────────────────────

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_fill_wash, commands=["extra_wash"], state="*")

    # город
    dp.register_callback_query_handler(on_city_selected, lambda cb: cb.data.startswith("wash_city:"), state=ExtraWashStates.WAIT_CITY)
    dp.register_callback_query_handler(on_city_back,    lambda cb: cb.data == "wash_back_city",    state=ExtraWashStates.WAIT_SERVICE)

    # выбор услуги
    dp.register_callback_query_handler(on_service_selected, lambda cb: cb.data.startswith("wash_service:"), state=ExtraWashStates.WAIT_SERVICE)
    dp.register_callback_query_handler(on_service_back,     lambda cb: cb.data == "wash_back_service",      state=ExtraWashStates.WAIT_PLATE)

    # ГРЗ
    dp.register_message_handler(on_plate_entered, state=ExtraWashStates.WAIT_PLATE)
    dp.register_callback_query_handler(on_plate_back, lambda cb: cb.data == 'wash_back_plate', state=ExtraWashStates.WAIT_LOCATION)

    # Адрес мойки
    dp.register_callback_query_handler(on_loc_selected, lambda cb: cb.data.startswith('wash_loc:'), state=ExtraWashStates.WAIT_LOCATION)
    dp.register_callback_query_handler(on_loc_back,     lambda cb: cb.data == 'wash_back_loc',      state=ExtraWashStates.WAIT_TICKET)

    # Тикет
    dp.register_message_handler(on_ticket_entered, state=ExtraWashStates.WAIT_TICKET)
    dp.register_callback_query_handler(on_ticket_back, lambda cb: cb.data == 'wash_back_ticket', state=ExtraWashStates.WAIT_PHOTOS)

    # Фото
    dp.register_message_handler(
        on_media_received,
        content_types=[ContentType.PHOTO, ContentType.DOCUMENT],
        state=ExtraWashStates.WAIT_PHOTOS
    )

    # Ошибки / редактирование
    dp.register_callback_query_handler(on_error_selected,       lambda cb: cb.data == 'wash_error',          state=ExtraWashStates.WAIT_CONFIRMATION)
    dp.register_callback_query_handler(on_error_field_selected, lambda cb: cb.data.startswith('error_edit:'), state=ExtraWashStates.WAIT_CONFIRMATION)

    # Подтверждение / выход
    dp.register_callback_query_handler(on_wash_confirm, lambda cb: cb.data == 'wash_confirm', state=ExtraWashStates.WAIT_CONFIRMATION)
    dp.register_callback_query_handler(on_wash_exit,    lambda cb: cb.data == 'wash_exit',    state="*")

    # Обновление адресов/услуг
    dp.register_message_handler(cmd_update_address, commands=["update_address"], state="*")
