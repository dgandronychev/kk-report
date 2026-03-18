import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import MessageToDeleteNotFound
from typing import Union
from app.utils.script import resolve_event


from app.config import (
    TOKEN_BOT,
    CHAT_ID_ARRIVAL,
    THREAD_MESSAGE_SKLAD_SITY,
    THREAD_MESSAGE_SKLAD_YNDX,
)
from app.utils.gsheets import load_data_rez_disk, write_row_to_sheet
import app.config as cfg
from app.states import OrderWheelStates

bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.HTML)
logger = logging.getLogger(__name__)

# ---- КОМАНДА ОБНОВЛЕНИЯ БАЗЫ ----
async def cmd_update_baza_data(event: Union[types.Message, types.CallbackQuery]):
    message, actor = resolve_event(event)
    if getattr(message.chat, "type", None) != "private":
        await message.answer("Команда доступна можно только в ЛС")
        return

    username = getattr(actor, "username", None) or getattr(message.chat, "username", None) or ""
    tag = f"@{username}" if username else ""
    logger.info("[update_baza_data] start: %s chat_id=%s", tag or "<no_username>", message.chat.id)

    await message.answer("🔄 Обновляем базу данных по складу")
    try:
        load_data_rez_disk()
        await message.answer("✅ База обновлена")
    except Exception:
        logger.exception("Failed to update baza date")
        await message.answer("❌ Не удалось обновить базу данных")

# ---- СТАРТ ЗАКАЗА ----
async def cmd_order_wheel(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    message, actor = resolve_event(event)
    if getattr(message.chat, "type", None) != "private":
        await message.answer("Команда доступна можно только в ЛС")
        return

    await state.finish()

    username = getattr(actor, "username", None) or getattr(message.chat, "username", None) or ""
    tag = f"@{username}" if username else ""
    logger.info("[order_wheel] start: %s chat_id=%s", tag or "<no_username>", message.chat.id)
    await state.finish()
    await state.update_data(items=[])
    kb = InlineKeyboardMarkup(row_width=2)
    kb.insert(InlineKeyboardButton("СитиДрайв", callback_data="order_company:city"))
    kb.insert(InlineKeyboardButton("ЯДрайв",   callback_data="order_company:yd"))
    kb.row(
        InlineKeyboardButton("Выход", callback_data="order_exit")
    )
    await message.answer("Выберите компанию:", reply_markup=kb)
    await OrderWheelStates.WAIT_COMPANY.set()

# ---- ВЫБОР КОМПАНИИ ----
async def choose_company(cb: types.CallbackQuery, state: FSMContext):
    _, comp = cb.data.split(":", 1)
    await state.update_data(company=comp)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=2)
    kb.insert(InlineKeyboardButton("Резина", callback_data="order_type:res"))
    kb.insert(InlineKeyboardButton("Диск",   callback_data="order_type:disk"))
    kb.row(
        InlineKeyboardButton("Назад", callback_data="order_back"),
        InlineKeyboardButton("Выход", callback_data="order_exit"),
    )
    await cb.message.answer("Выберите тип товара:", reply_markup=kb)
    await OrderWheelStates.WAIT_TYPE.set()

# ---- ВЫБОР ТИПА ----
async def choose_type(cb: types.CallbackQuery, state: FSMContext):
    _, t = cb.data.split(":", 1)
    data = await state.get_data()
    comp = data["company"]
    if comp == "city" and t == "disk": catalog = cfg.BAZA_DISK_SITY
    if comp == "city" and t == "res":  catalog = cfg.BAZA_REZN_SITY
    if comp == "yd"   and t == "disk": catalog = cfg.BAZA_DISK_YNDX
    if comp == "yd"   and t == "res":  catalog = cfg.BAZA_REZN_YNDX
    key_size = "Размер резины" if t == "disk" else "Размерность"
    sizes = sorted({ item[key_size] for item in catalog if item.get(key_size) })
    await state.update_data(type=t, catalog=catalog, sizes=sizes)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=1)
    for sz in sizes:
        kb.insert(InlineKeyboardButton(sz, callback_data=f"order_size:{sz}"))
    kb.row(
        InlineKeyboardButton("Назад", callback_data="order_back"),
        InlineKeyboardButton("Выход", callback_data="order_exit"),
    )
    await cb.message.answer("Выберите размерность:", reply_markup=kb)
    await OrderWheelStates.WAIT_SIZE.set()

# ---- ВЫБОР РАЗМЕРА ----
async def choose_size(cb: types.CallbackQuery, state: FSMContext):
    _, sz = cb.data.split(":", 1)
    data = await state.get_data()
    catalog, t = data["catalog"], data["type"]
    name_key = "наименование" if t == "disk" else "Наименование"
    names = sorted({
        item[name_key]
        for item in catalog
        if item.get(name_key)
        and (item["Размер резины"] if t == "disk" else item["Размерность"]) == sz
    })
    await state.update_data(size=sz, names=names)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=1)
    for nm in names:
        kb.insert(InlineKeyboardButton(nm, callback_data=f"order_name:{nm}"))
    kb.row(
        InlineKeyboardButton("Назад", callback_data="order_back"),
        InlineKeyboardButton("Выход", callback_data="order_exit"),
    )
    await cb.message.answer("Выберите наименование:", reply_markup=kb)
    await OrderWheelStates.WAIT_NAME.set()

# ---- ВЫБОР НАИМЕНОВАНИЯ ----
async def choose_name(cb: types.CallbackQuery, state: FSMContext):
    _, nm = cb.data.split(":", 1)
    data = await state.get_data()
    catalog, t, size = data["catalog"], data["type"], data["size"]
    nm_key = "наименование" if t == "disk" else "Наименование"
    model_key = "Модель авто" if any("Модель авто" in it for it in catalog) else "Модель"
    models = sorted({
        item[model_key]
        for item in catalog
        if item.get(model_key)
        and item[nm_key] == nm
        and (item["Размер резины"] if t == "disk" else item["Размерность"]) == size
    })
    await state.update_data(name=nm, models=models)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=1)
    for m in models:
        kb.insert(InlineKeyboardButton(m, callback_data=f"order_model:{m}"))
    kb.row(
        InlineKeyboardButton("Назад", callback_data="order_back"),
        InlineKeyboardButton("Выход", callback_data="order_exit"),
    )
    await cb.message.answer("Выберите модель:", reply_markup=kb)
    await OrderWheelStates.WAIT_MODEL.set()

# ---- ВЫБОР МОДЕЛИ ----
async def choose_model(cb: types.CallbackQuery, state: FSMContext):
    _, m = cb.data.split(":", 1)
    data = await state.get_data()
    catalog, t, size, nm = data["catalog"], data["type"], data["size"], data["name"]
    model_key = "Модель авто" if "Модель авто" in catalog[0] else "Модель"
    nm_key = "наименование" if t == "disk" else "Наименование"
    rec = next(item for item in catalog
               if item.get(model_key) == m
               and item[nm_key] == nm
               and (item["Размер резины"] if t == "disk" else item["Размерность"]) == size)
    avail = int(rec.get("Текущий остаток", rec.get("остаток", 0)))
    await state.update_data(model=m, available=avail)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=2)
    kb.insert(InlineKeyboardButton("Назад", callback_data="order_back"))
    kb.insert(InlineKeyboardButton("Выход", callback_data="order_exit"))
    await cb.message.answer(f"Введите количество (доступно: {avail}):", reply_markup=kb)
    await OrderWheelStates.WAIT_QUANTITY.set()

# ---- ВВОД КОЛИЧЕСТВА ----
async def enter_quantity(msg: types.Message, state: FSMContext):
    if not msg.text.isdigit():
        return await msg.answer("Введите число")
    qty = int(msg.text)
    data = await state.get_data()
    if qty > data["available"]:
        return await msg.answer(f"Нельзя больше {data['available']}")
    await state.update_data(quantity=qty)
    try:
        await msg.delete()
    except MessageToDeleteNotFound:
        pass
    kb = InlineKeyboardMarkup(row_width=2)
    kb.insert(InlineKeyboardButton("Да", callback_data="order_assembled:yes"))
    kb.insert(InlineKeyboardButton("Нет, замена...", callback_data="order_assembled:no"))
    kb.row(
        InlineKeyboardButton("Назад", callback_data="order_back"),
        InlineKeyboardButton("Выход", callback_data="order_exit"),
    )
    await msg.answer("Собрано по заявке?", reply_markup=kb)
    await OrderWheelStates.WAIT_ASSEMBLY.set()

# ---- ЗАВЕРШЕНИЕ ЗАКАЗА ----
async def finish_order(cb: types.CallbackQuery, state: FSMContext):
    _, assembled = cb.data.split(":", 1)
    data = await state.get_data()
    row = [
        datetime.now().strftime("%d.%м.%Y %H:%M"),  # исправить формат если нужно
        data["company"], data["type"], data["size"],
        data["name"], data["model"], data["quantity"], assembled
    ]
    # write_row_to_sheet("OrderWheel", row)
    text = (
        f"#Заказ_{'диска' if data['type']=='disk' else 'резины'}:\n\n"
        f"— Размерность: {data['size']}\n"
        f"— Наименование: {data['name']}\n"
        f"— Модель: {data['model']}\n"
        f"— Количество: {data['quantity']}\n"
        f"Собрано: {'Да' if assembled=='yes' else 'Нет, нужна замена'}"
    )
    thread_id = THREAD_MESSAGE_SKLAD_SITY if data['company'] =='city' else THREAD_MESSAGE_SKLAD_YNDX
    await bot.send_message(chat_id=CHAT_ID_ARRIVAL, text=text, message_thread_id=thread_id)
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    await cb.message.answer(text)
    await state.finish()

# ---- ОБРАБОТЧИК "НАЗАД" ----
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    data = await state.get_data()
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    if current_state == OrderWheelStates.WAIT_TYPE.state:
        return await cmd_order_wheel(cb.message, state)
    if current_state == OrderWheelStates.WAIT_SIZE.state:
        fake_cb = types.CallbackQuery(id=cb.id, from_user=cb.from_user, chat_instance=cb.chat_instance,
                                     data=f"order_company:{data.get('company')}", message=cb.message)
        return await choose_company(fake_cb, state)
    if current_state == OrderWheelStates.WAIT_NAME.state:
        fake_cb = types.CallbackQuery(id=cb.id, from_user=cb.from_user, chat_instance=cb.chat_instance,
                                     data=f"order_type:{data.get('type')}", message=cb.message)
        return await choose_type(fake_cb, state)
    if current_state == OrderWheelStates.WAIT_MODEL.state:
        fake_cb = types.CallbackQuery(id=cb.id, from_user=cb.from_user, chat_instance=cb.chat_instance,
                                     data=f"order_size:{data.get('size')}", message=cb.message)
        return await choose_size(fake_cb, state)
    if current_state == OrderWheelStates.WAIT_QUANTITY.state:
        fake_cb = types.CallbackQuery(id=cb.id, from_user=cb.from_user, chat_instance=cb.chat_instance,
                                     data=f"order_name:{data.get('name')}", message=cb.message)
        return await choose_name(fake_cb, state)
    if current_state == OrderWheelStates.WAIT_ASSEMBLY.state:
        fake_cb = types.CallbackQuery(id=cb.id, from_user=cb.from_user, chat_instance=cb.chat_instance,
                                     data=f"order_model:{data.get('model')}", message=cb.message)
        return await choose_model(fake_cb, state)
    return await exit_order(cb, state)

# ---- ОБРАБОТЧИК "ВЫХОД" ----
async def exit_order(cb: types.CallbackQuery, state: FSMContext):
    try:
        await cb.message.delete()
    except MessageToDeleteNotFound:
        pass
    await state.finish()
    await cb.message.answer("❌ Процесс заказа отменён.")

# ---- РЕГИСТРАЦИЯ ХЭНДЛЕРОВ ----
def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_update_baza_data, commands=["update_data"], state="*")
    dp.register_message_handler(cmd_order_wheel, commands=["order_wheel"], state="*")
    dp.register_callback_query_handler(exit_order, lambda c: c.data == "order_exit", state="*")
    dp.register_callback_query_handler(go_back,  lambda c: c.data == "order_back", state="*")
    dp.register_callback_query_handler(choose_company,
        lambda c: c.data.startswith("order_company:"), state=OrderWheelStates.WAIT_COMPANY)
    dp.register_callback_query_handler(choose_type,
        lambda c: c.data.startswith("order_type:"),    state=OrderWheelStates.WAIT_TYPE)
    dp.register_callback_query_handler(choose_size,
        lambda c: c.data.startswith("order_size:"),    state=OrderWheelStates.WAIT_SIZE)
    dp.register_callback_query_handler(choose_name,
        lambda c: c.data.startswith("order_name:"),    state=OrderWheelStates.WAIT_NAME)
    dp.register_callback_query_handler(choose_model,
        lambda c: c.data.startswith("order_model:"),   state=OrderWheelStates.WAIT_MODEL)
    dp.register_message_handler(enter_quantity,
        lambda m: m.text and m.text.isdigit(),        state=OrderWheelStates.WAIT_QUANTITY)
    dp.register_callback_query_handler(finish_order,
        lambda c: c.data.startswith("order_assembled:"), state=OrderWheelStates.WAIT_ASSEMBLY)
