import logging
import requests
import asyncio
from typing import Union

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import (
    ParseMode,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
)
from aiogram.utils.exceptions import MessageNotModified
from aiogram.utils.markdown import escape_md

from app.utils.script import resolve_event
from app.states import ProblemReportStates
from app.keyboards import (
    inline_kb_list,
    inline_kb_exit_and_list,
    inline_kb_back_exit,
    inline_kb_done_back,
)
from app.config import (
    CHAT_ID_PROBLEM,
    URL_GET_INFO_TASK,
    TOKEN_BOT,
    CHAT_ID_PROBLEM_SHM_YAND,
    CHAT_ID_PROBLEM_SHM_SITY,
    CHAT_ID_PROBLEM_MOIKA_ST,
    CHAT_ID_PROBLEM_SHM_ST,
    URL_TASK_FIO,
    CHAT_ID_PROBLEM_LOG,
    THREAD_ID_PROBLEM_LOG,
    ADMIN_ID,
)
from app.utils.gsheets import (
    write_problem_report,
    get_max_report_number,
    find_logistics_rows,
    write_problem_report_new,
)

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)
photo_locks: dict[int, asyncio.Lock] = {}

# Основной перечень проблем (то, что пользователь видит первым)
OFF_REASONS = [
    "Авто в сугробе",
    "Мойка закрывается",
    "На мойке проблема с оборудованием",
    "Нет ведомостей на локации",
    "Нет мойщика",
    "Нет НЗ на локации",
    "Не проведена химчистка",
    "Ожидание мойки",
    "Отсутствуют ТС для перегона",
    "Пробит бачок для НЗ",
    "Проблема ГИБДД",
    "Проблема с авто",
    "Проблема с качеством",
    "Сняться со смены",
]

# Подпричины только для «Сняться со смены»
SMENA_REASONS = [
    "Другая причина",
    "Плохое самочувствие",
    "Не работает ГЕО",
]

# Подтипы проблемы с качеством (мойка)
QUALITY_WASH_SUBTYPES = [
    "Замечания к стеклам после мойки",
    "Замечания к салону после мойки",
    "Замечания к кузову после мойки",
    "Замечания к багажнику после мойки",
    "Другое",
]

# Подтипы проблемы с качеством (шинный сервис)
QUALITY_TIRE_SUBTYPES = [
    "Давление установлено не верно",
    "Нет болта",
    "Нет баланса",
    "Не удалось поменять колесо",
    "Не верная направленность колеса",
    "Не сделали протяжку/нет динамометрического ключа",
]

ADMIN_WASH_SUBTYPES = {
    "Не проведена химчистка": "Административная проблема",
    "Мойка закрывается": "Административная проблема",
    "Нет мойщика": "Административная проблема",
    "На мойке проблема с оборудованием": "Административная проблема",
    "Нет ведомостей на локации": "Административная проблема",
    "Нет НЗ на локации": "Административная проблема",
}

OTHER_ADMIN_SUBTYPES = {
    "Пробит бачок для НЗ": "Другая проблема",
    "Проблема ГИБДД": "Другая проблема",
    "Проблема с авто": "Другая проблема",
}

# --- Вспомогательное: lock на чат для аккуратной обработки фото-групп ---


async def get_photo_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in photo_locks:
        photo_locks[chat_id] = asyncio.Lock()
    return photo_locks[chat_id]


# --- Шаг 1: Получение ФИО и выбор задачи ---


async def cmd_problem(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    message, actor = resolve_event(event)

    if message.chat.type != "private":
        await message.answer("Доступно только в личных сообщениях")
        return

    # Проверяем регистрацию пользователя
    try:
        resp = requests.get(URL_TASK_FIO, data={"chat_id": str(actor.id)})
        resp.raise_for_status()
        user = resp.json().get("user", {})
        fullname = user.get("fullname")
        if not fullname:
            raise ValueError("Не зарегистрирован")
    except Exception:
        return await message.answer(
            escape_md(
                "⚠️ Похоже, вы ещё не зарегистрированы. "
                "Пожалуйста, пройдите регистрацию командой /registration"
            ),
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )

    logger.info(
        "[problem] User @%s (%s) начал оформление проблемы",
        actor.username,
        actor.id,
    )
    await state.finish()
    await state.update_data(user_id=actor.id, username=actor.username)

    # Получаем задачи по пользователю
    try:
        data = {"chat_id": str(message.chat.id)}
        resp = requests.get(URL_GET_INFO_TASK, data=data)
        resp.raise_for_status()
        rep = resp.json()
        fio = rep["user"]["fullname"]
        tasks = rep.get("tasks", [])
    except Exception as e:
        logger.exception("Ошибка при обращении к API: %s", e)
        await message.answer(
            escape_md("Возникла ошибка запроса задач. Обратитесь к разработчикам"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )
        return

    await state.update_data(fio=fio)

    # Нет активных задач
    if not tasks:
        if actor.id in ADMIN_ID:
            # тестовая задача для админа
            task_id = 123456789
            car_plate = "А111АА777"
            address = "Тестовый адрес"
            company = "Тестовая компания"
            direction = "Мойка"
            await state.update_data(
                task_id=task_id,
                car_plate=car_plate,
                address=address,
                company=company,
                direction=direction,
            )
            keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("Подтвердить", callback_data="pr_confirm_task:0"),
                InlineKeyboardButton("Выход", callback_data="pr_exit"),
            )
            await message.answer(
                escape_md(
                    "ТЕСТОВАЯ ЗАДАЧА:\n"
                    f"Госномер: {car_plate}\n"
                    f"Адрес: {address}\n"
                    f"Компания: {company}\n"
                    f"Направление: {direction}\n\n"
                    "Подтвердите выбор задачи",
                ),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await ProblemReportStates.WAIT_TASK_CONFIRM.set()
            return
        else:
            await message.answer("У вас нет активных задач")
            return

    # Одна задача — сразу подтверждаем
    if len(tasks) == 1:
        task = tasks[0]
        task_id = task.get("id")
        car_plate = task.get("car_plate")
        address = task.get("wash__address") or task.get("tire_location")
        company = task.get("carsharing__name")
        direction = task.get("type")
        await state.update_data(
            task_id=task_id,
            car_plate=car_plate,
            address=address,
            company=company,
            direction=direction,
        )

        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("Подтвердить", callback_data="pr_confirm_task:0"),
            InlineKeyboardButton("Выход", callback_data="pr_exit"),
        )
        text = (
            f"Найдена задача:\n"
            f"Госномер: {car_plate}\n"
            f"Адрес: {address}\n"
            f"Компания: {company}\n"
            f"Направление: {direction}\n\n"
            f"Подтвердите выбор задачи"
        )
        await message.answer(
            escape_md(text),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=keyboard,
        )
        await ProblemReportStates.WAIT_TASK_CONFIRM.set()
    else:
        # Несколько задач — выбираем по госномеру
        await state.update_data(tasks=tasks)
        kb = InlineKeyboardMarkup(row_width=1)
        for index, task in enumerate(tasks):
            car_plate = task.get("car_plate")
            kb.add(
                InlineKeyboardButton(
                    text=car_plate, callback_data=f"pr_select_task:{index}"
                )
            )
        kb.add(InlineKeyboardButton("Выход", callback_data="pr_exit"))
        await message.answer(
            escape_md("Выберите задачу (госномер):"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )
        await ProblemReportStates.WAIT_TASK_SELECTION.set()


# --- Подтверждение единственной задачи ---


async def pr_confirm_task(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    car_plate = data.get("car_plate")
    address = data.get("address")
    company = data.get("company")
    direction = data.get("direction")

    text = (
        f"Госномер: {car_plate}\n"
        f"Адрес: {address}\n"
        f"Компания: {company}\n"
        f"Направление: {direction}\n\n"
        f"Выберите проблему из перечня:"
    )
    try:
        await call.message.edit_text(
            escape_md(text),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=inline_kb_list(
                OFF_REASONS,
                base_callback="pr_problem:",
                cb_back="pr_back",
                cb_exit="pr_exit",
            ),
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")
    await ProblemReportStates.WAIT_TYPE.set()


# --- Выбор задачи из списка ---


async def pr_select_task(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    tasks = data.get("tasks", [])
    try:
        _, idx = call.data.split(":", maxsplit=1)
        idx = int(idx)
        chosen_task = tasks[idx]
    except Exception as e:
        logger.exception("Ошибка выбора задачи: %s", e)
        await call.message.answer("Ошибка выбора задачи")
        return

    task_id = chosen_task.get("id")
    car_plate = chosen_task.get("car_plate")
    address = chosen_task.get("wash__address") or chosen_task.get("tire_location")
    company = chosen_task.get("carsharing__name")
    direction = chosen_task.get("type")

    await state.update_data(
        task_id=task_id,
        car_plate=car_plate,
        address=address,
        company=company,
        direction=direction,
    )

    text = (
        f"Госномер: {car_plate}\n"
        f"Адрес: {address}\n"
        f"Компания: {company}\n"
        f"Направление: {direction}\n\n"
        f"Выберите проблему из перечня:"
    )
    try:
        await call.message.edit_text(
            escape_md(text),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=inline_kb_list(
                OFF_REASONS,
                base_callback="pr_problem:",
                cb_back="pr_back",
                cb_exit="pr_exit",
            ),
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")
    await ProblemReportStates.WAIT_TYPE.set()


# --- Шаг 2: обработка выбранной проблемы ---


async def pr_problem(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    try:
        _, idx = call.data.split(":", maxsplit=1)
        idx = int(idx)
        problem = OFF_REASONS[idx]
    except Exception as e:
        logger.exception("Ошибка выбора проблемы: %s", e)
        await call.message.answer("Ошибка выбора проблемы")
        return

    problem_subtype = ""

    # Административная проблема (мойка)
    if problem in ADMIN_WASH_SUBTYPES:
        problem_subtype = problem
        problem = ADMIN_WASH_SUBTYPES[problem_subtype]

    # Другая проблема
    elif problem in OTHER_ADMIN_SUBTYPES:
        problem_subtype = problem
        problem = OTHER_ADMIN_SUBTYPES[problem_subtype]

    # Отсутствуют ТС для перегона — подтип = он же
    elif problem == "Отсутствуют ТС для перегона":
        problem = "Отсутствуют ТС"
        problem_subtype = "Отсутствуют ТС для перегона"

    elif problem == "Авто в сугробе":
        problem_subtype = "Авто в сугробе"

    await state.update_data(
        problem_type=problem,
        problem_subtype=problem_subtype
    )

    # Ветка: Сняться со смены → подпричины
    if problem == "Сняться со смены":
        try:
            await call.message.edit_text(
                "Выберите причину снятия со смены:",
                reply_markup=inline_kb_list(
                    SMENA_REASONS,
                    base_callback="off_reason:",
                    cb_back="pr_back",
                    cb_exit="pr_exit",
                ),
            )
        except MessageNotModified:
            logger.debug("Сообщение не изменилось — пропускаем edit_text")
        await ProblemReportStates.WAIT_OFF_REASON.set()
        return

    # Ветка: Отсутствуют ТС для перегона → обязательное фото карты
    if problem == "Отсутствуют ТС для перегона":
        await state.update_data(
            description="Просьба согласовать завершение смены; приложено фото карты",
            off_requires_media=True,
        )
        instr = (
            "Пришлите фото карты для согласования завершения смены "
            "или изменения локации (1–8 фото)"
        )
        try:
            msg = await call.message.edit_text(
                escape_md(instr),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_exit_and_list(
                    ["Готово"], base_callback="pr_done:", cb_exit="pr_exit"
                ),
            )
        except MessageNotModified:
            msg = call.message
        await state.update_data(instructions_msg_id=msg.message_id)
        await ProblemReportStates.WAIT_MEDIA.set()
        return

    # Ветка: Нет ведомостей / Пробит бачок → просто описать проблему
    if problem in ("Нет ведомостей на локации", "Пробит бачок для НЗ"):
        try:
            await call.message.edit_text(
                escape_md("Опишите проблему:"),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_back_exit(cb_back="pr_back", cb_exit="pr_exit"),
            )
        except MessageNotModified:
            logger.debug("Сообщение не изменилось — пропускаем edit_text")
        await state.update_data(off_requires_media=False)
        await ProblemReportStates.WAIT_DESCRIPTION.set()
        return

    # Ветка: Проблема с качеством → подтипы
    if problem == "Проблема с качеством":
        data = await state.get_data()
        direction = data.get("direction")

        # Для мойки — подтипы из схемы; для шин — старые подтипы;
        # для прочего — сразу описание.
        if direction == "Мойка":
            subtypes = QUALITY_WASH_SUBTYPES
        elif direction in ("Сезонный шиномонтаж", "Шиномонтаж"):
            subtypes = QUALITY_TIRE_SUBTYPES
        else:
            subtypes = []

        if subtypes:
            await state.update_data(available_subtypes=subtypes)
            try:
                await call.message.edit_text(
                    escape_md("Выберите подтип проблемы:"),
                    parse_mode=types.ParseMode.MARKDOWN_V2,
                    reply_markup=inline_kb_list(
                        subtypes,
                        base_callback="pr_quality:",
                        cb_back="pr_back",
                        cb_exit="pr_exit",
                    ),
                )
            except MessageNotModified:
                logger.debug("Сообщение не изменилось — пропускаем edit_text")
            await ProblemReportStates.WAIT_SUBTYPE.set()
            return

    # Остальные проблемы → сразу описание
    try:
        await call.message.edit_text(
            escape_md("Опишите проблему:"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=inline_kb_back_exit(cb_back="pr_back", cb_exit="pr_exit"),
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")
    await state.update_data(off_requires_media=False)
    await ProblemReportStates.WAIT_DESCRIPTION.set()


# --- Сняться со смены: выбор подпричины ---


async def off_reason(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    try:
        _, idx = call.data.split(":", maxsplit=1)
        idx = int(idx)
        reason = SMENA_REASONS[idx]
    except Exception as e:
        logger.exception("Ошибка выбора причины снятия: %s", e)
        await call.message.answer("Ошибка выбора причины")
        return

    await state.update_data(problem_subtype=reason)

    # После выбора причины (Другая, Плохое самочувствие, Не работает ГЕО)
    # всегда переходим к шагу "Опишите проблему"
    try:
        await call.message.edit_text(
            escape_md("Опишите проблему:"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=inline_kb_back_exit(cb_back="pr_back", cb_exit="pr_exit"),
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")

    await state.update_data(off_requires_media=False)
    await ProblemReportStates.WAIT_DESCRIPTION.set()

# --- Проблема с качеством: выбор подтипа ---
async def pr_quality_subtype(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    try:
        _, idx = call.data.split(":", maxsplit=1)
        idx = int(idx)
    except Exception as e:
        logger.exception("Ошибка выбора подтипа проблемы: %s", e)
        return

    data = await state.get_data()
    available = data.get("available_subtypes", [])
    if not (0 <= idx < len(available)):
        await call.message.answer("Некорректный подтип проблемы")
        return

    subtype = available[idx]
    await state.update_data(problem_subtype=subtype)

    try:
        await call.message.edit_text(
            escape_md("Опишите проблему:"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=inline_kb_back_exit(cb_back="pr_back", cb_exit="pr_exit"),
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")
    await state.update_data(off_requires_media=False)
    await ProblemReportStates.WAIT_DESCRIPTION.set()


# --- Шаг 5: Получение описания проблемы и предложение загрузить медиа ---
async def pr_description(message: types.Message, state: FSMContext):
    data = await state.get_data()
    problem_type = data.get("problem_type")

    # ------------------------------------------
    # Ветка "Сняться со смены"
    # ------------------------------------------
    if problem_type == "Сняться со смены":
        await state.update_data(description=message.text)

        waiting_msg = await message.answer(
            escape_md("Подождите, идет оформление"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )

        ok = await process_problem_report(waiting_msg, state)
        if ok:
            await waiting_msg.edit_text(
                escape_md("Заявка отправлена. Ожидается согласование логиста"),
                parse_mode=types.ParseMode.MARKDOWN_V2,
            )
        else:
            await waiting_msg.edit_text(
                escape_md("Ошибка при оформлении. Попробуйте позже"),
                parse_mode=types.ParseMode.MARKDOWN_V2,
            )
        return

    # ------------------------------------------
    # Остальные проблемы
    # ------------------------------------------
    await state.update_data(description=message.text)

    instr_msg = await message.answer(
        escape_md(
            "Прикрепите фото (если есть, 0–8 шт.) или документ.\n"
            "Если медиа не требуются, нажмите кнопку 'Готово'."
        ),
        parse_mode=types.ParseMode.MARKDOWN_V2,
        reply_markup=inline_kb_done_back(
            done_callback="pr_done:", back_callback="pr_back"
        ),
    )

    await state.update_data(instructions_msg_id=instr_msg.message_id)
    await ProblemReportStates.WAIT_MEDIA.set()

# --- Шаг 6a: Получение фото ---


async def pr_photo(message: types.Message, state: FSMContext):
    lock = await get_photo_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        photos = data.get("photos", [])

        group_id = message.media_group_id
        if group_id and data.get("last_media_group_id") != group_id:
            await state.update_data(last_media_group_id=group_id)

        if message.photo:
            photos.append(message.photo[-1].file_id)
            await state.update_data(photos=photos)

            last_photo_msg_id = data.get("last_photo_msg_id")
            if last_photo_msg_id:
                try:
                    await bot.delete_message(
                        chat_id=message.chat.id, message_id=last_photo_msg_id
                    )
                except Exception as e:
                    logger.exception("Ошибка удаления предыдущего сообщения: %s", e)
                await state.update_data(last_photo_msg_id=None)

            new_text = (
                f"Фото получено: {len(photos)}. "
                f"Можно отправить ещё или нажмите 'Готово'."
            )
            new_msg = await message.answer(
                escape_md(new_text),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_exit_and_list(
                    ["Готово"], base_callback="pr_done:", cb_exit="pr_exit"
                ),
            )
            await state.update_data(last_photo_msg_id=new_msg.message_id)
        else:
            await message.answer(
                escape_md("Не удалось получить фото. Попробуйте снова."),
                parse_mode=types.ParseMode.MARKDOWN_V2,
            )


# --- Шаг 6b: Получение документа ---


async def pr_document(message: types.Message, state: FSMContext):
    data = await state.get_data()
    documents = data.get("documents", [])
    if message.document:
        documents.append(message.document.file_id)
        await state.update_data(documents=documents)
        last_doc_msg_id = data.get("last_doc_msg_id")
        new_text = (
            f"Документ получен: {len(documents)}. "
            f"Можно отправить ещё или нажмите 'Готово'."
        )
        if last_doc_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=last_doc_msg_id,
                    text=new_text,
                    reply_markup=inline_kb_exit_and_list(
                        ["Готово"], base_callback="pr_done:", cb_exit="pr_exit"
                    ),
                )
            except Exception as e:
                logger.exception("Ошибка редактирования документа: %s", e)
        else:
            new_msg = await message.answer(
                escape_md(new_text),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_exit_and_list(
                    ["Готово"], base_callback="pr_done:", cb_exit="pr_exit"
                ),
            )
            await state.update_data(last_doc_msg_id=new_msg.message_id)
    else:
        await message.answer(
            escape_md("Не удалось получить документ. Попробуйте снова."),
            parse_mode=types.ParseMode.MARKDOWN_V2,
        )


# --- Обработчик "Готово" после медиа ---


async def pr_done_photos(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()

    # Для ветки "Отсутствуют ТС для перегона" требуем хотя бы одно фото
    if data.get("off_requires_media") and not data.get("photos"):
        try:
            await call.message.edit_text(
                escape_md(
                    "Нужно прикрепить хотя бы одно фото карты.\n"
                    "Отправьте фото и снова нажмите 'Готово'."
                ),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_exit_and_list(
                    ["Готово"], base_callback="pr_done:", cb_exit="pr_exit"
                ),
            )
        except MessageNotModified:
            pass
        return

    try:
        await call.message.edit_text(
            escape_md("Подождите, идет оформление"),
            parse_mode=types.ParseMode.MARKDOWN_V2,
            reply_markup=None,
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")

    success = await process_problem_report(call.message, state)
    if success:
        try:
            await call.message.edit_text("Проблема оформлена")
        except MessageNotModified:
            logger.debug("Сообщение не изменилось — пропускаем edit_text")
    else:
        try:
            await call.message.edit_text(
                escape_md("Ошибка при оформлении. Попробуйте позже"),
                parse_mode=types.ParseMode.MARKDOWN_V2,
            )
        except MessageNotModified:
            logger.debug("Сообщение не изменилось — пропускаем edit_text")


# --- Шаг 7: Формирование и отправка отчёта ---


async def process_problem_report(
    message: types.Message, state: FSMContext
) -> bool:
    data = await state.get_data()
    company = data.get("company")
    direction = data.get("direction")
    fio = data.get("fio")
    username = data.get("username")
    car_plate = data.get("car_plate")
    address = data.get("address")
    problem_type = data.get("problem_type")
    problem_subtype = data.get("problem_subtype", "")
    description = data.get("description", "")
    photos = data.get("photos", [])
    documents = data.get("documents", [])

    report_number = "#" + str(get_max_report_number() + 1)

    _teg, _fi = find_logistics_rows()
    if not _fi:
        logist = ""
    else:
        logist = " , ".join(f"{name} ({teg})" for name, teg in zip(_fi, _teg))

    tg_report = (
        f"Номер отчета: {report_number}\n"
        f"Номер авто: {car_plate}\n"
        f"Кто сообщает: {fio}\n"
        f"@{username}\n"
        f"Компания: {company}\n"
        f"Направление: {direction}\n"
        f"Адрес: {address}\n"
        f"Тип проблемы: {problem_type}\n"
        f"Подтип проблемы: {problem_subtype}\n"
        f"Описание проблемы:\n\"{description}\"\n\n"
        "Возьмите в работу данный инцидент\n"
        "@SerezhaMatukaytis\n@seregaflex\n"
        f"{logist}"
    )

    # Определяем целевые чаты
    if direction == "Сезонный шиномонтаж":
        id_chat = CHAT_ID_PROBLEM_SHM_YAND if company == "Яндекс" else CHAT_ID_PROBLEM_SHM_SITY
        id_chat_st = CHAT_ID_PROBLEM_SHM_ST
        thread_id = None
    elif direction == "Мойка":
        id_chat = CHAT_ID_PROBLEM
        id_chat_st = CHAT_ID_PROBLEM_MOIKA_ST
        thread_id = None
    else:
        id_chat = CHAT_ID_PROBLEM
        id_chat_st = CHAT_ID_PROBLEM_LOG
        thread_id = THREAD_ID_PROBLEM_LOG

    try:
        # Отправка медиа-группы, если есть фото
        if photos:
            media = [
                InputMediaPhoto(media=photos[0], caption=escape_md(tg_report))
            ] + [InputMediaPhoto(media=pid) for pid in photos[1:]]
            sent_main = await bot.send_media_group(
                chat_id=id_chat_st, media=media, message_thread_id=thread_id
            )
            if problem_type != "Сняться со смены":
                await bot.send_media_group(chat_id=id_chat, media=media)
            first_msg = sent_main[0]
            message_id = first_msg.message_id
        else:
            sent_main = await bot.send_message(
                chat_id=id_chat_st,
                text=escape_md(tg_report),
                message_thread_id=thread_id,
            )
            if problem_type != "Сняться со смены":
                await bot.send_message(
                    chat_id=id_chat, text=escape_md(tg_report)
                )
            message_id = sent_main.message_id

        # Отправка документов
        for doc in documents:
            sent_main = await bot.send_document(
                chat_id=id_chat_st,
                document=doc,
                message_thread_id=thread_id,
            )
            if problem_type != "Сняться со смены":
                await bot.send_document(chat_id=id_chat, document=doc)
            message_id = sent_main.message_id

        # Формирование ссылки на сообщение
        link = str(id_chat).lstrip("-100")
        if thread_id is not None:
            message_link = f"https://t.me/c/{link}/{thread_id}/{message_id}"
        else:
            message_link = f"https://t.me/c/{link}/{message_id}"

        write_problem_report(
            report_number,
            company,
            direction,
            fio,
            car_plate,
            address,
            problem_type,
            problem_subtype,
            description,
            message_link,
        )

        try:
            write_problem_report_new(
                report_number,
                company,
                direction,
                fio,
                car_plate,
                address,
                problem_type,
                problem_subtype,
                description,
                message_link
            )
        except Exception as e:
            logger.exception("Ошибка write_problem_report_new: %s", e)
            return True

        return True

    except Exception as e:
        logger.exception("Ошибка отправки отчета: %s", e)
        return False


# --- Обработчики кнопок "Назад" и "Выход" ---


async def pr_back(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    current_state = await state.get_state()

    if current_state == ProblemReportStates.WAIT_TYPE.state:
        # Возврат к выбору задачи / подтверждению
        data = await state.get_data()
        tasks = data.get("tasks")
        if tasks:
            kb = InlineKeyboardMarkup(row_width=1)
            for index, task in enumerate(tasks):
                car_plate = task.get("car_plate")
                kb.add(
                    InlineKeyboardButton(
                        text=car_plate, callback_data=f"pr_select_task:{index}"
                    )
                )
            kb.add(InlineKeyboardButton("Выход", callback_data="pr_exit"))
            try:
                await call.message.edit_text(
                    escape_md("Выберите задачу (госномер):"),
                    parse_mode=types.ParseMode.MARKDOWN_V2,
                    reply_markup=kb,
                )
            except MessageNotModified:
                logger.debug("Сообщение не изменилось — пропускаем edit_text")
            await ProblemReportStates.WAIT_TASK_SELECTION.set()
        else:
            car_plate = data.get("car_plate")
            address = data.get("address")
            company = data.get("company")
            direction = data.get("direction")
            keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("Подтвердить", callback_data="pr_confirm_task:0"),
                InlineKeyboardButton("Выход", callback_data="pr_exit"),
            )
            text = (
                f"Найдена задача:\n"
                f"Госномер: {car_plate}\n"
                f"Адрес: {address}\n"
                f"Компания: {company}\n"
                f"Направление: {direction}\n\n"
                f"Подтвердите выбор задачи"
            )
            try:
                await call.message.edit_text(
                    escape_md(text),
                    parse_mode=types.ParseMode.MARKDOWN_V2,
                    reply_markup=keyboard,
                )
            except MessageNotModified:
                logger.debug("Сообщение не изменилось — пропускаем edit_text")
            await ProblemReportStates.WAIT_TASK_CONFIRM.set()

    elif current_state in [
        ProblemReportStates.WAIT_SUBTYPE.state,
        ProblemReportStates.WAIT_DESCRIPTION.state,
        ProblemReportStates.WAIT_MEDIA.state,
        ProblemReportStates.WAIT_OFF_REASON.state,
    ]:
        data = await state.get_data()
        car_plate = data.get("car_plate")
        address = data.get("address")
        company = data.get("company")
        direction = data.get("direction")
        text = (
            f"Госномер: {car_plate}\n"
            f"Адрес: {address}\n"
            f"Компания: {company}\n"
            f"Направление: {direction}\n\n"
            f"Выберите проблему из перечня:"
        )
        try:
            await call.message.edit_text(
                escape_md(text),
                parse_mode=types.ParseMode.MARKDOWN_V2,
                reply_markup=inline_kb_list(
                    OFF_REASONS,
                    base_callback="pr_problem:",
                    cb_back="pr_back",
                    cb_exit="pr_exit",
                ),
            )
        except MessageNotModified:
            logger.debug("Сообщение не изменилось — пропускаем edit_text")
        await ProblemReportStates.WAIT_TYPE.set()


async def pr_exit(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    try:
        from app.handlers.menu import back_to_menu_kb

        await call.message.edit_text(
            "Работа завершена", reply_markup=back_to_menu_kb()
        )
    except MessageNotModified:
        logger.debug("Сообщение не изменилось — пропускаем edit_text")
    await state.finish()


# --- Регистрация хендлеров ---


def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_problem, commands=["problem"], state="*")

    dp.register_callback_query_handler(
        pr_confirm_task,
        lambda c: c.data.startswith("pr_confirm_task:"),
        state=ProblemReportStates.WAIT_TASK_CONFIRM,
    )
    dp.register_callback_query_handler(
        pr_select_task,
        lambda c: c.data.startswith("pr_select_task:"),
        state=ProblemReportStates.WAIT_TASK_SELECTION,
    )

    dp.register_callback_query_handler(
        pr_problem,
        lambda c: c.data.startswith("pr_problem:"),
        state=ProblemReportStates.WAIT_TYPE,
    )
    dp.register_callback_query_handler(
        off_reason,
        lambda c: c.data.startswith("off_reason:"),
        state=ProblemReportStates.WAIT_OFF_REASON,
    )
    dp.register_callback_query_handler(
        pr_quality_subtype,
        lambda c: c.data.startswith("pr_quality:"),
        state=ProblemReportStates.WAIT_SUBTYPE,
    )

    dp.register_message_handler(
        pr_description,
        content_types=["text"],
        state=ProblemReportStates.WAIT_DESCRIPTION,
    )
    dp.register_message_handler(
        pr_photo,
        content_types=[types.ContentType.PHOTO],
        state=ProblemReportStates.WAIT_MEDIA,
    )
    dp.register_message_handler(
        pr_document,
        content_types=[types.ContentType.DOCUMENT],
        state=ProblemReportStates.WAIT_MEDIA,
    )
    dp.register_callback_query_handler(
        pr_done_photos,
        lambda c: c.data.startswith("pr_done:"),
        state=ProblemReportStates.WAIT_MEDIA,
    )

    dp.register_callback_query_handler(pr_back, text="pr_back", state="*")
    dp.register_callback_query_handler(pr_exit, text="pr_exit", state="*")
