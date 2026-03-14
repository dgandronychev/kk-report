import logging
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md
from app.config import TOKEN_BOT, ADMIN_ID

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

HELP_TEXT = """
🛠 *Справка по командам*

👤 Пользовательские:
- /registration — пройти регистрацию
- /menu — меню по командам ниже
- /fill_chart — заполнить график на период
- /add_shift — добавить смену
- /cancel_shift — отменить смену
- /show_chart — показать предстоящие смены
- /problem — сообщить о проблеме

👮 Админские:
- /open_enroll — открыть запись на мойку
- /auto_distribute — автоматическое распределение исполнителей
- /graph_notify — ручное уведомление "Через 10 минут начнётся запись на мойку"
- /free_lot — отправить оповещение о свободных местах
- /graph_report — отчет о финальном распредении 
- /assign_cached — вручную назначить: выбрать локацию из кеша и вписать тег
- /graph_report — полный отчёт распределения в чат графика
- /change_loc @teg — Сменить уже назначенную смены 
- /help_admin — это сообщение со справкой
"""

async def cmd_help_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        return await message.answer("❌ У вас нет доступа к этой команде")

    await message.answer(escape_md(HELP_TEXT), parse_mode=types.ParseMode.MARKDOWN_V2)

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_help_admin, commands=["help_admin"], state="*")
