from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Tuple, Set

def inline_kb_yes_no_exit(exit_callback: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Да", callback_data="YES"),
        InlineKeyboardButton("Нет", callback_data="NO")
    )
    kb.add(
        InlineKeyboardButton("Выход", callback_data=exit_callback)
    )
    return kb

def inline_kb_exit_and_list(options: list[str], base_callback: str, cb_exit: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, opt in enumerate(options):
        kb.add(InlineKeyboardButton(opt, callback_data=f"{base_callback}{idx}"))
    kb.add(InlineKeyboardButton("Выход", callback_data=cb_exit))
    return kb

# ── Функция формирования клавиатуры для исправления ошибок (для /transfer) ──
def build_transfer_fix_keyboard(transfer_items: list) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for i, it in enumerate(transfer_items):
        kb.add(InlineKeyboardButton(text=f"{it['name']} (Количество: {it['quantity']})", callback_data=f"transfer_delete:{i}"))
    kb.add(
        InlineKeyboardButton(text="Добавить", callback_data="transfer_fix:add"),
        InlineKeyboardButton(text="Завершить отчет", callback_data="transfer_fix:finish")
    )
    return kb

def build_dates_kb(dates: list[str], selected: set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, d in enumerate(dates):
        prefix = "✅ " if d in selected else ""
        kb.add(InlineKeyboardButton(prefix + d, callback_data=f"select_date:{idx}"))
    kb.add(InlineKeyboardButton("Готово", callback_data="exit"))
    return kb

def build_locations_kb(locations: list[str], selected: set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, loc in enumerate(locations):
        prefix = "✅ " if loc in selected else ""
        kb.add(InlineKeyboardButton(prefix + loc, callback_data=f"select_loc:{idx}"))
    kb.add(InlineKeyboardButton("Готово", callback_data="exit_loc"))
    return kb

def build_confirmation_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Всё верно", callback_data="confirm:yes"),
        InlineKeyboardButton("✏️ Изменить даты", callback_data="confirm:dates"),
        InlineKeyboardButton("✏️ Изменить локации", callback_data="confirm:locs"),
    )
    kb.add(InlineKeyboardButton("❌ Выход", callback_data="exit"))
    return kb

def build_cancellation_kb(
    shifts: List[Tuple[str, str]],
    selected: Set[int]
) -> InlineKeyboardMarkup:
    """
    shifts — список (дата, локация)
    selected — множество индексов уже отмеченных на отмену
    по умолчанию все показываем с зелёной галочкой,
    при выборе меняем на ❌
    """
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, (d, loc) in enumerate(shifts):
        prefix = "❌ " if idx in selected else "✅ "
        text = f"{prefix}{d} — {loc}"
        kb.add(
            InlineKeyboardButton(
                text,
                callback_data=f"select_shift:{idx}"
            )
        )
    # кнопка подтверждения отмены
    kb.add(
        InlineKeyboardButton(
            "Отменить смену",
            callback_data="exit_cancel"
        )
    )
    return kb


def build_confirm_cancel_kb() -> InlineKeyboardMarkup:
    """
    Клавиатура с финальным Да/Нет для подтверждения отмены.
    """
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Да, отменить", callback_data="confirm:yes"),
        InlineKeyboardButton("Нет",         callback_data="confirm:no"),
    )
    return kb

def inline_kb_list(options: list[str], base_callback: str, cb_back: str, cb_exit: str) -> InlineKeyboardMarkup:
    """
    Пример клавиатуры для списка вариантов + «Назад», «Выход».
    Каждая опция порождает callback_data= base_callback + <индекс>
    """
    kb = InlineKeyboardMarkup(row_width=2)
    for idx, opt in enumerate(options):
        kb.add(InlineKeyboardButton(opt, callback_data=f"{base_callback}{idx}"))
    kb.add(
        InlineKeyboardButton("Назад", callback_data=cb_back),
        InlineKeyboardButton("Выход", callback_data=cb_exit)
    )
    return kb

def inline_kb_back_exit(cb_back: str, cb_exit: str) -> InlineKeyboardMarkup:
    """
    Пример клавиатуры «Назад / Выход»
    """
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Назад", callback_data=cb_back),
        InlineKeyboardButton("Выход", callback_data=cb_exit)
    )
    return kb

def inline_kb_done_back(done_callback: str, back_callback: str) -> InlineKeyboardMarkup:
    """
    Клавиатура с кнопками "Готово" и "Назад".
    """
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Готово", callback_data=done_callback),
        InlineKeyboardButton("Назад", callback_data=back_callback)
    )
    return kb