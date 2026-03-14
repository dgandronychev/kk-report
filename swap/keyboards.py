from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
from typing import List, Tuple, Set

WEEKDAYS_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}

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

# ── Клавиатура исправления списка (используется в /transfer) ──
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
        try:
            dt = datetime.strptime(d, "%d.%m.%Y")
            weekday = WEEKDAYS_RU[dt.weekday()]  # или WEEKDAYS_RU_FULL[dt.weekday()]
            text = f"{prefix}{d} ({weekday})"
        except Exception:
            text = f"{prefix}{d}"
        kb.add(InlineKeyboardButton(text, callback_data=f"select_date:{idx}"))
    kb.add(InlineKeyboardButton("Готово", callback_data="exit"))
    return kb

def build_locations_kb(locations: list[str], selected: set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, loc in enumerate(locations):
        prefix = "✅ " if loc in selected else ""
        kb.add(InlineKeyboardButton(prefix + loc, callback_data=f"select_loc:{idx}"))
    kb.add(InlineKeyboardButton("Готово", callback_data="exit_loc"))
    return kb

# ── Подтверждение для большинства сценариев (НЕ трогаем старые коллбэки) ──
def build_confirmation_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Всё верно", callback_data="confirm:yes"),
        InlineKeyboardButton("✏️ Изменить даты", callback_data="confirm:dates"),
        InlineKeyboardButton("✏️ Изменить локации", callback_data="confirm:locs"),
    )
    kb.add(InlineKeyboardButton("❌ Выход", callback_data="exit"))
    return kb

# ── Подтверждение специально для «Заполнить график» ──
# Кнопка «Есть ошибка» запускает режим удаления выбранных дат
def build_confirmation_kb_fill() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Всё ок", callback_data="confirm:yes"),
        InlineKeyboardButton("❗ Есть ошибка", callback_data="confirm:fix"),
    )
    kb.add(InlineKeyboardButton("❌ Выход", callback_data="exit"))
    return kb


# ── Клавиатура для пометки «ошибочных» дат ──
def build_fix_dates_kb(chosen_dates: list[str], to_remove: Set[int]) -> InlineKeyboardMarkup:
    """
    chosen_dates — уже выбранные пользователем даты (строки DD.MM.YYYY)
    to_remove — индексы дат, помеченные на удаление
    """
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, d in enumerate(chosen_dates):
        # помеченные на удаление показываем с ❌, остальные — с ✅
        prefix = "❌ " if idx in to_remove else "✅ "
        kb.add(InlineKeyboardButton(prefix + d, callback_data=f"fix_date:{idx}"))
    kb.add(
        InlineKeyboardButton("🗑 Удалить отмеченные", callback_data="fix_dates:apply"),
    )
    kb.add(
        InlineKeyboardButton("↩ Назад", callback_data="confirm:back"),
    )
    return kb

# ── Клавиатуры для отмены смен ──
def build_cancellation_kb(
    shifts: List[Tuple[str, str]],
    selected: Set[int]
) -> InlineKeyboardMarkup:
    """
    shifts — список (дата, локация)
    selected — множество индексов уже отмеченных на отмену
    """
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, (d, loc) in enumerate(shifts):
        prefix = "❌ " if idx in selected else "✅ "
        text = f"{prefix}{d} {loc}"
        kb.add(InlineKeyboardButton(text, callback_data=f"select_shift:{idx}"))
    kb.add(InlineKeyboardButton("Отменить смену", callback_data="exit_cancel"))
    return kb

def build_confirm_cancel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Да, отменить", callback_data="confirm:yes"),
        InlineKeyboardButton("Нет",         callback_data="confirm:no"),
    )
    return kb

# ── Списки/навигация для «Проблемы» ──
def inline_kb_list(options: list[str], base_callback: str, cb_back: str, cb_exit: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for idx, opt in enumerate(options):
        kb.add(InlineKeyboardButton(opt, callback_data=f"{base_callback}{idx}"))
    kb.add(
        InlineKeyboardButton("Назад", callback_data=cb_back),
        InlineKeyboardButton("Выход", callback_data=cb_exit)
    )
    return kb


def inline_kb_back_exit(cb_back: str, cb_exit: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Назад", callback_data=cb_back),
        InlineKeyboardButton("Выход", callback_data=cb_exit)
    )
    return kb

def inline_kb_done_back(done_callback: str, back_callback: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Готово", callback_data=done_callback),
        InlineKeyboardButton("Назад", callback_data=back_callback)
    )
    return kb

def kb_enroll_start() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Записаться", callback_data="enroll:start"))
    return kb

def kb_fill_chart_start() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Записать на смену", callback_data="fill:start"))
    return kb
