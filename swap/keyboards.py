from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.config import PAGE_SIZE

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

def inline_kb_paginated(
    options: list[str],
    base_callback: str,
    cb_prev: str,
    cb_next: str,
    cb_item: str,
    cb_exit: str,
    page: int = 0
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    total_items = len(options)
    total_pages = (total_items + PAGE_SIZE - 1) // PAGE_SIZE
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total_items)
    slice_options = options[start:end]

    for idx_on_page, opt in enumerate(slice_options):
        kb.add(
            InlineKeyboardButton(
                text=opt,
                callback_data=f"{cb_item}:{page}:{idx_on_page}"
            )
        )

    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"{cb_prev}:{page - 1}"
            )
        )
    if page < total_pages - 1:
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперед ➡️",
                callback_data=f"{cb_next}:{page + 1}"
            )
        )
    if nav_buttons:
        kb.row(*nav_buttons)

    kb.add(InlineKeyboardButton(text="Выход", callback_data=cb_exit))
    return kb

def build_arrival_fix_keyboard(arrival_items: list[dict]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, it in enumerate(arrival_items):
        # показываем имя, ячейку и количество
        text = f"{it['name']} 🗂 {it.get('cell','')} ({it['quantity']})"
        kb.insert(InlineKeyboardButton(text=text, callback_data=f"arrival_delete:{idx}"))
    # добавляем действия
    kb.add(
        InlineKeyboardButton(text="Добавить", callback_data="arrival_fix:add"),
        InlineKeyboardButton(text="Завершить отчет", callback_data="arrival_fix:finish")
    )
    return kb
