import asyncio
import requests
import logging
from datetime import date
from typing import Dict, Set, Union
import gspread

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import escape_md

from app.config import (
    TOKEN_BOT,
    ADMIN_ID,
    URL_GET_USER_ID_BY_FIO,
    CHAT_ID_PUSH,
    THREAD_ID_PUSH_SHIFT,
    CHAT_ID_GRAPH,
    THREAD_ID_GRAPH,
    URL_GOOGLE_SHEETS_ANSWERS,
)
from app.states import EnrollStates, AdminAssignStates
from app.keyboards import build_locations_kb, build_confirmation_kb_fill

from app.utils.gsheets import (
    read_today_locations_with_capacity_and_district,
    performers_for_today_flag1_with_district,
    update_location_for_user_today_by_tag,
    fetch_best_and_good_ids_for_today_with_threshold,
    _read_assignments_for_date,
    get_performers_for_date,
    find_today_assignment_row_and_loc_by_tag,
    find_logistics_rows,
)

from app.utils.write_buffer import SheetsWriteBuffer, UpdateTask

TRUCK_LOCATIONS = {
    "Ленинградское шоссе, 71Б, Москва": "https://yandex.ru/maps/-/CHGxeVKH",
    "Волгоградский проспект, 32к12, Москва": "https://yandex.ru/maps/org/24_moy_sam/1789263946?si=25g18e35xrbw6g18zg77v58g40",
    "улица Ибрагимова, 5, Москва (только Sollers Atlant)": "https://yandex.ru/maps/-/CHWKY6mo",
    "улица Авиаторов, 13с4, Москва": "https://yandex.ru/maps/-/CHWKQEoR",
    "1-й Магистральный тупик, 5с1, Москва": "https://yandex.ru/maps/-/CHAIrN-m",
    "Дзержинское шоссе, 7к1, Котельники": "https://yandex.ru/maps/-/CHWKQAJQ",
    "Сиреневый бульвар, 85, Москва": "https://yandex.ru/maps/-/CHS4EXJR",
}

logger = logging.getLogger(__name__)
bot = Bot(token=TOKEN_BOT, parse_mode=types.ParseMode.MARKDOWN_V2)

# ─────────────────────────────────────────────────────────────────────────────
# КЕШ и флажок «запись открыта»
# ─────────────────────────────────────────────────────────────────────────────
# cache: адрес -> оставшиеся места
LOC_CACHE: Dict[str, dict] = {}
ENROLL_LOCK: dict[int, dict] = {}
SIGNUP_OPEN: bool = False
# Кеш индексов на сегодня
SIGNUP_CTX: Dict[str, dict] = {}
# Буфер записей в "Ответы"
WRITE_BUFFER = SheetsWriteBuffer(flush_interval=30)
WRITE_BUFFER.start()

def _purge_old_locks():
    """Удаляет блокировки за прошлые даты."""
    today = _today_str()
    drop = [uid for uid, v in ENROLL_LOCK.items() if v.get("date") != today]
    for uid in drop:
        ENROLL_LOCK.pop(uid, None)

def _user_locked(uid: int) -> bool:
    v = ENROLL_LOCK.get(uid)
    return bool(v and v.get("date") == _today_str())

def _user_lock_addr(uid: int) -> str:
    v = ENROLL_LOCK.get(uid) or {}
    return str(v.get("addr", ""))

def _lock_user(uid: int, addr: str):
    ENROLL_LOCK[uid] = {"date": _today_str(), "addr": addr}
async def _push_shift_line(text: str) -> None:
    """
    Отправить строку/сообщение в топик смен (CHAT_ID_PUSH, THREAD_ID_PUSH_SHIFT).
    Текст экранируется под MarkdownV2.
    """
    try:
        await bot.send_message(
            CHAT_ID_PUSH,
            escape_md(text),
            message_thread_id=THREAD_ID_PUSH_SHIFT
        )
    except Exception as e:
        logger.warning("_push_shift_line: не удалось отправить в топик: %s", e)

def _get_fio_from_api_payload(data: dict) -> str:
    """
    Унифицировать извлечение ФИО из ответа API.
    Поддерживает возможные ключи: fio | full_name | name.
    """
    return (
        (data or {}).get("fio")
        or (data or {}).get("full_name")
        or (data or {}).get("name")
        or ""
    ).strip()

def _build_change_loc_kb_pick(tag: str) -> types.InlineKeyboardMarkup:
    """Клавиатура выбора новой локации из кеша."""
    kb = types.InlineKeyboardMarkup(row_width=1)
    locs = _available_locations_list()
    for i, addr in enumerate(locs):
        left = LOC_CACHE.get(addr, {}).get("left", 0)
        kb.add(types.InlineKeyboardButton(f"{addr} — {left}", callback_data=f"chg_loc:sel:{tag}:{i}"))
    # кнопка отмены
    kb.add(types.InlineKeyboardButton("Отмена", callback_data="chg_loc:sel:__cancel__:__"))
    return kb

def _loc_by_idx(i: int) -> str:
    locs = _available_locations_list()
    return locs[i] if 0 <= i < len(locs) else ""

async def _show_free_locations(cb: types.CallbackQuery, text: str, state: FSMContext):
    """Сообщает об ошибке/занятости и показывает актуальные свободные локации."""
    locs = _available_locations_list()
    if not locs:
        # закрываем запись, если во всех локациях 0
        global SIGNUP_OPEN
        SIGNUP_OPEN = False
        await cb.message.edit_text(escape_md("❌ Места закончились на всех адресах. Запись закрыта."))
        if state:
            await state.finish()
        return
    if state:
        await state.update_data(picked_loc=None)
    kb = build_locations_kb(locs, set())
    await cb.message.edit_text(escape_md(f"{text}\n\nВыберите другой адрес:"), reply_markup=kb)
    if state:
        await EnrollStates.WAIT_LOCATION.set()

# ─────────────────────────────────────────────────────────────────────────────
# /change_loc <tag>  (только админам, в ЛС)
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_change_loc(message: types.Message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await message.answer(escape_md("Использование: /change_loc <тег без @>"))
    tag = parts[1].strip().lstrip("@")
    if not LOC_CACHE:
        return await message.answer(escape_md("ℹ️ Кеш пуст. Сначала выполните /open_enroll"))
    row_idx, old_loc = await asyncio.to_thread(find_today_assignment_row_and_loc_by_tag, tag)
    if not row_idx:
        return await message.answer(escape_md(f"❌ На сегодня не найдено записи 'Заполнен' для @{tag}"))
    txt = (
        f"Текущий адрес @{tag}: {old_loc or '—'}\n\n"
        f"Изменить адрес?"
    )
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("Да", callback_data=f"chg_loc:pick:{tag}"),
        types.InlineKeyboardButton("Нет", callback_data="chg_loc:sel:__cancel__:__"),
    )
    await message.answer(escape_md(txt), reply_markup=kb)

# «Да» → показать доступные локации из кеша
async def on_chg_loc_pick(cb: types.CallbackQuery):
    await cb.answer()
    _, _, tag = cb.data.split(":", 2)  # chg_loc:pick:<tag>
    locs = _available_locations_list()
    if not locs:
        return await cb.message.edit_text(escape_md("❌ Во всех локациях места закончились"))
    await cb.message.edit_text(escape_md(f"Выберите новую локацию для @{tag}:"), reply_markup=_build_change_loc_kb_pick(tag))

# выбор локации → подтверждение
async def on_chg_loc_select(cb: types.CallbackQuery):
    await cb.answer()
    _, _, tag, idx_s = cb.data.split(":", 3)  # chg_loc:sel:<tag>:<idx>
    if tag == "__cancel__":
        return await cb.message.edit_text(escape_md("Операция отменена"))
    try:
        idx = int(idx_s)
    except Exception:
        return
    picked = _loc_by_idx(idx)
    if not picked:
       return
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("Подтвердить", callback_data=f"chg_loc:ok:{tag}:{idx}"),
        types.InlineKeyboardButton("Назад", callback_data=f"chg_loc:pick:{tag}")
    )
    await cb.message.edit_text(escape_md(f"Подтвердить изменение адреса @{tag} на:\n\n{picked}?"), reply_markup=kb)

# подтверждение → проверка left → апдейт «Ответы» → кеш
async def on_chg_loc_confirm(cb: types.CallbackQuery):
    await cb.answer()
    _, _, tag, idx_s = cb.data.split(":", 3)  # chg_loc:ok:<tag>:<idx>
    try:
        idx = int(idx_s)
    except Exception:
        return
    new_loc = _loc_by_idx(idx)
    if not new_loc:
        return
    left = int(LOC_CACHE.get(new_loc, {}).get("left", 0) or 0)
    if left <= 0:
        # кто-то успел занять — предложим выбрать снова
        return await cb.message.edit_text(
            escape_md("❌ На выбранной локации мест уже нет. Выберите другую:"),
            reply_markup=_build_change_loc_kb_pick(tag)
        )

    # ещё раз найдём точную строку и старую локацию
    row_idx, old_loc = await asyncio.to_thread(find_today_assignment_row_and_loc_by_tag, tag)
    if not row_idx:
        return await cb.message.edit_text(escape_md(f"❌ Не нашли актуальную запись для @{tag}"))
    if (old_loc or "").strip() == new_loc.strip():
        return await cb.message.edit_text(escape_md("ℹ️ Выбрана та же локация — изменений нет"))

    try:
        WRITE_BUFFER.enqueue_location_update(row_idx, new_loc)
        ok = True
    except Exception as e:
        logger.exception("change_loc: enqueue failed for @%s → %s: %s", tag, new_loc, e)
        ok = False

    if not ok:
        return await cb.message.edit_text(escape_md("❌ Не удалось обновить локацию в 'Ответах'"))

    # кеш: -1 у новой, +1 у старой (если была в кеше)
    LOC_CACHE[new_loc]["left"] = max(0, left - 1)
    if old_loc and old_loc in LOC_CACHE and old_loc != new_loc:
        LOC_CACHE[old_loc]["left"] = int(LOC_CACHE[old_loc].get("left", 0) or 0) + 1

    # уведомление пользователю
    try:
        today = _today_str()
        await _notify_user(tag, f"ℹ️ Ваша локация на {today} изменена: {old_loc or '—'} → {new_loc}")
    except Exception as e:
        logger.warning("change_loc: DM failed for @%s: %s", tag, e)

    await cb.message.edit_text(escape_md(f"✅ Локация @{tag} изменена:\n{old_loc or '—'} → {new_loc}"))
async def _fio_by_username(username: str, fallback_fullname: str = "") -> str:
    """
    Вернуть ФИО по tg_username через URL_GET_USER_ID_BY_FIO.
    Если API не вернул ФИО — используем fallback_fullname.
    """
    uname = (username or "").strip().lstrip("@")
    if not uname:
        return (fallback_fullname or "").strip()
    try:
        r = await asyncio.to_thread(
            requests.get,
            URL_GET_USER_ID_BY_FIO,
            params = {"tg_username": uname},
            timeout = 7)
        r.raise_for_status()
        return _get_fio_from_api_payload(r.json() or {}) or (fallback_fullname or "").strip()
    except Exception as e:
        logger.warning("_fio_by_username: ошибка API для @%s: %s", uname, e)
        return (fallback_fullname or "").strip()

async def _notify_user(username: str, text: str) -> bool:
    """
    Уведомляет пользователя в ЛС по его tg_username (без @).
    1) Берёт chat_id через URL_GET_USER_ID_BY_FIO
    2) Отправляет сообщение
    Возвращает True, если отправлено.
    """
    uname = (username or "").strip().lstrip("@")
    if not uname:
        logger.warning("_notify_user: пустой username")
        return False

    try:
        r = await asyncio.to_thread(
            requests.get,
            URL_GET_USER_ID_BY_FIO,
            params = {"tg_username": uname},
            timeout = 7)
        r.raise_for_status()
        data = r.json() or {}
        raw_id = data.get("id") or data.get("chat_id")
        if raw_id is None:
            logger.warning("_notify_user: API не вернул id для @%s, payload=%s", uname, data)
            return False
        chat_id = int(raw_id)
    except Exception as e:
        logger.warning("_notify_user: ошибка запроса API для @%s: %s", uname, e)
        return False

    try:
        await bot.send_message(chat_id, escape_md(text))
        return True
    except Exception as e:
        logger.warning("_notify_user: не удалось отправить DM @%s (%s): %s", uname, chat_id, e)
        return False

def _available_locations_list() -> list[str]:
    return [addr for addr, meta in LOC_CACHE.items() if meta.get("left", 0) > 0]

def _today_str() -> str:
    return date.today().strftime("%d.%m.%Y")

# ─────────────────────────────────────────────────────────────────────────────
# 1) Команда админа: открыть запись
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_open_enroll(message: types.Message):
    """
    Открыть запись:
      1) читаем «Актуальные локации на смене» в кеш LOC_CACHE
      2) собираем BEST/GOOD за сегодня по листу «График исполнителей»
         (порог ratings берётся из 2-й строки колонки «Рейтинг»)
      3) рассылаем: BEST сразу, GOOD через 10 минут
    """
    global LOC_CACHE, SIGNUP_OPEN
    _purge_old_locks()

    # только админ в личке
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))

    # 1) загрузим актуальные локации на сегодня
    try:
        locs = await asyncio.to_thread(read_today_locations_with_capacity_and_district)
    except Exception as e:
        logger.exception("Не смогли прочитать локации: %s", e)
        return await message.answer(escape_md("❌ Ошибка чтения актуальных локаций"))

    if not locs:
        SIGNUP_OPEN = False
        return await message.answer(escape_md("❌ На сегодня нет актуальных локаций/мест"))

    LOC_CACHE = {addr: {"okrug": ok, "left": left} for addr, (ok, left) in locs.items()}

    total_slots = sum(meta["left"] for meta in LOC_CACHE.values())
    if total_slots <= 0:
        SIGNUP_OPEN = False
        return await message.answer(escape_md("❌ Свободных мест на сегодня нет"))

    SIGNUP_OPEN = True

    # Клавиатура «Записаться»
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("Записаться", callback_data="enroll:start_new"))

    text = escape_md("Запись на мойку открыта")

    # 2) Собираем получателей (BEST/GOOD) и текущий порог
    try:
        best_ids, good_ids, threshold = await asyncio.to_thread(
            fetch_best_and_good_ids_for_today_with_threshold)
    except Exception as e:
        logger.exception("Ошибка разбора 'График исполнителей': %s", e)
        return await message.answer(escape_md("❌ Не удалось прочитать лист «График исполнителей»"))

    # 3) Первая волна — BEST
    for uid in best_ids:
        try:
            await bot.send_message(uid, text, reply_markup=kb)
        except Exception as e:
            logger.warning("Не отправили BEST %s: %s", uid, e)

    # 4) Вторая волна — GOOD (через 10 минут)
    async def _delayed_broadcast():
        await asyncio.sleep(600)
        for uid in good_ids:
            try:
                await bot.send_message(uid, text, reply_markup=kb)
            except Exception as e:
                logger.warning("Не отправили GOOD %s: %s", uid, e)

    asyncio.create_task(_delayed_broadcast())

    # Ответ админу — что именно доступно по локациям + текущий порог
    details = "\n".join([f"• {addr} — {meta['left']} (округ: {meta.get('okrug', '')})"
                         for addr, meta in LOC_CACHE.items()])
    await message.answer(escape_md(
        f"✅ Запись открыта на {_today_str()}.\n"
        f"Порог рейтинга: {threshold}\n"
        f"Доступно по локациям:\n{details}"
    ))
    # ─────────────────────────────────────────────────────────────
    #  Построение индексов на сегодня: tag(no@) -> row_idx в "Ответы"
    # и (опционально) fio -> tag из «График исполнителей»
    # ─────────────────────────────────────────────────────────────

    try:
        today_s = _today_str()
        gc = gspread.service_account("app/creds.json")
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_ANSWERS)
        ws_ans = sh.worksheet("Ответы")
        rows = await asyncio.to_thread(ws_ans.get_all_values)
        answers_row_by_tag: Dict[str, int] = {}
        if rows:
            header = [h.strip() for h in rows[0]]
            def col_idx(name: str, default=None):
                try: return header.index(name)
                except ValueError:return default
            i_fio = col_idx("ФИО", 1)
            i_tag = col_idx("Тег", 2)
            i_date = col_idx("Дата выхода", 3)
            i_stat = col_idx("Статус", 5)
            for r_idx, r in enumerate(rows[1:], start=2):
                if len(r) <= max(i_fio, i_tag, i_date, i_stat):
                    continue
                if (r[i_date] or "").strip() != today_s:
                    continue
                if (r[i_stat] or "").strip() != "Заполнен":
                    continue
                tag = (r[i_tag] or "").strip().lstrip("@").lower()
                if tag:
                    answers_row_by_tag[tag] = r_idx
        fio_tag_pairs = await asyncio.to_thread(get_performers_for_date, date.today())
        tag_by_fio = {}
        for fio, tag in fio_tag_pairs:
            t = (tag or "").strip()
            if t.startswith("@"): t = t[1:]
            if fio: tag_by_fio[fio.strip()] = t
        SIGNUP_CTX[today_s] = {
        "answers_row_by_tag": answers_row_by_tag,
        "tag_by_fio": tag_by_fio,
    }
    except Exception as e:
        logger.warning("SIGNUP_CTX build failed: %s", e)
# ─────────────────────────────────────────────────────────────────────────────
# 2) Пользователь нажал «Записаться»
# ─────────────────────────────────────────────────────────────────────────────
async def on_enroll_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()

    try:
        await cb.message.delete()
    except Exception:
        pass

    if _user_locked(cb.from_user.id):
        addr = _user_lock_addr(cb.from_user.id)
        return await cb.answer(escape_md(f"Повторная запись невозможна. Вы уже записаны на {addr}."), show_alert=True)

    if not SIGNUP_OPEN:
        return await cb.message.answer(escape_md("❌ Запись сейчас закрыта"))

    # Выдаём только те адреса, где ещё есть места
    locs = _available_locations_list()
    if not locs:
        return await cb.message.answer(escape_md("❌ Увы, свободных мест не осталось"))

    # В этом сценарии выбираем ОДИН адрес: при клике снимаем предыдущий выбор
    await state.finish()
    await state.update_data(picked_loc=None)

    kb = build_locations_kb(locs, set())  # галочки уже реализованы :contentReference[oaicite:3]{index=3}
    await cb.message.answer(escape_md("Выберите адрес:"), reply_markup=kb)
    await EnrollStates.WAIT_LOCATION.set()

# ─────────────────────────────────────────────────────────────────────────────
# 3) Тоггл адреса (делаем выбор одиночным)
# ─────────────────────────────────────────────────────────────────────────────
async def on_location_toggle(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()

    locs = _available_locations_list()
    try:
        idx = int(cb.data.split(":", 1)[1])
    except Exception:
        return

    # Кнопка могла устареть: индекс вне диапазона → локацию заняли
    if idx < 0 or idx >= len(locs):
        return await _show_free_locations(cb, "❌ Похоже, выбранную локацию только что заняли", state)

    chosen = locs[idx]
    # На всякий случай перепроверим остаток (если кеш успели изменить)
    left = int(LOC_CACHE.get(chosen, {}).get("left", 0) or 0)
    if left <= 0:
        return await _show_free_locations(cb, "❌ На выбранной локации мест уже нет", state)

    # дальше — как было: одиночный выбор
    data = await state.get_data()
    current = data.get("picked_loc")
    picked = None if current == chosen else chosen
    await state.update_data(picked_loc=picked)

    selected = {picked} if picked else set()
    kb = build_locations_kb(locs, selected)
    await cb.message.edit_reply_markup(reply_markup=kb)

# ─────────────────────────────────────────────────────────────────────────────
# 4) Кнопка «Готово» на экране адресов → подтверждение
# ─────────────────────────────────────────────────────────────────────────────
async def on_location_done(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    picked = data.get("picked_loc")
    if not picked:
        return await cb.message.answer(escape_md("Вы не выбрали адрес"))

    text = f"Проверьте данные:\n\n📍 Адрес: {picked}\n📅 Дата: {_today_str()}"
    await cb.message.edit_text(escape_md(text), reply_markup=build_confirmation_kb_fill())  # :contentReference[oaicite:4]{index=4}
    await EnrollStates.WAIT_CONFIRM.set()

# ─────────────────────────────────────────────────────────────────────────────
# 5) Подтверждение → проверка места → запись в 'Ответы' → уменьшить кеш
# ─────────────────────────────────────────────────────────────────────────────
async def on_confirm_yes(cb: types.CallbackQuery, state: FSMContext):
    global SIGNUP_OPEN, LOC_CACHE, SIGNUP_CTX

    if _user_locked(cb.from_user.id):
        addr = _user_lock_addr(cb.from_user.id)
        await cb.answer()
        return await cb.message.edit_text(escape_md(f"❌ Повторная запись невозможна.\nВы уже записаны сегодня на: {addr}"))

    await cb.answer()
    data = await state.get_data()
    picked = data.get("picked_loc")
    if not picked:
        return await cb.message.answer(escape_md("Не выбран адрес"))

    # Ещё раз проверим, что запись открыта и в кеше есть место
    if not SIGNUP_OPEN:
        return await cb.message.answer(escape_md("❌ Запись уже закрыта"))
    left = int(LOC_CACHE.get(picked, {}).get("left", 0) or 0)
    if left <= 0:
        return await _show_free_locations(cb, "❌ На выбранной локации мест уже нет", state)

    # Находим заранее рассчитанную строку "Ответов" по тегу
    tag = (cb.from_user.username or "").strip().lstrip("@").lower()
    today = _today_str()
    ctx = SIGNUP_CTX.get(today) or {}
    row_idx = (ctx.get("answers_row_by_tag") or {}).get(tag)

    if not row_idx:
        return await cb.message.edit_text(escape_md(
            "❌ Не найдена строка 'Ответов' на сегодня со статусом 'Заполнен' для вашего тега.\nПроверьте, что вы в графике на сегодня"))

    # Кладём задачу в буфер, отвечаем мгновенно
    WRITE_BUFFER.enqueue_location_update(row_idx, picked)

    # Уменьшаем кеш
    LOC_CACHE[picked]["left"] = max(0, left - 1)
    _lock_user(cb.from_user.id, picked)

    # Уведомление в топик о назначении при ручном подтверждении
    try:
        username = (cb.from_user.username or "").strip()
        fio = await asyncio.to_thread(_fio_from_table_by_username, username)
        tag_part = f" @{username}" if username else ""
        await _push_shift_line(f"🟢 {today} {fio}{tag_part} назначен на объект {picked}")
    except Exception as e:
        logger.warning("push manual assign failed: %s", e)

    # Если по всем адресам мест больше нет — закрываем запись
    if all(meta.get("left", 0) <= 0 for meta in LOC_CACHE.values()):
        SIGNUP_OPEN = False
        try:
            report = await asyncio.to_thread(_build_graph_distribution_report)
            await _push_graph_notify(report)
        except Exception as e:
            logger.warning("graph report build/send failed: %s", e)
        await cb.message.edit_text(
            escape_md(
                f"✅ Адрес «{picked}» подтверждён на {_today_str()}.\n\n🔒 Запись закрыта — мест больше нет."))
        await state.finish()
        return

    # Иначе просто подтверждаем
    await cb.message.edit_text(
        escape_md(f"✅ Адрес «{picked}» подтверждён на {_today_str()}")
    )
    await state.finish()

# ─────────────────────────────────────────────────────────────────────────────
# Команда админа: автораспределение по кешу (остатки после open_enroll)
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_auto_distribute_cached(message: types.Message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))

    if not LOC_CACHE:
        return await message.answer(escape_md("ℹ️ Кеш пуст. Сначала выполните /open_enroll"))

    # берём только адреса с left > 0
    from collections import defaultdict, deque
    today = _today_str()

    # okrug -> очередь адресов (каждый адрес повторяем left-раз)
    by_ok = defaultdict(deque)
    pool_all = deque()
    for addr, meta in LOC_CACHE.items():
        ok = meta.get("okrug", "")
        left = int(meta.get("left", 0) or 0)
        for _ in range(max(0, left)):
            by_ok[ok].append(addr)
            pool_all.append(addr)

    if not pool_all:
        return await message.answer(escape_md("ℹ️ Свободных мест в кеше больше нет"))

    # Исполнители «1» на сегодня
    performers = await asyncio.to_thread(performers_for_today_flag1_with_district)
    if not performers:
        return await message.answer(escape_md("ℹ️ Нет исполнителей с отметкой '1' на сегодня"))

    assigned = []
    skipped  = []

    # 1) по совпадению округов
    rest = []
    for fio, username, ok in performers:
        dq = by_ok.get(ok)
        addr = None
        while dq and addr is None:
            candidate = dq.popleft()
            # попытка обновить запись «Ответы»
            try:
                ok_update = await asyncio.to_thread(update_location_for_user_today_by_tag, username, candidate)
            except Exception as e:
                logger.warning("Ошибка update_location для @%s на %s: %s", username, candidate, e)
                ok_update = False

            if ok_update:
                addr = candidate
                assigned.append((fio, username, addr, "округ"))
                # уменьшаем кеш
                LOC_CACHE[addr]["left"] = max(0, LOC_CACHE[addr]["left"] - 1)
                # выкидываем одно вхождение из общего пула
                try:
                    pool_all.remove(addr)
                except ValueError:
                    pass
                # уведомляем
                await _notify_user(username, f"✅ Вас записали на {today} по адресу: {addr}")
        if addr is None:
            rest.append((fio, username, ok))

    # 2) остаточный принцип
    for fio, username, _ in rest:
        # чистим пул от адресов, где left уже 0
        while pool_all and LOC_CACHE[pool_all[0]]["left"] <= 0:
            pool_all.popleft()
        if not pool_all:
            skipped.append((fio, username, "мест больше нет"))
            continue

        addr = pool_all.popleft()
        try:
            ok_update = await asyncio.to_thread(update_location_for_user_today_by_tag, username, addr)
        except Exception as e:
            logger.warning("Ошибка update_location (остаток) для @%s на %s: %s", username, addr, e)
            ok_update = False

        if ok_update:
            assigned.append((fio, username, addr, "остаток"))
            LOC_CACHE[addr]["left"] = max(0, LOC_CACHE[addr]["left"] - 1)
            await _notify_user(username, f"✅ Вас записали на {today} по адресу: {addr}")
            try:
                await _push_shift_line(f"- {today} {fio} назначен на объект {addr}")
            except Exception as e:
                logger.warning("push okrug assign failed: %s", e)
        else:
            skipped.append((fio, username, "не найдена запись 'Ответы'"))

    # 3) отчёт админу
    lines_ok  = [f"• {fio} (@{u}) → {a} ({why})" for fio, u, a, why in assigned]
    lines_bad = [f"• {fio} (@{u}) — {why}" for fio, u, why in skipped]
    report = ["Автораспределение (из кеша) на " + today]
    if lines_ok:  report += ["\n✅ Назначены:", *lines_ok]
    if lines_bad: report += ["\n⚠️ Пропущены:", *lines_bad]
    # осталось по адресам
    rest_lines = [f"• {addr}: {meta['left']} (округ: {meta.get('okrug','')})"
                  for addr, meta in LOC_CACHE.items()]
    report += ["\nОстатки в кеше:", *rest_lines]
    await message.answer(escape_md("\n".join(report)))

    try:
        if assigned and len(assigned) == len(performers):
            report = await asyncio.to_thread(_build_graph_distribution_report)
            await _push_graph_notify(report)
    except Exception as e:
        logger.warning("push final schedule failed: %s", e)

async def _push_graph_notify(text: str) -> None:
    """
    Отправить уведомление в чат/топик графика (CHAT_ID_GRAPH, THREAD_ID_GRAPH).
    Текст экранируется под MarkdownV2.
    """
    try:
        await bot.send_message(
            CHAT_ID_GRAPH,
            escape_md(text),
            message_thread_id=THREAD_ID_GRAPH
        )
    except Exception as e:
        logger.warning("_push_graph_notify: не удалось отправить: %s", e)

def _build_graph_distribution_report() -> str:
    """
    Строит текст вида:
    Доброго вечера!
    Распределение на DD.MM:

    <Адрес>
    <ФИО> @tag
    ...

    + служебный блок внизу.
    """
    from datetime import date
    today = date.today()
    # 1) назначения из «Ответы»: [(fio, address), ...]
    assignments = _read_assignments_for_date(today)  # [(ФИО, Локация)]
    # 2) соответствия ФИО → тег из «График исполнителей»
    fio_tag_pairs = get_performers_for_date(today)
    tag_by_fio = {}
    for fio, tag in fio_tag_pairs:
        t = (tag or "").strip()
        if t.startswith("@"):
            t = t[1:]
        tag_by_fio[fio.strip()] = t

    # 3) группируем по адресу
    by_addr: dict[str, list[tuple[str, str]]] = {}
    for fio, addr in assignments:
        fio = (fio or "").strip()
        addr = (addr or "").strip()
        if not fio:
            continue
        bad_addr = (not addr) or addr in {"1", "0", "-", "."} or addr.isdigit()
        if bad_addr:
            continue
        tag = tag_by_fio.get(fio, "")
        by_addr.setdefault(addr, []).append((fio, tag))

    # 4) собираем текст
    header = [
        "Доброго вечера!",
        f"Распределение на {today.strftime('%d.%m')}:",
        ""
    ]
    body: list[str] = []
    for addr in sorted(by_addr.keys(), key=str.lower):
        link = TRUCK_LOCATIONS.get(addr)
        suffix = " 🚚" if link else ""
        if link:
            addr_fmt = f"🏠 [{escape_md(addr)}]({link}) {suffix}"
        else:
            addr_fmt = f"🏠 {escape_md(addr)}"
        body.append(addr_fmt)
        for fio, tag in by_addr[addr]:
            tag_fmt = f" @{tag}" if tag else ""
            body.append(f"{fio}{tag_fmt}")
        body.append("")

    _teg, _fi = find_logistics_rows()
    if not _fi:
        logist = ""
    else:
        logist = " , ".join(f"{name} ({teg})" for name, teg in zip(_fi, _teg))
    footer = [
        f"👨‍💻 - {logist}",
        "🤖 - [Бот @KK_Washing_bot](https://t.me/KK_Washing_bot)",
        "🧹 - [Химчистка BelkaCar<>CleanCar](https://t.me/+08rvaj4Fxvg1N2Ri)",
        "🚚 - [Sollers Atlant, Ford Transit](https://yandex.ru/maps/-/CHWKQAJQ)",
    ]
    # Убираем лишние пустые строки по краям
    while body and not body[-1].strip():
        body.pop()
    return "\n".join(header + body + [""] + footer)


def _fio_from_table_by_username(username: str) -> str:
    """Вернёт ФИО из листа 'График исполнителей' по tg_username (без @)."""

    uname = (username or "").strip().lstrip("@")
    if not uname:
        return ""

    gc = gspread.service_account("app/creds.json")
    sh = gc.open_by_url(URL_GOOGLE_SHEETS_ANSWERS)
    ws = sh.worksheet("График исполнителей")

    data = ws.get_all_values()
    if not data:
        return ""

    headers = [h.strip() for h in data[0]]
    try:
        idx_fio = headers.index("Исполнители")
        idx_tag = headers.index("Тег")
    except ValueError:
        return ""

    for row in data[2:]:  # начиная с 3-й строки
        fio = (row[idx_fio] if len(row) > idx_fio else "").strip()
        tag = (row[idx_tag] if len(row) > idx_tag else "").strip().lstrip("@")
        if tag.lower() == uname.lower():
            return fio
    return ""

async def cmd_graph_report(message: types.Message):
    """
    Ручная отправка полного отчёта распределения за сегодня в чат графика.
    Доступна только админу в личке.
    """
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))
    try:
        report = await asyncio.to_thread(_build_graph_distribution_report)
        if not report.strip():
            return await message.answer(escape_md("ℹ️ Отчёт пуст — нет назначений на сегодня"))
        await _push_graph_notify(report)
        await message.answer(escape_md("✅ Полный отчёт отправлен в чат графика"))
    except Exception as e:
        logger.warning("cmd_graph_report failed: %s", e)
        await message.answer(escape_md("❌ Ошибка формирования/отправки отчёта"))

def _build_admin_locations_kb(locs: list[str]) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for i, addr in enumerate(locs):
        left = LOC_CACHE.get(addr, {}).get("left", 0)
        kb.add(types.InlineKeyboardButton(f"{addr} — {left}", callback_data=f"adm_loc:{i}"))
    return kb

async def cmd_assign_cached(message: types.Message, state: FSMContext):
    """Админ выбирает локацию из кеша, затем вводит тег исполнителя (без @)."""
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))
    if not LOC_CACHE:
        return await message.answer(escape_md("ℹ️ Кеш пуст. Сначала выполните /open_enroll"))
    locs = _available_locations_list()
    if not locs:
        return await message.answer(escape_md("ℹ️ Во всех локациях места закончились"))
    await state.finish()
    await state.update_data(picked_loc=None)
    await message.answer(escape_md("Выберите локацию для назначения:"), reply_markup=_build_admin_locations_kb(locs))
    await AdminAssignStates.WAIT_LOC.set()

async def on_admin_pick_loc(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    locs = _available_locations_list()
    try:
        idx = int(cb.data.split(":", 1)[1])
    except Exception:
        return
    if idx < 0 or idx >= len(locs):
        return
    picked = locs[idx]
    await state.update_data(picked_loc=picked)
    await cb.message.edit_text(escape_md(f"📍 Локация: {picked}\n\nВведите тег исполнителя без '@':"))
    await AdminAssignStates.WAIT_TAG.set()

async def on_admin_enter_tag(message: types.Message, state: FSMContext):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))
    data = await state.get_data()
    picked = data.get("picked_loc")
    if not picked:
        await state.finish()
        return await message.answer(escape_md("Не выбрана локация. Повторите /assign_cached"))
    tag = (message.text or "").strip().lstrip("@")
    if not tag:
        return await message.answer(escape_md("Введите непустой тег без '@'"))

    # проверяем наличие места в кеше прямо сейчас
    left = int(LOC_CACHE.get(picked, {}).get("left", 0) or 0)
    if left <= 0:
        await state.finish()
        return await message.answer(escape_md("❌ На выбранной локации мест больше нет"))

    # обновляем «Ответы» по тегу (сегодня, статус 'Заполнен') → поле 'Локация'
    try:
        ok = await asyncio.to_thread(update_location_for_user_today_by_tag, tag, picked)
    except Exception as e:
        logger.exception("cmd_assign_cached: ошибка обновления 'Ответы' для @%s: %s", tag, e)
        ok = False

    if not ok:
        return await message.answer(escape_md("❌ Не найден исполнитель на сегодня со статусом 'Заполнен' для указанного тега"))

    # уменьшаем кеш
    LOC_CACHE[picked]["left"] = max(0, left - 1)

    # уведомляем исполнителя и пишем в чат смен
    today = _today_str()
    try:
        await _notify_user(tag, f"✅ Вас записали на {today} по адресу: {picked}")
    except Exception as e:
        logger.warning("cmd_assign_cached: notify failed for @%s: %s", tag, e)
    try:
        fio = await asyncio.to_thread(_fio_from_table_by_username, tag)
        tag_part = f" @{tag}" if tag else ""
        await _push_shift_line(f"🟢 {today} {fio}{tag_part} назначен на объект {picked}")
    except Exception as e:
        logger.warning("cmd_assign_cached: _push_shift_line failed: %s", e)

    # ответ админу + остатки
    rest_lines = [f"• {addr}: {meta['left']} (округ: {meta.get('okrug','')})"
                  for addr, meta in LOC_CACHE.items()]
    await message.answer(escape_md(
        "✅ Назначение выполнено.\n\n"
        f"📍 {picked}\n"
        f"👤 @{tag}\n\n"
        "Остатки по локациям:\n" + "\n".join(rest_lines)
    ))

    # если везде 0 — можно при желании сформировать финальный отчёт (не обязательно)
    if all(int(meta.get("left", 0) or 0) <= 0 for meta in LOC_CACHE.values()):
        try:
            report = _build_graph_distribution_report()
            await _push_graph_notify(report)
        except Exception as e:
            logger.warning("cmd_assign_cached: final graph report failed: %s", e)
    await state.finish()

async def cmd_graph_notify(message: types.Message):
    """
    Отдельная команда для оповещения в чат графика:
    "Через 10 минут начнётся запись на мойку"
    """
    if message.chat.type != "private" or message.from_user.id not in ADMIN_ID:
        return await message.answer(escape_md("Доступ запрещён"))

    try:
        await _push_graph_notify("🔔Через 10 минут начнётся запись на мойку🔔")
        await message.answer(escape_md("✅ Уведомление отправлено"))
    except Exception as e:
        logger.warning("cmd_graph_notify failed: %s", e)
        await message.answer(escape_md("❌ Ошибка отправки уведомления"))
async def on_confirm_fix(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    await _show_free_locations(cb, "Ок, исправим. Выберите адрес ещё раз:", state)

def register_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_open_enroll, commands=["open_enroll"], state="*")
    dp.register_message_handler(cmd_auto_distribute_cached, commands=["auto_distribute"], state="*")
    dp.register_message_handler(cmd_graph_notify, commands=["graph_notify"], state="*")
    dp.register_message_handler(cmd_graph_report, commands=["graph_report"], state="*")
    dp.register_message_handler(cmd_assign_cached, commands=["assign_cached"], state="*")
    dp.register_message_handler(cmd_change_loc, commands=["change_loc"], state="*")
    dp.register_callback_query_handler(on_admin_pick_loc, lambda c: c.data.startswith("adm_loc:"),state=AdminAssignStates.WAIT_LOC)
    dp.register_message_handler(on_admin_enter_tag, state=AdminAssignStates.WAIT_TAG)
    dp.register_callback_query_handler(on_enroll_start,  lambda c: c.data == "enroll:start_new", state="*")
    dp.register_callback_query_handler(on_location_toggle, lambda c: c.data.startswith("select_loc:"), state=EnrollStates.WAIT_LOCATION)
    dp.register_callback_query_handler(on_location_done,  lambda c: c.data == "exit_loc", state=EnrollStates.WAIT_LOCATION)
    dp.register_callback_query_handler(on_confirm_yes,    lambda c: c.data == "confirm:yes", state=EnrollStates.WAIT_CONFIRM)
    FIX_CB = {"confirm:fix", "confirm:error", "confirm:err", "confirm:back", "exit"}
    dp.register_callback_query_handler(on_confirm_fix, lambda c: c.data in FIX_CB, state="*")
    dp.register_callback_query_handler(on_chg_loc_pick, lambda c: c.data.startswith("chg_loc:pick:"), state="*")
    dp.register_callback_query_handler(on_chg_loc_select, lambda c: c.data.startswith("chg_loc:sel:"), state="*")
    dp.register_callback_query_handler(on_chg_loc_confirm, lambda c: c.data.startswith("chg_loc:ok:"), state="*")
