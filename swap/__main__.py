# __main__.py
import asyncio
import logging
import calendar
from zoneinfo import ZoneInfo
from pathlib import Path
from datetime import datetime, timedelta, time, date
from app.bot import dp, bot
from app.config import LOGS_DIR
from app.config import CHAT_ID_GRAPH, THREAD_ID_GRAPH
#
from app.utils.status_smen import (
    send_initial_shift_status,
    handle_shift_updates,
    send_daily_shift_reminder,
    send_count_target_message,
)

logger = logging.getLogger(__name__)

MOSCOW = ZoneInfo("Europe/Moscow")

async def scheduler_fill_schedule():
    """Шлёт сообщение строго в 19:00 Мск 6–10 и 21–25 числа каждого месяца."""
    tz = ZoneInfo("Europe/Moscow")
    send_days = sorted(set(range(6, 11)) | set(range(21, 26)))

    while True:
        try:
            now = datetime.now(tz)

            def build_candidates(year: int, month: int):
                cands = []
                for day in send_days:
                    try:
                        cands.append(datetime(year, month, day, 19, 0, 0, tzinfo=tz))
                    except ValueError:
                        # на случай некорректной даты (в целом тут не должно быть, но пусть будет безопасно)
                        continue
                return cands

            # Кандидаты в текущем месяце строго позже текущего времени
            cands = [dt for dt in build_candidates(now.year, now.month) if dt >= now]

            if not cands:
                # Переходим к следующему месяцу
                nm = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
                cands = build_candidates(nm.year, nm.month)

                # Теоретически список может быть пустым только при аномалиях,
                # но на всякий случай:
                cands = [dt for dt in cands if dt > now] or cands

            target = min(cands)

            delay = max(0.0, (target - now).total_seconds())
            logging.info(f"[fill_schedule] next run at {target.isoformat()} (in {int(delay)}s)")
            await asyncio.sleep(delay)

            await bot.send_message(
                CHAT_ID_GRAPH,
                "⏰ Заполните индивидуальное расписание предоставления услуг на следующий период",
                message_thread_id=THREAD_ID_GRAPH
            )
            logging.info("[fill_schedule] sent")

            # Важно: после отправки не даём циклу тут же пересчитать тот же target на границе секунды
            await asyncio.sleep(1)

        except Exception:
            logging.exception("scheduler_fill_schedule crashed, restarting loop")
            await asyncio.sleep(5)


async def scheduler_shift_status():
    """Запускает send_initial_shift_status() каждый день в 21:00 и порождает задачу обновления."""
    while True:
        now = datetime.now(MOSCOW)
        target = datetime.combine(now.date(), time(21, 0), tzinfo=MOSCOW)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        # шлём первое сообщение и запоминаем его message_id
        msg_id = await send_initial_shift_status()
        # запускаем его «жизненный цикл» обновлений
        asyncio.create_task(handle_shift_updates(msg_id, target.date()))

async def scheduler_shift_count_target():
    """
    Ночное окно одной смены:
      шлём отчёт в 22:00 и 23:00 ТЕКУЩЕГО дня,
      а также в 00:00, 01:00, 02:00, 03:00, 04:00, 05:00, 06:00, 07:00 ПРЕДЫДУЩЕГО дня.
    Любой сбой логируется; планировщик продолжает работу.
    """
    run_times = (
        time(22, 0), time(22, 30), time(23, 0),
        time(0, 0), time(1, 0), time(2, 0), time(3, 0),
        time(4, 0), time(5, 0), time(6, 0), time(7, 0),
        time(8, 0),
    )

    def _report_date_for_slot(target_dt: datetime) -> date:
        return (target_dt.date() if target_dt.hour >= 22
                else (target_dt.date() - timedelta(days=1)))

    while True:
        now = datetime.now(MOSCOW)

        # кандидаты ещё сегодня
        today_targets = [
            datetime.combine(now.date(), t, tzinfo=MOSCOW)
            for t in run_times
            if datetime.combine(now.date(), t, tzinfo=MOSCOW) > now
        ]

        if today_targets:
            next_targets = today_targets
        else:
            # на завтра берём весь набор слотов
            next_day = (now + timedelta(days=1)).date()
            next_targets = [
                datetime.combine(next_day, t, tzinfo=MOSCOW) for t in run_times
            ]

        target = min(next_targets)
        delay = max(0, (target - now).total_seconds())
        logging.info(f"[count_target] next run at {target.isoformat()} (in {int(delay)}s)")

        try:
            await asyncio.sleep(delay)
        except Exception:
            logging.exception("[count_target] sleep interrupted")
            continue

        try:
            report_date = _report_date_for_slot(target)
            msg_id = await send_count_target_message(for_date=report_date)
            logging.info(f"[count_target] sent for {report_date} (message_id={msg_id})")
        except Exception:
            # не падаем, идём к следующему запланированному часу
            logging.exception(f"[count_target] failed to send for planned slot {target}")
            continue

async def _safe_send_daily_shift_reminder():
    try:
        await send_daily_shift_reminder()
    except Exception:
        logger.exception("send_daily_shift_reminder crashed")


async def scheduler_daily_reminder():
    while True:
        now = datetime.now(MOSCOW)
        target = datetime.combine(now.date(), time(13,0), tzinfo=MOSCOW)
        if target <= now:
            target += timedelta(days=1)

        await asyncio.sleep((target - now).total_seconds())

        asyncio.create_task(_safe_send_daily_shift_reminder())


async def main():
    # Настраиваем логирование в файл
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    log_name = f'{LOGS_DIR}/{datetime.now().strftime("%Y-%m-%d")}.log'
    file_handler = logging.FileHandler(log_name, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    asyncio.create_task(scheduler_fill_schedule())
    # ежедневный «график смен»
    asyncio.create_task(scheduler_shift_status())
    asyncio.create_task(scheduler_shift_count_target())
    asyncio.create_task(scheduler_daily_reminder())
    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())
