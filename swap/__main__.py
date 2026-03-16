# __main__.py

import asyncio
import logging
from pathlib import Path
from datetime import datetime
from app.bot import dp
from app.config import LOGS_DIR
from app.utils.gsheets import load_extra_wash_locations

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

    load_extra_wash_locations()

    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())
