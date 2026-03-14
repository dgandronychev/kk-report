# app/utils/write_buffer.py
from __future__ import annotations
import threading, time, logging
from queue import Queue, Empty
from dataclasses import dataclass
from typing import List, Optional, Dict

import gspread
from gspread.utils import rowcol_to_a1
from app.config import URL_GOOGLE_SHEETS_ANSWERS

log = logging.getLogger(__name__)

@dataclass
class UpdateTask:
    row_idx: Optional[int] = None            # строка в "Ответы" (1-based)
    new_location: Optional[str] = None       # новое значение в колонке "Локация" (E)
    append_row: Optional[list] = None        # если нужно добавлять новые строки

class SheetsWriteBuffer:
    """Копит обновления и раз в N секунд отправляет их одной пачкой."""
    def __init__(self, creds_path: str = "app/creds.json",
                 worksheet_title: str = "Ответы", flush_interval: int = 60):
        self._q: "Queue[UpdateTask]" = Queue()
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, daemon=True)
        self._creds_path = creds_path
        self._ws_title = worksheet_title
        self._flush_interval = flush_interval

    # ── Публичное API ─────────────────────────────────────────────
    def start(self) -> None:
        if not self._thr.is_alive():
            self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        self._thr.join(timeout=5)
        try:
            self._flush(force=True)
        except Exception:
            log.exception("Final flush failed")

    def put(self, task: UpdateTask) -> None:
        self._q.put(task)

    def enqueue_location_update(self, row_idx: int, new_location: str) -> None:
        self.put(UpdateTask(row_idx=row_idx, new_location=new_location))

    # ── Внутреннее: поток и сброс ────────────────────────────────
    def _run(self) -> None:
        # НИЧЕГО не забираем из очереди здесь — только таймер → _flush
        next_at = time.time() + self._flush_interval
        while not self._stop.is_set():
            time.sleep(0.2)
            if time.time() >= next_at:
                try:
                    self._flush()
                except Exception:
                    log.exception("Periodic flush failed")
                next_at = time.time() + self._flush_interval

    def _drain_queue(self) -> List[UpdateTask]:
        tasks: List[UpdateTask] = []
        while True:
            try:
                tasks.append(self._q.get_nowait())
            except Empty:
                break
        return tasks

    def _flush(self, force: bool = False) -> None:
        tasks = self._drain_queue()
        if not tasks and not force:
            return
        if not tasks and force:
            return

        # один коннект на пачку
        gc = gspread.service_account(filename=self._creds_path)
        sh = gc.open_by_url(URL_GOOGLE_SHEETS_ANSWERS)
        ws = sh.worksheet(self._ws_title)

        # 1) схлопываем обновления по строкам (последнее значение выигрывает)
        updates_by_row: Dict[int, str] = {}
        appends: List[list] = []
        for t in tasks:
            if t.row_idx and t.new_location is not None:
                updates_by_row[t.row_idx] = t.new_location
            if t.append_row:
                appends.append(t.append_row)

        # 2) пакетная запись значений (E-колонка «Локация»)
        if updates_by_row:
            value_ranges = []
            for row_idx, loc in updates_by_row.items():
                a1 = rowcol_to_a1(row_idx, 5)  # "E{row}"
                value_ranges.append({"range": a1, "values": [[loc]]})
            # ВАЖНО: batch_update по значениям, а не структурный Spreadsheet.batch_update
            ws.batch_update(value_ranges, value_input_option="USER_ENTERED")

        # 3) добавления (если используете)
        for row in appends:
            ws.append_row(row, value_input_option="USER_ENTERED")
