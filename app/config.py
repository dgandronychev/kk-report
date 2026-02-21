# app/config.py
import os

MAX_TOKEN = os.getenv("MAX_TOKEN", "").strip()

if not MAX_TOKEN:
    raise RuntimeError("MAX_TOKEN environment variable is not set")

HEADERS = {"Authorization": MAX_TOKEN}

API_BASE = os.environ.get("MAX_API_BASE", "https://platform-api.max.ru").rstrip("/")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))

LOGS_DIR = "logs"
URL_REGISTRASHION = "https://stage.app.clean-car.net/api/v2/bots/max_information/update/"
URL_GET_FIO = "https://stage.app.clean-car.net/api/v2/bots/accident/retrieve/"
URL_GET_INFO_TASK = "https://stage.app.clean-car.net/api/v2/bots/open_tasks/list/"

LOGISTICS_CHAT_IDS = "-70635401257012,-70646361948407,-70637799415860"
REPORT_CHAT_ID = "199909595"
WORK_SHIFT_CHAT_ID ="199909595"
DAMAGE_CHAT_ID_CITY = "199909595"
DAMAGE_CHAT_ID_YANDEX = "199909595"
DAMAGE_CHAT_ID_BELKA = "199909595"
SBORKA_CHAT_ID_CITY = "199909595"
SBORKA_CHAT_ID_YANDEX = "199909595"
SBORKA_CHAT_ID_BELKA = "199909595"

URL_GOOGLE_SHEETS_CHART = "https://docs.google.com/spreadsheets/d/15Kw7bweFKg3Dp0INeA47eki1cuPIgtVgk_o_Ul3LGyM/edit?gid=1647640846#gid=1647640846"
GSPREAD_URL_MAIN = "https://docs.google.com/spreadsheets/d/1Rk_9eyjx0u5dUGnz84-6GCshd1zLLWOR-QGPZtQTMKg/edit?gid=2129054551#gid=2129054551"
GSPREAD_URL_ANSWER = "https://docs.google.com/spreadsheets/d/1p044-xtk5TxFOsPZ9l_kbthc53toqdh3kRXrGbU5Iao/edit?gid=0#gid=0"
GSPREAD_URL_SKLAD = "https://docs.google.com/spreadsheets/d/1X-2KWPrGeDPyRj9WrzyrdGA2knxjQf9k1XeAWrGQWvg/edit?gid=0#gid=0"
GSPREAD_URL_GATES = "https://docs.google.com/spreadsheets/d/1BH7HDYBS6E-nSoq3ZBQljhA74aAEe8QIOviwOPpAyX4/edit?gid=0#gid=0"
GOOGLE_SHEETS_SHIFT = "https://docs.google.com/spreadsheets/d/1lXmm7IzvT6oBhGf62aOU1rlcmGRbyQxfBTTAjUtF7PQ/edit?gid=0#gid=0"

GOOGLE_DRIVE_CREDS_JSON = os.environ.get("GOOGLE_DRIVE_CREDS_JSON", "app/creds.json")
GOOGLE_DRIVE_DAMAGE_BELKA_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_DAMAGE_BELKA_FOLDER_ID", "1EzE_RQBt8-tkbPstIll_KfdOUAwY3564")

WELCOME_TEXT = (
    "Здравствуйте, это бот компании КлинКар\n\n"
    "Для начала работы пройдите регистрацию по команде /registration"
)


POR_NOMER_DIS = int(os.environ.get("POR_NOMER_DIS", "13"))
POR_NOMER_REZ = int(os.environ.get("POR_NOMER_REZ", "16"))

def _parse_user_ids_csv(value: str) -> set[int]:
    out: set[int] = set()
    for part in (value or "").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out

NOMENCLATURE_ALLOWED_USER_IDS = _parse_user_ids_csv("199909595")