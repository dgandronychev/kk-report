# app/config.py
import os

MAX_TOKEN = os.getenv("MAX_TOKEN", "").strip()

if not MAX_TOKEN:
    raise RuntimeError("MAX_TOKEN environment variable is not set")

HEADERS = {"Authorization": MAX_TOKEN}

API_BASE = os.environ.get("MAX_API_BASE", "https://platform-api.max.ru").rstrip("/")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))

LOGS_DIR = "logs"
URL_REGISTRASHION = "https://stage.app.clean-car.net/api/v1/bots/max_information/update/"
URL_GET_FIO = "https://stage.app.clean-car.net/api/v1/bots/accident/retrieve/",

LOGISTICS_CHAT_IDS = "199909595,199909595"
REPORT_CHAT_ID = "199909595"
WORK_SHIFT_CHAT_ID ="199909595"

URL_GOOGLE_SHEETS_CHART = "https://docs.google.com/spreadsheets/d/15Kw7bweFKg3Dp0INeA47eki1cuPIgtVgk_o_Ul3LGyM/edit?gid=1647640846#gid=1647640846"
WELCOME_TEXT = (
    "Здравствуйте, это бот компании КлинКар\n\n"
    "Для начала работы пройдите регистрацию по команде /registration"
)