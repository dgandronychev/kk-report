import os
import zipfile
import tempfile
import json
from datetime import datetime
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

def safe_zip_name(grz: str) -> str:
    grz = (grz or "").strip().upper()
    if not grz or grz in ("Б/Н", "БН"):
        grz = "NONAME"
    date_s = datetime.now().strftime("%Y-%m-%d")
    return f"{date_s}_{grz}.zip"


def _drive_service(creds_json_path: str):
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(
        creds_json_path, scopes=scopes
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


async def build_zip_from_tg_files(bot, files: List[Dict]) -> str:
    tmp_dir = tempfile.mkdtemp(prefix="tgzip_")
    zip_path = os.path.join(tmp_dir, "bundle.zip")

    def ext_by_type(t: str) -> str:
        return {"photo": "jpg", "video": "mp4", "document": "bin"}.get(t, "bin")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, item in enumerate(files, start=1):
            file_id = item.get("media")
            ftype = item.get("type", "document")

            tg_file = await bot.get_file(file_id)
            buf = await bot.download_file(tg_file.file_path)
            data = buf.read()

            zf.writestr(f"{idx:02d}_{ftype}.{ext_by_type(ftype)}", data)

    return zip_path

def upload_zip_private(zip_path, zip_name, folder_id, creds_json_path):
    drive = _drive_service(creds_json_path)
    meta = {"name": zip_name, "parents": [folder_id]}
    media = MediaFileUpload(zip_path, mimetype="application/zip", resumable=True)

    try:
        created = drive.files().create(
            body=meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute(num_retries=3)
        return created["id"]

    except HttpError as e:
        content = e.content.decode("utf-8", errors="replace") if hasattr(e, "content") else str(e)
        raise RuntimeError(f"Drive upload failed: status={e.status_code} content={content}") from e
