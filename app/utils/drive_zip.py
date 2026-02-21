from __future__ import annotations

import importlib.util
import json
import os
import logging
import tempfile
import zipfile
from datetime import datetime
from typing import Any, Iterable

import httpx

logger = logging.getLogger(__name__)

def is_google_drive_upload_available() -> bool:
    return importlib.util.find_spec("googleapiclient") is not None

def safe_zip_name(grz: str) -> str:
    grz = (grz or "").strip().upper()
    if not grz or grz in ("Б/Н", "БН"):
        grz = "NONAME"
    date_s = datetime.now().strftime("%Y-%m-%d")
    return f"{date_s}_{grz}.zip"


def _drive_service(creds_json_path: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(
        creds_json_path, scopes=scopes
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _iter_urls(payload: Any) -> Iterable[str]:
    if isinstance(payload, dict):
        for key in ("url", "download_url", "src", "href"):
            value = payload.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                yield value
        for value in payload.values():
            yield from _iter_urls(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _iter_urls(value)


async def _download_bytes_from_payload(payload: Any) -> bytes | None:
    urls = list(dict.fromkeys(_iter_urls(payload)))
    if not urls:
        return None

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for url in urls:
            try:
                response = await client.get(url)
                if response.is_success and response.content:
                    return response.content
            except Exception:
                continue
    return None


async def build_zip_from_max_attachments(files: list[dict]) -> str:
    tmp_dir = tempfile.mkdtemp(prefix="maxzip_")
    zip_path = os.path.join(tmp_dir, "bundle.zip")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, item in enumerate(files, start=1):
            f_type = str(item.get("type") or "file")
            payload = item.get("payload")

            content = await _download_bytes_from_payload(payload)
            if content:
                ext = {
                    "image": "jpg",
                    "video": "mp4",
                    "audio": "mp3",
                    "file": "bin",
                }.get(f_type, "bin")
                zf.writestr(f"{idx:02d}_{f_type}.{ext}", content)
            else:
                zf.writestr(
                    f"{idx:02d}_{f_type}.json",
                    json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                )

    return zip_path


def upload_zip_private(zip_path, zip_name, folder_id, creds_json_path):
    try:
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError
    except ModuleNotFoundError:
        logger.warning("[drive_zip] google-api-python-client is not installed, zip upload skipped")
        return ""


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
