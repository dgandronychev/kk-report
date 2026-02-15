# app/utils/http.py
from __future__ import annotations

from typing import Optional

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from app.config import HTTP_PORT
from app.utils.max_api import send_image, send_text

app = FastAPI()


class NotifyIn(BaseModel):
    chat_id: int
    text: str


@app.post("/notify")
async def notify(payload: NotifyIn):
    try:
        await send_text(payload.chat_id, payload.text)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/notify_image")
async def notify_image(
    chat_id: int = Form(...),
    text: Optional[str] = Form(None),
    file: UploadFile = File(...),
):
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty file")

        await send_image(
            chat_id=chat_id,
            file_bytes=content,
            filename=file.filename or "image.jpg",
            caption=text,
        )
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"ok": True}


def run_http():
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
