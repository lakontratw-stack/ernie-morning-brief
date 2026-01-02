import os
import requests
from fastapi import FastAPI, Request

app = FastAPI()

LINE_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()


def push_message(user_id: str, text: str):
    if not LINE_TOKEN:
        raise RuntimeError("LINE_CHANNEL_ACCESS_TOKEN is not set")

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": user_id,
        "messages": [{"type": "text", "text": text[:4900]}],
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/webhook")
async def webhook(req: Request):
    body = await req.json()
    events = body.get("events", [])

    for e in events:
        if e.get("type") == "follow":
            user_id = e["source"]["userId"]

            welcome_text = (
                "👋 歡迎加入 Ernie 早安 AI 日報！\n\n"
                "我會每天早上 06:00 推送精選新聞：\n"
                "• 台灣屈臣氏與競爭對手\n"
                "• 國內外會計與監管\n"
                "• AI 應用與重大訊息\n\n"
                "你也可以隨時在 Dashboard 調整主題。"
            )
            push_message(user_id, welcome_text)

    return {"ok": True}
