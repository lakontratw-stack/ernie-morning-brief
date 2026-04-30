from __future__ import annotations

import json
import threading
import time
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from . import db
from .config import settings
from .line_client import get_profile, reply_text, verify_signature
from .lyra import ask_lyra
from .knowledge import refresh_knowledge
from .markers import parse_escalation, parse_notify
from .telegram_client import (
    drain_once,
    enqueue_escalation,
    enqueue_notify,
    handle_callback,
)

ESCALATION_REPLY = "這個我幫您轉給專員確認比較準，稍後會有同事接續協助您。"
KEYWORD_ESCALATIONS = ["客訴", "退貨", "退款", "發票", "投訴", "主管", "人工", "真人", "訂單", "庫存"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    stop_event = threading.Event()
    thread = threading.Thread(target=_drain_loop, args=(stop_event,), daemon=True)
    thread.start()
    yield
    stop_event.set()


app = FastAPI(title="Lyra LINE OA Integration", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/admin/refresh-knowledge")
def admin_refresh_knowledge() -> dict[str, str]:
    refresh_knowledge()
    return {"status": "refreshed"}


@app.get("/", response_class=HTMLResponse)
def demo_page() -> str:
    return """
    <!doctype html>
    <html lang="zh-Hant">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Lyra LINE OA Preview</title>
      <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7f9; color: #202124; }
        main { max-width: 760px; margin: 40px auto; padding: 0 20px; }
        h1 { font-size: 28px; margin: 0 0 8px; }
        p { color: #5f6368; line-height: 1.6; }
        section { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 20px; margin-top: 20px; }
        textarea { width: 100%; min-height: 120px; padding: 12px; font-size: 16px; border: 1px solid #d1d5db; border-radius: 6px; box-sizing: border-box; }
        button { margin-top: 12px; padding: 10px 16px; border: 0; border-radius: 6px; background: #166534; color: white; font-size: 15px; cursor: pointer; }
        button:disabled { opacity: .6; cursor: wait; }
        pre { white-space: pre-wrap; background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 16px; min-height: 120px; }
        .meta { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
        .pill { background: #eef2ff; color: #3730a3; border-radius: 999px; padding: 6px 10px; font-size: 13px; }
      </style>
    </head>
    <body>
      <main>
        <h1>Lyra LINE OA Preview</h1>
        <p>這裡可以先測 Lyra 的客服回覆邏輯，不會真的傳 LINE，也不會通知 Telegram。</p>
        <section>
          <textarea id="text">請問屈臣氏營業到幾點？</textarea>
          <button id="send">測試 Lyra 回覆</button>
          <div class="meta" id="meta"></div>
          <pre id="result">尚未測試</pre>
        </section>
      </main>
      <script>
        const text = document.getElementById('text');
        const send = document.getElementById('send');
        const result = document.getElementById('result');
        const meta = document.getElementById('meta');
        send.onclick = async () => {
          send.disabled = true;
          result.textContent = 'Lyra 思考中...';
          meta.innerHTML = '';
          try {
            const res = await fetch('/demo/chat', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({text: text.value})
            });
            const data = await res.json();
            result.textContent = data.customer_reply || data.raw_reply || '';
            meta.innerHTML = [
              data.escalated ? '會轉人工：' + data.reason : '不轉人工',
              data.notify ? '會通知：' + data.notify : '無通知 marker',
              'Provider：' + data.provider
            ].map(v => '<span class="pill">' + v + '</span>').join('');
          } catch (err) {
            result.textContent = '測試失敗：' + err;
          } finally {
            send.disabled = false;
          }
        };
      </script>
    </body>
    </html>
    """


@app.post("/demo/chat")
async def demo_chat(request: Request):
    payload = await request.json()
    text = (payload.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "missing text"}, status_code=400)
    raw_reply = ask_lyra("demo-user", text)
    escalated, reason, cleaned = parse_escalation(raw_reply)
    notify, notify_cleaned = parse_notify(cleaned if escalated else raw_reply)
    return {
        "provider": settings.lyra_provider,
        "raw_reply": raw_reply,
        "customer_reply": notify_cleaned if notify else (cleaned if escalated else raw_reply),
        "escalated": escalated,
        "reason": reason,
        "notify": notify,
    }


@app.post("/webhook")
async def line_webhook_root(request: Request, background_tasks: BackgroundTasks):
    return await _handle_line_webhook(request, background_tasks)


@app.post("/webhook/line")
async def line_webhook(request: Request, background_tasks: BackgroundTasks):
    return await _handle_line_webhook(request, background_tasks)


async def _handle_line_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_signature(body, signature):
        return PlainTextResponse("invalid signature", status_code=400)

    payload = json.loads(body.decode("utf-8") or "{}")
    for event in payload.get("events", []):
        background_tasks.add_task(_dispatch_event, event)
    return PlainTextResponse("")


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    update = await request.json()
    handle_callback(update)
    return JSONResponse({"ok": True})


def _dispatch_event(event: dict) -> None:
    source = event.get("source") or {}
    user_id = source.get("userId")
    reply_token = event.get("replyToken")
    message = event.get("message") or {}
    msg_type = message.get("type")
    if not user_id or not reply_token:
        return

    profile = get_profile(user_id)
    display_name = profile.get("displayName") or user_id
    db.ensure_line_user(user_id, display_name)

    if msg_type == "text":
        _process_text(user_id, display_name, reply_token, message.get("text") or "")
    elif msg_type == "sticker":
        reply_text(reply_token, "謝謝您的貼圖～有什麼可以幫您的嗎？")
    elif msg_type == "image":
        reply_text(reply_token, ESCALATION_REPLY)
        db.set_takeover(user_id)
        enqueue_escalation(user_id, display_name, "(客戶傳圖片)", "客戶傳送圖片", ESCALATION_REPLY)
    else:
        reply_text(reply_token, "抱歉這類訊息我目前看不到～請打字告訴我您的需求。")


def _process_text(user_id: str, display_name: str, reply_token: str, text: str) -> None:
    if db.get_line_status(user_id) == "human_taken_over":
        db.log_chat(user_id, text, "(人工接手中，AI 不回)")
        enqueue_escalation(user_id, display_name, text, "人工接手期間客戶補充訊息", None)
        return

    keyword = next((kw for kw in KEYWORD_ESCALATIONS if kw in text), None)
    if keyword:
        reply_text(reply_token, ESCALATION_REPLY)
        db.set_takeover(user_id)
        db.log_chat(user_id, text, ESCALATION_REPLY)
        enqueue_escalation(user_id, display_name, text, f"關鍵字「{keyword}」", ESCALATION_REPLY)
        return

    try:
        ai_reply = ask_lyra(user_id, text)
    except Exception as exc:
        ai_reply = f"[ESCALATE:Lyra 回覆引擎錯誤]\n{ESCALATION_REPLY}"
        print(f"Lyra error: {exc}")

    escalated, reason, cleaned = parse_escalation(ai_reply)
    if escalated:
        customer_reply = cleaned or ESCALATION_REPLY
        reply_text(reply_token, customer_reply)
        db.set_takeover(user_id)
        db.log_chat(user_id, text, customer_reply)
        enqueue_escalation(user_id, display_name, text, reason or "AI 判斷", customer_reply)
        return

    notify_summary, cleaned = parse_notify(ai_reply)
    customer_reply = cleaned or ai_reply
    reply_text(reply_token, customer_reply)
    db.log_chat(user_id, text, customer_reply)

    if notify_summary:
        enqueue_notify(user_id, display_name, text, notify_summary, customer_reply)


def _drain_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            db.auto_recover_takeovers()
            drain_once()
        except Exception as exc:
            print(f"drainer error: {exc}")
        stop_event.wait(5)
