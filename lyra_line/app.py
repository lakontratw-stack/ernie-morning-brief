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
from .lyra import ask_lyra, _deterministic_procurement_reply
from .knowledge import (
    active_promotions,
    find_store,
    load_knowledge,
    official_source_status,
    refresh_knowledge,
    store_cache_status,
)
from .markers import parse_escalation, parse_notify
from .telegram_client import (
    drain_once,
    enqueue_escalation,
    enqueue_notify,
    handle_callback,
)

ESCALATION_REPLY = "這題可能會影響正式判斷，我先幫你轉給 NTP team 確認，比較安全。"
KEYWORD_ESCALATIONS = [
    "例外核准",
    "稽核",
    "Internal Audit",
    "Legal",
    "法務",
    "客訴",
    "個資",
    "申訴",
    "違規",
    "合約爭議",
    "主管要求",
    "直接核准",
    "幫我批准",
    "幫我送簽",
]
FRUSTRATION_ESCALATIONS = ["聽不懂", "不懂", "不聰明", "你不明白", "沒用", "爛", "笨"]
APP_VERSION = "procurement-mvp-legacy-fastapi-compat-20260502-fast-policy-route-v2"
RESET_AI_KEYWORDS = ["恢復AI", "恢復ai", "解除人工", "重啟AI", "重啟ai", "讓AI回覆", "讓ai回覆"]


class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"

    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    stop_event = threading.Event()
    thread = threading.Thread(target=_drain_loop, args=(stop_event,), daemon=True)
    thread.start()
    yield
    stop_event.set()


app = FastAPI(
    title="Lyra LINE OA Integration",
    lifespan=lifespan,
    default_response_class=UTF8JSONResponse,
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "version": APP_VERSION}


@app.post("/admin/refresh-knowledge")
def admin_refresh_knowledge() -> dict[str, str]:
    db.init_db()
    refresh_knowledge()
    return {"status": "refreshed"}


@app.get("/admin/diagnostics", response_class=UTF8JSONResponse)
def admin_diagnostics() -> dict:
    db.init_db()
    refresh_knowledge()
    data = load_knowledge()
    sample_questions = [
        "我有一筆150,000的支出請問一下我要怎麼進行",
        "我多久要做一次供應商評估？",
        "我想採購19999999的車子，需要走甚麼程序",
        "is that ok? if i would like to appoint a supplier",
        "我有一筆鐵捲門的緊急採購，要怎麼做",
        "Tender要做什麼程序",
        "多少錢要做tender",
        "超過多少錢要做tender？",
        "如果是Rate Card但預估金額接近HK$3M怎麼辦？",
        "what's the difference between Rate card contract and committment contract in Investment policy",
        "如果我要直接指定供應商可以嗎",
        "我想採購99999的平板，需要走甚麼程序",
        "如果我要續約廣告代理人的合約，我要注意甚麼",
        "A vendor 100, B vendor 170",
        "那這樣可以嗎",
    ]
    return {
        "status": "ok",
        "version": APP_VERSION,
        "provider": settings.lyra_provider,
        "store_count": len(data.get("stores", [])),
        "promotion_count": len(data.get("promotions", [])),
        "active_promotion_count": len(active_promotions()),
        "has_minquan_store": find_store("民權店") is not None,
        "store_cache": store_cache_status(),
        "watsons_fetch_timeout": settings.watsons_fetch_timeout,
        "watsons_fetch_retries": settings.watsons_fetch_retries,
        "watsons_default_hours_enabled": settings.watsons_default_hours_enabled,
        "watsons_default_hours": {
            "days": settings.watsons_default_days,
            "open": settings.watsons_default_open,
            "close": settings.watsons_default_close,
        },
        "official_sources": official_source_status(),
        "sample_replies": {question: _diagnostic_reply(question) for question in sample_questions},
    }


def _diagnostic_reply(question: str) -> str:
    reply = _deterministic_procurement_reply(question, [])
    return reply or "(diagnostics skipped LLM fallback)"


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
        <p>這裡可以先測 Lyra 的內部採購回覆邏輯，不會真的傳 LINE，也不會通知 Telegram。</p>
        <section>
          <textarea id="text">我想採購99999的平板，需要走甚麼程序</textarea>
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
        reply_text(reply_token, "收到。採購或 tender 問題請直接打字給我。")
    elif msg_type in {"image", "file", "audio", "video"}:
        reply_text(reply_token, ESCALATION_REPLY)
        db.set_takeover(user_id)
        db.log_chat(user_id, f"({msg_type} message)", ESCALATION_REPLY)
        enqueue_escalation(
            user_id,
            display_name,
            f"(使用者傳送 {msg_type})",
            f"使用者傳送 {msg_type}，需要人工判讀",
            ESCALATION_REPLY,
        )
    else:
        reply_text(reply_token, "抱歉這類訊息我目前看不到～請打字告訴我您的需求。")


def _process_text(user_id: str, display_name: str, reply_token: str, text: str) -> None:
    if any(keyword in text for keyword in RESET_AI_KEYWORDS):
        db.clear_takeover(user_id)
        customer_reply = "已恢復 Lyra 自動回覆。你可以再問我採購流程、tender、續約或議價問題。"
        reply_text(reply_token, customer_reply)
        db.log_chat(user_id, text, customer_reply)
        return

    if db.get_line_status(user_id) == "human_taken_over":
        user = db.get_line_user(user_id)
        takeover_by = user["takeover_by"] if user and "takeover_by" in user.keys() else None
        if _looks_like_new_policy_question(text):
            db.clear_takeover(user_id)
        else:
            db.log_chat(user_id, text, "(人工接手中，AI 不回)")
            enqueue_escalation(user_id, display_name, text, "人工接手期間客戶補充訊息", None)
            return

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

    frustration = next((kw for kw in FRUSTRATION_ESCALATIONS if kw in text), None)
    if frustration:
        customer_reply = "我剛剛沒有抓準你的意思。這題我先幫你轉給 NTP team，避免來回誤判。"
        reply_text(reply_token, customer_reply)
        db.set_takeover(user_id)
        db.log_chat(user_id, text, customer_reply)
        enqueue_escalation(user_id, display_name, text, f"客戶反映 AI 未理解：「{frustration}」", customer_reply)
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
        db.log_chat(user_id, text, customer_reply)
        enqueue_escalation(user_id, display_name, text, reason or "AI 判斷", customer_reply)
        return

    notify_summary, cleaned = parse_notify(ai_reply)
    customer_reply = cleaned or ai_reply
    reply_text(reply_token, customer_reply)
    db.log_chat(user_id, text, customer_reply)

    if notify_summary:
        enqueue_notify(user_id, display_name, text, notify_summary, customer_reply)


def _looks_like_new_policy_question(text: str) -> bool:
    lower = text.lower()
    return any(
        keyword in lower
        for keyword in [
            "採購",
            "tender",
            "quotation",
            "rfq",
            "rfp",
            "緊急",
            "程序",
            "怎麼做",
            "怎麼進行",
            "供應商評估",
            "rate card",
            "ratecard",
            "investment policy",
            "hk$3m",
            "金額",
            "門檻",
            "asl",
        ]
    )


def _drain_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            db.auto_recover_takeovers()
            drain_once()
        except Exception as exc:
            print(f"drainer error: {exc}")
        stop_event.wait(5)
