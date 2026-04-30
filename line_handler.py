from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import threading
from datetime import datetime
from typing import Any

import requests
from flask import Request

import db
from config import settings
from lyra_prompt import ESCALATION_REPLY, ask_lyra
from notification_queue import enqueue_notification

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_PROFILE_URL = "https://api.line.me/v2/bot/profile/{user_id}"

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

ESCALATE_RE = re.compile(r"^\s*\[ESCALATE(?::\s*([^\]]+))?\]\s*(.*)$", re.S)
NOTIFY_RE = re.compile(r"\[NOTIFY:\s*([^\]]+)\]", re.S)


def verify_line_signature(body: bytes, signature: str) -> bool:
    if not settings.line_channel_secret or not signature:
        return False
    digest = hmac.new(settings.line_channel_secret.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def handle_line_webhook(request: Request) -> tuple[str, int]:
    body = request.get_data()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_line_signature(body, signature):
        return "invalid signature", 400

    payload = json.loads(body.decode("utf-8") or "{}")
    for event in payload.get("events", []):
        thread = threading.Thread(target=_handle_event, args=(event,), daemon=True)
        thread.start()
    return "OK", 200


def _handle_event(event: dict[str, Any]) -> None:
    source = event.get("source") or {}
    line_user_id = source.get("userId")
    reply_token = event.get("replyToken")
    message = event.get("message") or {}
    msg_type = message.get("type")
    if not line_user_id or not reply_token:
        return

    profile = get_line_profile(line_user_id)
    display_name = profile.get("displayName") or line_user_id
    db.ensure_line_user(line_user_id, display_name)

    if msg_type == "text":
        _handle_text(line_user_id, display_name, reply_token, message.get("text") or "")
    elif msg_type == "sticker":
        reply_line_text(reply_token, "收到貼圖。如果要問採購流程或 tender 問題，可以直接打字給我。")
    elif msg_type in {"image", "file", "audio", "video"}:
        _escalate_direct(
            line_user_id,
            display_name,
            reply_token,
            f"使用者傳送 {msg_type}，需要人工判讀。",
            f"({msg_type} message)",
        )
    else:
        _escalate_direct(line_user_id, display_name, reply_token, "未知訊息類型，需要人工確認。", f"({msg_type})")


def _handle_text(line_user_id: str, display_name: str, reply_token: str, text: str) -> None:
    if db.get_line_status(line_user_id) == "human_taken_over":
        db.log_chat(line_user_id, text, "(human takeover: AI silent)")
        _enqueue_takeover_message(line_user_id, display_name, text)
        return

    keyword = next((word for word in KEYWORD_ESCALATIONS if word.lower() in text.lower()), None)
    if keyword:
        _escalate_direct(line_user_id, display_name, reply_token, f"keyword hard escalation: {keyword}", text)
        return

    history = db.recent_history(line_user_id, limit=10)
    try:
        raw_reply = ask_lyra(text, history)
    except Exception as exc:
        print(f"Lyra error: {exc}")
        raw_reply = f"[ESCALATE:AI 回覆錯誤]\n{ESCALATION_REPLY}"

    escalated, reason, cleaned_reply = parse_escalation(raw_reply)
    if escalated:
        customer_reply = cleaned_reply or ESCALATION_REPLY
        reply_line_text(reply_token, customer_reply)
        db.set_takeover(line_user_id)
        db.log_chat(line_user_id, text, customer_reply)
        enqueue_notification(
            "escalation",
            _payload(line_user_id, display_name, text, customer_reply, reason or "AI escalation"),
        )
        return

    notify_summary, customer_reply = parse_notify(raw_reply)
    reply_line_text(reply_token, customer_reply)
    db.log_chat(line_user_id, text, customer_reply)
    if notify_summary:
        enqueue_notification(
            "notify",
            _payload(line_user_id, display_name, text, customer_reply, notify_summary),
        )


def _escalate_direct(
    line_user_id: str,
    display_name: str,
    reply_token: str,
    reason: str,
    user_message: str,
) -> None:
    reply_line_text(reply_token, ESCALATION_REPLY)
    db.set_takeover(line_user_id)
    db.log_chat(line_user_id, user_message, ESCALATION_REPLY)
    enqueue_notification(
        "escalation",
        _payload(line_user_id, display_name, user_message, ESCALATION_REPLY, reason),
    )


def _enqueue_takeover_message(line_user_id: str, display_name: str, text: str) -> None:
    enqueue_notification(
        "takeover_message",
        _payload(line_user_id, display_name, text, "", "human takeover active"),
    )


def reply_line_text(reply_token: str, text: str) -> None:
    if not settings.line_channel_access_token:
        print(f"[line disabled] reply: {text}")
        return
    response = requests.post(
        LINE_REPLY_URL,
        headers={
            "Authorization": f"Bearer {settings.line_channel_access_token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        data=json.dumps(
            {
                "replyToken": reply_token,
                "messages": [{"type": "text", "text": (text or "")[:4900]}],
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        timeout=15,
    )
    response.raise_for_status()


def get_line_profile(line_user_id: str) -> dict[str, Any]:
    if not settings.line_channel_access_token:
        return {}
    try:
        response = requests.get(
            LINE_PROFILE_URL.format(user_id=line_user_id),
            headers={"Authorization": f"Bearer {settings.line_channel_access_token}"},
            timeout=10,
        )
        if response.status_code >= 400:
            return {}
        return response.json()
    except Exception as exc:
        print(f"LINE profile failed: {exc}")
        return {}


def parse_escalation(reply: str) -> tuple[bool, str | None, str]:
    match = ESCALATE_RE.match(reply or "")
    if not match:
        return False, None, reply
    return True, (match.group(1) or "AI escalation").strip(), (match.group(2) or "").strip()


def parse_notify(reply: str) -> tuple[str | None, str]:
    match = NOTIFY_RE.search(reply or "")
    if not match:
        return None, reply
    summary = match.group(1).strip()
    cleaned = NOTIFY_RE.sub("", reply, count=1).strip()
    return summary, cleaned


def _payload(
    line_user_id: str,
    display_name: str,
    user_message: str,
    bot_reply: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "line_user_id": line_user_id,
        "display_name": display_name,
        "user_message": user_message,
        "bot_reply": bot_reply,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
    }
