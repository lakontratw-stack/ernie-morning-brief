from __future__ import annotations

import html
import hashlib
import json
from datetime import datetime, timedelta

import requests

from .config import settings
from .db import connect, enqueue, set_takeover

MAX_ATTEMPTS = 5


def esc(value: str | None) -> str:
    return html.escape(value or "")


def enqueue_escalation(
    user_id: str,
    display_name: str,
    user_text: str,
    reason: str,
    ai_reply: str | None = None,
) -> None:
    message = "\n".join(
        [
            "同事們，客戶需要支援",
            "",
            f"客戶：{esc(display_name) or esc(user_id)}",
            f"原因：{esc(reason)}",
            "",
            "客戶原訊息：",
            esc(user_text[:500]),
            "",
            "Lyra 已回覆：",
            esc((ai_reply or "已轉給專員確認。")[:500]),
        ]
    )
    digest = hashlib.sha256(f"{user_id}:{user_text}".encode("utf-8")).hexdigest()[:24]
    enqueue(
        "line.escalation",
        settings.telegram_work_group_chat_id,
        {
            "message": message,
            "inline_keyboard": [[{"text": "我先處理", "callback_data": f"claim:{user_id}"}]],
        },
        dedup_key=f"escalation:{digest}",
    )


def enqueue_notify(user_id: str, display_name: str, user_text: str, summary: str, ai_reply: str) -> None:
    message = "\n".join(
        [
            "新通知",
            "",
            f"摘要：{esc(summary)}",
            f"客戶：{esc(display_name) or esc(user_id)}",
            "",
            "客戶原訊息：",
            esc(user_text[:300]),
            "",
            "Lyra 已回覆：",
            esc(ai_reply[:300]),
        ]
    )
    enqueue("line.notify", settings.telegram_work_group_chat_id, {"message": message})


def drain_once(limit: int = 10) -> int:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM notification_outbox
             WHERE status='pending' AND next_attempt_at <= ?
             ORDER BY id LIMIT ?
            """,
            (datetime.utcnow().isoformat(), limit),
        ).fetchall()

    sent = 0
    for row in rows:
        if _send_row(row):
            sent += 1
    return sent


def _send_row(row) -> bool:
    with connect() as conn:
        claimed = conn.execute(
            """
            UPDATE notification_outbox
               SET status='in_flight', claimed_by='web', claimed_at=?
             WHERE id=? AND status='pending'
            """,
            (datetime.utcnow().isoformat(), row["id"]),
        ).rowcount
        if claimed != 1:
            return False

    payload = json.loads(row["payload_json"])
    body = {
        "chat_id": row["target_chat_id"],
        "text": payload.get("message", ""),
        "parse_mode": "HTML",
    }
    if payload.get("inline_keyboard"):
        body["reply_markup"] = {"inline_keyboard": payload["inline_keyboard"]}

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
            json=body,
            timeout=15,
        )
        response.raise_for_status()
    except Exception as exc:
        _mark_retry(row["id"], row["attempts"], str(exc))
        return False

    with connect() as conn:
        conn.execute(
            "UPDATE notification_outbox SET status='sent', sent_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), row["id"]),
        )
    return True


def _mark_retry(row_id: int, attempts: int, error: str) -> None:
    if attempts + 1 >= MAX_ATTEMPTS:
        with connect() as conn:
            conn.execute(
                "UPDATE notification_outbox SET status='dead_letter', last_error=? WHERE id=?",
                (error[:500], row_id),
            )
        return
    next_attempt = datetime.utcnow() + timedelta(seconds=min(3600, 60 * (2 ** attempts)))
    with connect() as conn:
        conn.execute(
            """
            UPDATE notification_outbox
               SET status='pending', attempts=attempts+1, next_attempt_at=?, last_error=?
             WHERE id=?
            """,
            (next_attempt.isoformat(), error[:500], row_id),
        )


def handle_callback(update: dict) -> None:
    callback = update.get("callback_query") or {}
    data = callback.get("data") or ""
    if not data.startswith("claim:"):
        _answer_callback(callback.get("id"), "收到")
        return

    user_id = data.split(":", 1)[1]
    from_user = callback.get("from") or {}
    staff_name = from_user.get("username") or from_user.get("first_name") or str(from_user.get("id") or "Unknown")
    set_takeover(user_id, staff_name)
    _answer_callback(callback.get("id"), f"{staff_name} 接手中")

    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    original = message.get("text") or ""
    if chat_id and message_id:
        requests.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token}/editMessageText",
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": f"{original}\n\n已由 {staff_name} 接手中",
                "parse_mode": "HTML",
            },
            timeout=10,
        )


def _answer_callback(callback_id: str | None, text: str) -> None:
    if not callback_id:
        return
    requests.post(
        f"https://api.telegram.org/bot{settings.telegram_bot_token}/answerCallbackQuery",
        json={"callback_query_id": callback_id, "text": text},
        timeout=10,
    )
