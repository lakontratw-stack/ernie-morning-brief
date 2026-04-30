from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from config import settings


def send_telegram_message(
    text: str,
    chat_id: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> bool:
    target_chat_id = chat_id or settings.telegram_work_group_chat_id
    if not settings.telegram_bot_token or not target_chat_id:
        print(f"[telegram disabled]\n{text}")
        return False

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    body: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": text[:3900],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        body["reply_markup"] = reply_markup
    response = requests.post(
        url,
        json=body,
        timeout=15,
    )
    response.raise_for_status()
    return True


def build_reply_markup(event_type: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if event_type != "escalation":
        return None
    line_user_id = payload.get("line_user_id")
    if not line_user_id:
        return None
    return {"inline_keyboard": [[{"text": "我先處理", "callback_data": f"claim:{line_user_id}"}]]}


def format_notification(event_type: str, payload: dict[str, Any]) -> str:
    display_name = payload.get("display_name") or payload.get("line_user_id") or "unknown"
    user_message = payload.get("user_message") or ""
    bot_reply = payload.get("bot_reply") or ""
    reason = payload.get("reason") or payload.get("summary") or ""
    timestamp = payload.get("timestamp") or datetime.utcnow().isoformat()

    title_map = {
        "escalation": "Lyra 轉人工通知",
        "notify": "Lyra 資訊通知",
        "takeover_message": "人工接手中，使用者補充訊息",
    }
    title = title_map.get(event_type, event_type)

    lines = [
        f"<b>{_escape(title)}</b>",
        f"時間：{_escape(timestamp)}",
        f"LINE 使用者：{_escape(display_name)}",
    ]
    if reason:
        lines.append(f"原因 / 摘要：{_escape(reason)}")
    if user_message:
        lines.append(f"使用者訊息：\n{_escape(user_message)}")
    if bot_reply:
        lines.append(f"Lyra 回覆：\n{_escape(bot_reply)}")
    return "\n\n".join(lines)


def _escape(value: Any) -> str:
    text = str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def handle_telegram_callback(update: dict[str, Any]) -> None:
    callback = update.get("callback_query") or {}
    callback_id = callback.get("id")
    data = callback.get("data") or ""
    if not data.startswith("claim:"):
        _answer_callback(callback_id, "收到")
        return

    line_user_id = data.split(":", 1)[1]
    from_user = callback.get("from") or {}
    staff_name = (
        from_user.get("username")
        or from_user.get("first_name")
        or str(from_user.get("id") or "Unknown")
    )

    try:
        from db import set_takeover

        set_takeover(line_user_id, staff_name)
    except Exception as exc:
        print(f"telegram claim failed: {exc}")
        _answer_callback(callback_id, "接手失敗，請稍後再試")
        return

    _answer_callback(callback_id, f"{staff_name} 接手中")
    _mark_message_claimed(callback, staff_name)


def _answer_callback(callback_id: str | None, text: str) -> None:
    if not callback_id or not settings.telegram_bot_token:
        return
    requests.post(
        f"https://api.telegram.org/bot{settings.telegram_bot_token}/answerCallbackQuery",
        json={"callback_query_id": callback_id, "text": text},
        timeout=10,
    )


def _mark_message_claimed(callback: dict[str, Any], staff_name: str) -> None:
    if not settings.telegram_bot_token:
        return
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    original = message.get("text") or ""
    if not chat_id or not message_id:
        return
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
