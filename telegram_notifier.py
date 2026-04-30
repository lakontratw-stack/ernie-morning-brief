from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from config import settings


def send_telegram_message(text: str) -> bool:
    if not settings.telegram_bot_token or not settings.telegram_work_group_chat_id:
        print(f"[telegram disabled]\n{text}")
        return False

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": settings.telegram_work_group_chat_id,
            "text": text[:3900],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=15,
    )
    response.raise_for_status()
    return True


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
