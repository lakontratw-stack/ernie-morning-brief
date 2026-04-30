from __future__ import annotations

import base64
import hashlib
import hmac
import time

import requests

from .config import settings

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_PROFILE_URL = "https://api.line.me/v2/bot/profile/{user_id}"


def verify_signature(body: bytes, signature: str) -> bool:
    if not settings.line_channel_secret or not signature:
        return False
    digest = hmac.new(
        settings.line_channel_secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def reply_text(reply_token: str, text: str) -> None:
    text = (text or "").strip()[:4900]
    delay = min(15.0, 2.0 + len(text) * 0.04)
    time.sleep(delay)
    response = requests.post(
        LINE_REPLY_URL,
        headers={
            "Authorization": f"Bearer {settings.line_channel_access_token}",
            "Content-Type": "application/json",
        },
        json={
            "replyToken": reply_token,
            "messages": [
                {
                    "type": "text",
                    "text": text,
                    "sender": {"name": settings.line_sender_name[:20]},
                }
            ],
        },
        timeout=15,
    )
    response.raise_for_status()


def get_profile(user_id: str) -> dict:
    if not settings.line_channel_access_token:
        return {}
    response = requests.get(
        LINE_PROFILE_URL.format(user_id=user_id),
        headers={"Authorization": f"Bearer {settings.line_channel_access_token}"},
        timeout=10,
    )
    if response.status_code >= 400:
        return {}
    return response.json()
