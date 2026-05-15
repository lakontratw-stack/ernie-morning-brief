from __future__ import annotations

import base64
import hashlib
import hmac
import json

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
    response = requests.post(
        LINE_REPLY_URL,
        headers={
            "Authorization": f"Bearer {settings.line_channel_access_token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        data=json.dumps(
            {
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": text,
                        "sender": {"name": settings.line_sender_name[:20]},
                    }
                ],
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        timeout=settings.line_reply_timeout_seconds,
    )
    response.raise_for_status()


def get_profile(user_id: str) -> dict:
    if not settings.line_channel_access_token or not settings.line_profile_lookup_enabled:
        return {}
    try:
        response = requests.get(
            LINE_PROFILE_URL.format(user_id=user_id),
            headers={"Authorization": f"Bearer {settings.line_channel_access_token}"},
            timeout=settings.line_profile_timeout_seconds,
        )
    except requests.RequestException as exc:
        print(f"LINE profile lookup skipped: {exc}")
        return {}
    if response.status_code >= 400:
        print(f"LINE profile lookup failed: {response.status_code}")
        return {}
    return response.json()
