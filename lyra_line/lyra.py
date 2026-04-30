from __future__ import annotations

import json
import subprocess
from typing import Any

import requests

from .config import settings
from .prompts import build_messages

try:
    from opencc import OpenCC

    _cc = OpenCC("s2twp")
except Exception:
    _cc = None


def to_traditional(text: str) -> str:
    if _cc is None or not text:
        return text
    return _cc.convert(text)


def ask_lyra(user_id: str, text: str) -> str:
    messages = build_messages(user_id, text)
    provider = settings.lyra_provider.lower()
    if provider == "hermes_cli":
        reply = _ask_hermes_cli(messages)
    elif provider == "openai_compatible":
        reply = _ask_openai_compatible(messages)
    else:
        reply = _ask_mock(text)
    return to_traditional(reply.strip())


def _ask_mock(text: str) -> str:
    normalized = text.lower().replace(" ", "")
    if any(word in text for word in ["退貨", "退款", "發票", "客訴", "庫存", "訂單", "會員帳號"]):
        return "[ESCALATE:需要查詢系統或人工確認]\n這個我幫您轉給專員確認比較準，稍後會有同事接續協助您。"
    if _is_business_hours_question(normalized):
        if "民權" in text:
            return "屈臣氏民權店的營業時間可能會依門市公告調整。\n以一般屈臣氏門市來說，多數大約是 11:00 到 22:00。\n建議您出發前再用 Google 地圖或屈臣氏官網門市查詢確認一下，比較準喔。"
        return "多數屈臣氏門市大約是 11:00 到 22:00。\n不過每間門市可能不同，建議以官網門市查詢或 Google 地圖為準喔。"
    if any(word in text for word in ["地址", "在哪", "門市"]):
        return "屈臣氏門市很多，每間地址不同。\n您可以提供想查的地區或門市名稱，我可以先協助判斷；實際地址建議以官網門市查詢為準。"
    return "您好，我是 Lyra。\n我可以先協助您查一般門市服務、營業時間、商品分類和常見購物問題。\n如果需要查訂單、庫存或會員資料，我會幫您轉給專員。"


def _is_business_hours_question(normalized_text: str) -> bool:
    hour_keywords = [
        "營業時間",
        "營業到幾點",
        "營業至幾點",
        "營業幾點",
        "幾點營業",
        "幾點開",
        "幾點關",
        "幾點休息",
        "幾點打烊",
        "有開嗎",
        "還有開",
        "開到幾點",
        "關到幾點",
        "打烊",
    ]
    return any(keyword in normalized_text for keyword in hour_keywords)


def _ask_hermes_cli(messages: list[dict[str, str]]) -> str:
    prompt = _messages_to_prompt(messages)
    result = subprocess.run(
        [settings.hermes_bin, "chat", "-q", prompt],
        cwd=settings.hermes_workdir,
        text=True,
        capture_output=True,
        timeout=90,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Hermes CLI failed: {result.stderr[-500:]}")
    return result.stdout.strip()


def _ask_openai_compatible(messages: list[dict[str, str]]) -> str:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_COMPATIBLE_API_KEY is not configured")
    response = requests.post(
        f"{settings.openai_base_url.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": settings.openai_model,
            "messages": messages,
            "temperature": 0.3,
        },
        timeout=60,
    )
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    return data["choices"][0]["message"]["content"]


def _messages_to_prompt(messages: list[dict[str, str]]) -> str:
    return "\n\n".join(
        f"{item['role'].upper()}:\n{item['content']}" for item in messages
    )
