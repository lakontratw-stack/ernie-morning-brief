from __future__ import annotations

import json
import subprocess
from typing import Any

import requests

from .config import settings
from .knowledge import (
    active_promotions,
    fallback,
    find_store,
    find_store_candidates,
    format_promotions,
    format_store_candidates,
    format_store_hours,
)
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
        store = find_store(text)
        if store:
            return format_store_hours(store)
        candidates = find_store_candidates(text)
        if candidates:
            return format_store_candidates(candidates)
        return fallback("store_hours") or "我可以幫您查門市營業時間。請問您想查哪一間門市呢？"
    if _is_promotion_question(normalized):
        return format_promotions(active_promotions())
    if any(word in text for word in ["地址", "在哪", "門市"]):
        store = find_store(text)
        if store:
            address = store.get("address", "")
            if address and not address.startswith("請在這裡"):
                return f"{store.get('name')}地址是：\n{address}"
        candidates = find_store_candidates(text)
        if candidates:
            return format_store_candidates(candidates)
        return "屈臣氏門市很多，每間地址不同。\n您可以提供想查的地區或門市名稱，我再幫您確認。"
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


def _is_promotion_question(normalized_text: str) -> bool:
    promotion_keywords = [
        "促銷",
        "活動",
        "優惠",
        "折扣",
        "特價",
        "買一送一",
        "第二件",
        "會員優惠",
        "檔期",
    ]
    return any(keyword in normalized_text for keyword in promotion_keywords)


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
