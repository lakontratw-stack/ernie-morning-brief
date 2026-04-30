from __future__ import annotations

import os
import json
import re
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
    format_store_summary,
)
from .prompts import build_messages

try:
    from opencc import OpenCC

    _cc = OpenCC("s2twp") if os.getenv("ENABLE_OPENCC", "false").lower() == "true" else None
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
    return _ask_procurement_mock(text)


def _ask_procurement_mock(text: str) -> str:
    hard_escalation_keywords = [
        "例外核准",
        "稽核",
        "internal audit",
        "legal",
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
    lower = text.lower()
    if any(keyword in lower for keyword in hard_escalation_keywords):
        return "[ESCALATE:需要正式判斷]\n這題可能會影響正式判斷，我先幫你轉給 NTP team 確認，比較安全。"

    price_reply = _quick_price_analysis(text)
    if price_reply:
        return price_reply

    amount = _extract_amount(text)
    if amount is not None and any(word in text for word in ["採購", "買", "平板", "設備", "需要走", "程序"]):
        if amount <= 100000:
            return (
                "結論：\n"
                "如果這筆採購是單筆或年度同項目不超過 NT$100,000，且不是拆單，通常不需要提交 PR 給 NTP。\n\n"
                "但仍要注意：\n"
                "1. 供應商仍應是 ASL 供應商。\n"
                "2. 如果不是 ASL 供應商，仍需交 Supplier Profile Form 給 NTP 評估。\n"
                "3. 如果是 CapEx、IT、Security、個資或其他 function 相關，可能還有額外 approval。\n\n"
                "以你說的 NT$58,000 平板來看，我會先確認：是否 IT 設備、是否 CapEx、是否已 budgeted、供應商是否在 ASL。"
            )
        return (
            "結論：\n"
            "金額超過 NT$100,000 時，應由 NTP 依程序執行 sourcing activity。\n\n"
            "需要先確認：\n"
            "1. 金額與幣別\n"
            "2. CapEx 或 Opex\n"
            "3. 是否已 budgeted\n"
            "4. 供應商是否在 ASL\n"
            "5. 是否涉及 IT、Supply Chain、Security、Legal、Tax 或 Privacy"
        )

    if any(word in lower for word in ["ntp", "non-trade", "non trade"]):
        return (
            "NTP 是 Non-Trade Procurement，主要處理非轉售商品的採購流程，例如設備、服務、顧問、維修、物流、IT solution 等。\n\n"
            "如果採購不在 Negative List，原則上會落在 NTP 管轄。"
        )
    if "asl" in lower or "供應商" in text:
        return (
            "ASL 是 Approved Supplier List。\n\n"
            "原則上不論金額多寡，使用單位都應與 ASL 供應商合作。若要用新供應商，需要先由 NTP 做 supplier pre-evaluation，並完成 ASL 建立後才適合進 tender 或採購流程。"
        )
    if "tender" in lower or "招標" in text:
        return (
            "Tender 通常適用在 estimated budget 或 previous purchase amount 超過 HK$3M 的案件。\n\n"
            "原則上至少需要 5 家互相獨立的 competing suppliers 參與，且 tender criteria、weightage、tenderer list 應在發 RFQ/RFP 前先由 tender committee 確認。"
        )
    if "quotation" in lower or "報價" in text or "rfq" in lower:
        return (
            "採購金額超過 NT$100,000 且不超過 HK$3M 時，通常至少需邀請 3 家供應商參與 quotation process，且需保留書面報價。\n\n"
            "報價有效性也要能涵蓋採購決策當下。"
        )

    return (
        "您好，我是 Lyra，Watsons Taiwan 內部 NTP 採購小助理。\n"
        "我可以協助你看 NTP policy、ASL、PR/PO、quotation/tender、議價 wording、報價比較和 IC summary 草稿。\n\n"
        "你可以直接貼採購情境，例如：\n"
        "「我要採購 58000 的平板，需要走什麼程序？」"
    )


def _extract_amount(text: str) -> float | None:
    matches = re.findall(r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d{4,}(?:\.\d+)?)", text.lower().replace(",", ""))
    if not matches:
        return None
    return float(matches[0])


def _quick_price_analysis(text: str) -> str | None:
    lower = text.lower()
    if not any(keyword in lower for keyword in ["漲", "vendor", "供應商", "current", "new", "報價", "價格"]):
        return None

    percentages = [float(value) for value in re.findall(r"(\d+(?:\.\d+)?)\s*%", text)]
    numbers = [float(value) for value in re.findall(r"(?<![\w.])(\d+(?:\.\d+)?)(?![\w.])", text)]
    risks: list[str] = []

    if any(value > 20 for value in percentages):
        risks.append("漲幅超過 20%，屬於 high increase，需要更完整成本拆解。")
    elif any(value > 10 for value in percentages):
        risks.append("漲幅超過 10%，建議要求成本拆解與 benchmark。")

    current_new = re.search(r"current\s*(\d+(?:\.\d+)?).*?new\s*(\d+(?:\.\d+)?)", lower, re.S)
    if current_new:
        current = float(current_new.group(1))
        new = float(current_new.group(2))
        if current:
            change = (new - current) / current
            if change > 0.2:
                risks.append(f"new 比 current 高約 {change:.1%}，屬於 high increase。")
            elif change > 0.1:
                risks.append(f"new 比 current 高約 {change:.1%}，建議要求成本拆解與 benchmark。")
            elif change < -0.2:
                risks.append(f"new 比 current 低約 {abs(change):.1%}，需留意 suspicious low。")

    if len(numbers) >= 2:
        low = min(numbers)
        high = max(numbers)
        if low and (high - low) / low > 0.5:
            risks.append("多供應商價差超過 50%，需要確認規格是否一致，避免 high variance。")

    if not risks:
        return None

    return "\n".join(
        [
            "判斷：",
            "這個案子可以先做商務合理性檢查，但不能直接下最終決策。",
            "風險：",
            *[f"- {risk}" for risk in risks],
            "建議追問：",
            "- 請供應商提供成本拆解、規格差異、服務範圍與漲價原因。",
            "- 請確認是否有同規格 benchmark 或第二家報價可比。",
            "可用 wording：",
            "Could you please provide a detailed cost breakdown and clarify the key drivers behind the price movement, so we can complete an internal commercial review?",
        ]
    )


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
