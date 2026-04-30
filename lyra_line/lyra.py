from __future__ import annotations

import os
import json
import re
import subprocess
from typing import Any

import requests

from .config import settings
from .db import recent_history
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
        reply = _ask_mock(text, recent_history(user_id, limit=10))
    return to_traditional(reply.strip())


def _ask_mock(text: str, history: list[Any] | None = None) -> str:
    return _ask_procurement_mock(text, history or [])


def _ask_procurement_mock(text: str, history: list[Any]) -> str:
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

    followup_reply = _followup_reply(text, history)
    if followup_reply:
        return followup_reply

    price_reply = _quick_price_analysis(text)
    if price_reply:
        return price_reply

    renewal_reply = _contract_renewal_reply(text)
    if renewal_reply:
        return renewal_reply

    amount = _extract_amount(text)
    if amount is not None and any(word in text for word in ["採購", "買", "平板", "設備", "需要走", "程序"]):
        if amount <= 100000:
            return _small_purchase_reply(amount, text)
        return (
            f"這筆約 NT${amount:,.0f}，已超過 NT$100,000，原則上要進 NTP sourcing。\n"
            "先不要自己直接找廠商定案。\n"
            "我需要 3 個資訊：CapEx/Opex、是否已 budgeted、供應商是否在 ASL。\n"
            "補上後我再幫你看應走 quotation 還是 tender。"
        )

    if any(word in lower for word in ["ntp", "non-trade", "non trade"]):
        return (
            "NTP 是 Non-Trade Procurement，主要處理非轉售商品的採購流程，例如設備、服務、顧問、維修、物流、IT solution 等。\n\n"
            "如果採購不在 Negative List，原則上會落在 NTP 管轄。"
        )
    if "asl" in lower or "供應商" in text:
        return (
            "ASL 是 Approved Supplier List，簡單說就是可合作供應商名單。\n"
            "如果供應商已在 ASL，下一步通常就看金額門檻、CapEx/Opex 和 quotation/tender 要求。\n"
            "如果你是在補前一筆採購資料，可以直接回金額、CapEx/Opex、是否 budgeted。"
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
        "你可以直接把採購情境貼給我，我會先幫你整理流程、風險和要補的資料。\n"
        "最好包含金額、品項、供應商、是否續約、是否已有報價。\n"
        "資訊越完整，我就能越快幫你判斷下一步。"
    )


def _extract_amount(text: str) -> float | None:
    matches = re.findall(r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d{4,}(?:\.\d+)?)", text.lower().replace(",", ""))
    if not matches:
        return None
    return float(matches[0])


def _followup_reply(text: str, history: list[Any]) -> str | None:
    context = _history_text(history)
    if not context:
        return None

    context_lower = context.lower()
    lower = text.lower().strip()
    amount = _extract_amount(text)

    has_procurement_context = any(word in context for word in ["採購", "平板", "展示桌", "設備", "買"])
    if amount is not None and has_procurement_context and _is_short_followup(text):
        if amount <= 100000:
            return (
                f"如果前面那筆改成 NT${amount:,.0f}，且不是拆單或年度累計超過 NT$100,000，通常不用走 NTP sourcing。\n"
                "但仍要確認供應商在 ASL、是否已 budgeted，以及是否有 IT/CapEx 需求。\n"
                "如果這三點都 OK，流程會比較單純。"
            )
        return (
            f"如果前面那筆改成 NT${amount:,.0f}，就已經超過 NT$100,000。\n"
            "原則上要進 NTP sourcing，通常會往 quotation process 看。\n"
            "先確認 CapEx/Opex、是否已 budgeted、供應商是否在 ASL。"
        )

    detail_markers = ["opex", "capex", "budgeted", "unbudgeted", "asl", "asl上", "asl 內", "asl內"]
    if has_procurement_context and any(marker in lower for marker in detail_markers):
        latest_amount = _extract_amount(context)
        if latest_amount is not None and latest_amount > 100000:
            return (
                "收到，Opex、budgeted、供應商也在 ASL，方向就比較清楚。\n"
                "因為金額超過 NT$100,000，下一步建議交 NTP 走 quotation process。\n"
                "通常至少要準備 3 家書面報價；如果只有一家或家數不足，就要先把原因寫清楚給 NTP 確認。"
            )
        return (
            "收到，這樣流程會比較單純。\n"
            "如果金額未超過 NT$100,000、不是拆單、供應商在 ASL，通常不用走 NTP sourcing。\n"
            "但如果有 IT/CapEx 或年度累計超門檻，還是要再確認。"
        )

    if _looks_contextual(text) and has_procurement_context:
        latest_amount = amount or _extract_amount(context)
        if latest_amount and latest_amount > 100000:
            return (
                "如果你是指前面那筆，因為已超過 NT$100,000，建議先進 NTP sourcing。\n"
                "我還需要 CapEx/Opex、是否已 budgeted、供應商是否 ASL。\n"
                "補上後我再幫你看 quotation 或 tender 路徑。"
            )
        return (
            "如果你是指前面那筆小額採購，可以先走比較簡單的路徑。\n"
            "但還不能直接說可以下單，因為要先確認 ASL、是否拆單/年度累計、以及 IT 或 CapEx 需求。"
        )

    return None


def _is_short_followup(text: str) -> bool:
    stripped = text.strip()
    if re.fullmatch(r"(?:nt\$?|twd|台幣|新台幣)?\s*\d{4,}(?:\.\d+)?", stripped.lower().replace(",", "")):
        return True
    return len(stripped) <= 18 and bool(_extract_amount(stripped))


def _looks_contextual(text: str) -> bool:
    stripped = text.strip().lower()
    return any(marker in stripped for marker in ["那", "這樣", "這個", "那如果", "那這個", "可以嗎"])


def _history_text(history: list[Any]) -> str:
    parts: list[str] = []
    for item in history[-10:]:
        user = item["user_message"] if hasattr(item, "keys") else item[0]
        bot = item["bot_reply"] if hasattr(item, "keys") else item[1]
        user_text = str(user or "").strip()
        bot_text = str(bot or "").strip()
        if not user_text or not bot_text:
            continue
        if bot_text in {"(human takeover: AI silent)", "(人工接手中，AI 不回)", "人工接手中，AI 不回"}:
            continue
        parts.append(user_text)
        parts.append(bot_text)
    return "\n".join(parts)


def _small_purchase_reply(amount: float, text: str) -> str:
    lower = text.lower()
    notes = "供應商是否在 ASL、是否已 budgeted、有沒有拆單或年度累計超過門檻"
    if any(word in lower for word in ["平板", "ipad", "電腦", "筆電", "系統", "software", "it"]):
        notes = "供應商是否在 ASL、是否已 budgeted、IT 是否同意"
        return (
            f"這筆約 NT${amount:,.0f}，如果不是拆單或年度累計超過 NT$100,000，通常不用走 NTP sourcing。\n"
            "但平板多半會牽涉 IT / hardware，也可能要看 CapEx。\n"
            f"你先確認這 3 件事：{notes}。\n"
            "回我這幾點，我再幫你判斷下一步。"
        )

    return (
        f"這筆約 NT${amount:,.0f}，如果不是拆單或年度累計超過 NT$100,000，通常不用走 NTP sourcing。\n"
        f"先確認這 3 件事：{notes}。\n"
        "如果任何一項不確定，就先不要直接下單。\n"
        "你回我狀況，我再幫你接下一步。"
    )


def _contract_renewal_reply(text: str) -> str | None:
    if any(word in text.lower() for word in ["平板", "ipad", "電腦", "筆電", "系統", "software", "it"]):
        return None
    lower = text.lower()
    if not any(word in lower for word in ["續約", "展延", "延長合約", "合約到期", "renew contract", "extend contract"]):
        return None

    is_agency = any(
        word in lower
        for word in ["agency", "代理", "廣告", "pr agency", "media agency", "consultant", "kol", "third party"]
    )
    supplier_question = "這家是否在 ASL？之前有做 market testing 或其他報價嗎？"
    if is_agency:
        supplier_question += " agency 類也可能要一起看 Significant Expenditures / Investment Policy。"

    return "\n".join(
        [
            "可以先看，但我不會直接判斷能不能續。",
            "我先需要這 3 個資訊：",
            "1. 原合約期間多久？本次想續多久？",
            "2. 原合約金額和這次預估續約金額是多少？",
            f"3. {supplier_question}",
        ]
    )


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
            "這個價格先不要直接收，我會先當成 commercial risk 看。",
            "主要風險：" + "；".join(risks),
            "先請供應商補 cost breakdown、scope 差異和 benchmark。",
            "英文可以這樣問：Could you provide a cost breakdown and explain the key drivers behind the price movement?",
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
