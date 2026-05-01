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

TENDER_THRESHOLD_NTD = 12_000_000
PROCUREMENT_CASE_KEYWORDS = [
    "採購",
    "採買",
    "買",
    "支出",
    "費用",
    "花費",
    "付款",
    "請款",
    "平板",
    "展示桌",
    "設備",
    "需要走",
    "怎麼進行",
    "程序",
]

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
    history = recent_history(user_id, limit=10)
    hard_escalation = _hard_escalation_reply(text)
    if hard_escalation:
        return to_traditional(hard_escalation.strip())

    fast_reply = _fast_policy_anchor_reply(text, history)
    if fast_reply:
        return to_traditional(fast_reply.strip())

    messages = build_messages(user_id, text)
    provider = settings.lyra_provider.lower()
    try:
        if provider == "hermes_cli":
            reply = _ask_hermes_cli(messages)
        elif provider == "openai_compatible" and settings.openai_api_key:
            reply = _ask_openai_compatible(messages)
        else:
            reply = _ask_mock(text, history)
    except Exception as exc:
        print(f"LLM provider failed, falling back to deterministic reply: {exc}")
        reply = _ask_mock(text, history)
    return to_traditional(reply.strip())


def _ask_mock(text: str, history: list[Any] | None = None) -> str:
    return _ask_procurement_mock(text, history or [])


def _ask_procurement_mock(text: str, history: list[Any]) -> str:
    deterministic_reply = _deterministic_procurement_reply(text, history)
    if deterministic_reply:
        return deterministic_reply

    return (
        "你可以直接把採購情境貼給我，我會先幫你整理流程、風險和要補的資料。\n"
        "最好包含金額、品項、供應商、是否續約、是否已有報價。\n"
        "資訊越完整，我就能越快幫你判斷下一步。"
    )


def _hard_escalation_reply(text: str) -> str | None:
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
        return "[ESCALATE:需要正式判斷]\n這題會牽涉正式判斷，我先通知 NTP team 一起看，比較安全。\n你也可以先補金額、供應商、合約/報價狀況，我會先幫你整理問題點。"
    return None


def _deterministic_procurement_reply(text: str, history: list[Any]) -> str | None:
    lower = text.lower()
    hard_escalation = _hard_escalation_reply(text)
    if hard_escalation:
        return hard_escalation

    threshold_reply = _tender_threshold_question_reply(text)
    if threshold_reply:
        return threshold_reply

    rate_card_threshold_reply = _rate_card_threshold_reply(text)
    if rate_card_threshold_reply:
        return rate_card_threshold_reply

    rate_card_reply = _rate_card_commitment_reply(text)
    if rate_card_reply:
        return rate_card_reply

    policy_reply = _policy_qa_reply(text)
    if policy_reply:
        return policy_reply

    appointment_reply = _supplier_appointment_reply(text)
    if appointment_reply:
        return appointment_reply

    urgent_reply = _urgent_purchase_reply(text)
    if urgent_reply:
        return urgent_reply

    followup_reply = _followup_reply(text, history)
    if followup_reply:
        return followup_reply

    price_reply = _quick_price_analysis(text)
    if price_reply:
        return price_reply

    renewal_reply = _contract_renewal_reply(text, history)
    if renewal_reply:
        return renewal_reply

    amount = _extract_amount(text)
    if amount is not None and _is_procurement_case_text(text):
        if amount >= TENDER_THRESHOLD_NTD:
            return _tender_threshold_reply(amount)
        if amount <= 100000:
            return _small_purchase_reply(amount, text)
        return (
            f"這筆 NT${amount:,.0f} 已超過 NT$100,000，若不是 Negative List，先開 PR 給 NTP。\n"
            "金額還沒到 tender 門檻，通常會走 quotation。\n"
            "NTP 原則上要邀至少 3 家 ASL 供應商提供書面報價。\n"
            "你再補品項、CapEx/Opex、是否 budgeted，我可以幫你確認下一步文件。"
        )

    if any(word in lower for word in ["ntp", "non-trade", "non trade"]):
        return (
            "NTP 是 Non-Trade Procurement，主要處理非轉售商品的採購流程，例如設備、服務、顧問、維修、物流、IT solution 等。\n\n"
            "如果採購不在 Negative List，原則上會落在 NTP 管轄。"
        )
    if "asl" in lower:
        return (
            "ASL 是 Approved Supplier List，簡單說就是可合作供應商名單。\n"
            "如果供應商已在 ASL，下一步通常就看金額門檻、CapEx/Opex 和 quotation/tender 要求。\n"
            "如果你是在補前一筆採購資料，可以直接回金額、CapEx/Opex、是否 budgeted。"
        )
    if "tender" in lower or "招標" in text:
        return _tender_process_reply()
    if "quotation" in lower or "報價" in text or "rfq" in lower:
        return (
            "採購金額超過 NT$100,000 且不超過 HK$3M 時，通常至少需邀請 3 家供應商參與 quotation process，且需保留書面報價。\n\n"
            "報價有效性也要能涵蓋採購決策當下。"
        )

    return None


def _fast_policy_anchor_reply(text: str, history: list[Any]) -> str | None:
    """Fast path for policy questions that should not wait for an LLM round trip."""
    threshold_reply = _tender_threshold_question_reply(text)
    if threshold_reply:
        return threshold_reply

    rate_card_threshold_reply = _rate_card_threshold_reply(text)
    if rate_card_threshold_reply:
        return rate_card_threshold_reply

    rate_card_reply = _rate_card_commitment_reply(text)
    if rate_card_reply:
        return rate_card_reply

    urgent_reply = _urgent_purchase_reply(text)
    if urgent_reply:
        return urgent_reply

    appointment_reply = _supplier_appointment_reply(text)
    if appointment_reply:
        return appointment_reply

    policy_reply = _policy_qa_reply(text)
    if policy_reply:
        return policy_reply

    followup_reply = _followup_reply(text, history)
    if followup_reply:
        return followup_reply

    amount = _extract_amount(text)
    if amount is not None and _is_procurement_case_text(text):
        return _deterministic_procurement_reply(text, history)

    return None


def _extract_amount(text: str) -> float | None:
    normalized = text.lower().replace(",", "")
    million_match = re.search(
        r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d+(?:\.\d+)?)\s*(?:m|mn|million|百萬)\b",
        normalized,
    )
    if million_match:
        return float(million_match.group(1)) * 1_000_000

    ten_thousand_match = re.search(r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d+(?:\.\d+)?)\s*萬", normalized)
    if ten_thousand_match:
        return float(ten_thousand_match.group(1)) * 10_000

    matches = re.findall(r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d{4,}(?:\.\d+)?)", normalized)
    if not matches:
        return None
    return float(matches[0])


def _policy_qa_reply(text: str) -> str | None:
    lower = text.lower()
    if _is_supplier_evaluation_question(text):
        return (
            "Policy 明確寫到的是：PO 或合約金額達 HK$3M 以上，re-tender 前要做 supplier performance appraisal。\n"
            "評估要由 user department 依 agreed KPI 做，NTP 會一起看供應商品質和服務表現。\n"
            "另外 ASL 至少每兩年要 review/update 一次。\n"
            "如果你問的是固定年度評估頻率，我目前不會硬說每年一次，建議看你們內部 KPI 或由 NTP 確認。"
        )
    if "asl" in lower and (_is_asl_definition_question(text) or not _is_procurement_detail_followup(text)):
        return (
            "ASL 是 Approved Supplier List，也就是可合作供應商名單。\n"
            "原則上不論金額大小，都應使用 ASL 供應商。\n"
            "新供應商要先完成 supplier pre-evaluation；要進 tender 前，必須已正式在 ASL。"
        )
    return None


def _tender_threshold_question_reply(text: str) -> str | None:
    if not _is_tender_threshold_question(text):
        return None
    return _tender_threshold_policy_reply()


def _tender_threshold_policy_reply() -> str:
    return (
        "Tender 門檻看 estimated budget / purchase amount 或 previous spending。\n"
        "超過 HK$3M 就要走 tender，原則上至少 5 家 competing suppliers / sealed bids。\n"
        "NT$100,000 以上到 HK$3M，通常是 quotation，至少 3 家書面報價。\n"
        "如果是 Rate Card，要用合約期間 estimated spending 保守估；接近門檻時建議往 tender 看。"
    )


def _rate_card_commitment_reply(text: str) -> str | None:
    lower = text.lower()
    if not (
        ("rate card" in lower or "ratecard" in lower or "rate-card" in lower)
        and ("commitment" in lower or "committment" in lower or "承諾" in text)
    ):
        return None

    return (
        "差別重點是：有沒有 commitment。\n"
        "Rate Card 是先約好單價/價格表，但沒有最低採購量、最低付款義務，也沒有排他限制。\n"
        "Commitment contract 會綁住 BU，例如 minimum order、minimum financial obligation、exclusivity，或兩者都有。\n"
        "如果價格表其實有任何 commitment，就不能當純 Rate Card，通常要按 Investment Policy 的 Commitment 規則看 approval。"
    )


def _rate_card_threshold_reply(text: str) -> str | None:
    lower = text.lower()
    if not any(word in lower for word in ["rate card", "ratecard", "rate-card"]):
        return None
    if not any(
        word in lower
        for word in ["hk$3m", "3m", "3 m", "hk$ 3m", "門檻", "threshold", "estimated", "預估", "接近"]
    ):
        return None

    return (
        "Rate Card 不能只看單價表，要看合約期間 estimated spending。\n"
        "如果預估金額接近 HK$3M，我會建議保守往 tender 路徑準備，不要只當 quotation。\n"
        "另外要確認它沒有 minimum order、minimum financial obligation 或 exclusivity。\n"
        "只要有這些 commitment，就不算單純 Rate Card，approval 也要另外看 Investment Policy。"
    )


def _supplier_appointment_reply(text: str) -> str | None:
    lower = text.lower()
    if not any(word in lower for word in ["appoint", "direct award", "single source", "指定供應商", "直接指定", "直接給", "單一供應商"]):
        return None

    return (
        "如果你說的 appoint 是直接指定供應商，先不要當成 OK。\n"
        "要先看金額門檻、是否 ASL、是否有足夠 quotation/tender，以及為什麼不能公平競爭。\n"
        "如果真的只能單一供應商，要有 written justification，通常也要補 benchmark 或其他合理性證明。\n"
        "我可以幫你整理 rationale 草稿，但不能替公司確認可以指定。"
    )


def _urgent_purchase_reply(text: str) -> str | None:
    lower = text.lower()
    if not any(word in lower for word in ["urgent", "緊急採購", "急件", "緊急"]):
        return None

    return (
        "緊急採購可以先看，但不是用來跳過流程。\n"
        "Policy 上通常要是影響門市/辦公室營運、安全，或設備需 3 天內更換這類情況。\n"
        "限制也要記得：供應商仍要在 ASL、金額不能超過 HK$50,000，事後 PR 和 supporting document 要補齊。\n"
        "你先回我金額、門市/辦公室影響、預計幾天內要處理，我再幫你整理怎麼寫。"
    )


def _tender_process_reply() -> str:
    return (
        "Tender 先抓這幾步：定 scope/spec、設 evaluation criteria 和 weighting、整理 tenderer list。\n"
        "這些在發 RFQ/RFP 前，要先給 tender committee review。\n"
        "原則上至少 5 家互相獨立供應商參與，也要留 COI declaration 和 communication trail。\n"
        "收到 proposal 後才做 scoring、commercial comparison、sourcing/open tender report，最後才是 award recommendation。"
    )


def _is_supplier_evaluation_question(text: str) -> bool:
    lower = text.lower()
    supplier = "供應商" in text or "supplier" in lower or "vendor" in lower
    evaluation = any(word in text for word in ["評估", "績效", "考核", "年度評估"]) or any(
        word in lower for word in ["evaluation", "performance appraisal", "performance review", "annual review"]
    )
    timing = any(word in text for word in ["多久", "多常", "何時", "什麼時候", "要求", "規定"]) or any(
        word in lower for word in ["when", "how often", "requirement", "required"]
    )
    return supplier and evaluation and timing


def _is_asl_definition_question(text: str) -> bool:
    lower = text.lower()
    if "asl" not in lower:
        return False
    definition_patterns = [
        "asl 是什麼",
        "asl是什麼",
        "什麼是 asl",
        "什麼是asl",
        "asl 是啥",
        "asl是啥",
        "asl 意思",
        "asl意思",
        "asl 定義",
        "asl定義",
    ]
    return any(pattern in lower for pattern in definition_patterns)


def _followup_reply(text: str, history: list[Any]) -> str | None:
    context = _history_text(history)
    if not context:
        return None

    context_lower = context.lower()
    lower = text.lower().strip()
    amount = _extract_amount(text)
    if _is_tender_threshold_question(text) or (
        _is_amount_threshold_followup(text) and any(word in context_lower for word in ["tender", "招標"])
    ):
        return _tender_threshold_policy_reply()

    if (
        _is_contract_renewal_context(context_lower)
        and _looks_renewal_followup(text)
        and not _is_procurement_case_text(text)
        and not _is_procurement_detail_followup(text)
    ):
        return _contract_renewal_reply(text, history)

    has_procurement_context = _is_procurement_case_text(context)
    if has_procurement_context and _is_threshold_challenge(text):
        return (
            "你說得對，金額如果超過約 NT$12M，應該先判斷為 tender，不是先問 CapEx/Opex。\n"
            "CapEx/Opex、budgeted、ASL 會影響 approval、供應商資格和後續文件，但不會把 tender 需求變成 quotation。\n"
            "這種案子下一步應抓 tender committee、5 家供應商、scoring criteria 和 sourcing report。"
        )

    if amount is not None and has_procurement_context and _is_short_followup(text):
        if amount >= TENDER_THRESHOLD_NTD:
            return _tender_threshold_reply(amount)
        if amount <= 100000:
            return (
                f"如果前面那筆改成 NT${amount:,.0f}，且不是拆單或年度累計超過 NT$100,000，通常不用走 NTP sourcing。\n"
                "但仍要確認供應商在 ASL、是否已 budgeted，以及是否有 IT/CapEx 需求。\n"
                "如果這三點都 OK，流程會比較單純。"
            )
        return (
            f"如果前面那筆改成 NT${amount:,.0f}，就已經超過 NT$100,000。\n"
            "若不是 Negative List，先開 PR 給 NTP，通常會往 quotation process 看。\n"
            "先確認 CapEx/Opex、是否已 budgeted、供應商是否在 ASL。"
        )

    if amount is not None and has_procurement_context and _looks_contextual(text):
        if amount >= TENDER_THRESHOLD_NTD:
            return _tender_threshold_reply(amount)
        if amount <= 100000:
            return (
                f"如果你是指前面那筆，金額 NT${amount:,.0f} 未超過 NT$100,000。\n"
                "通常不用走 NTP sourcing，但仍要確認不是拆單、供應商在 ASL、是否有 IT/CapEx 需求。"
            )
        return (
            f"你前面提到的預估金額我抓 NT${amount:,.0f}。\n"
            "如果是 NTP 範圍，已超過 NT$100,000，通常先走 quotation process。\n"
            "下一步補 CapEx/Opex、是否 budgeted、供應商是否在 ASL。"
        )

    detail_markers = ["opex", "capex", "budgeted", "unbudgeted", "asl", "asl上", "asl 內", "asl內"]
    if has_procurement_context and any(marker in lower for marker in detail_markers):
        latest_amount = _extract_amount(context)
        if latest_amount is not None and latest_amount >= TENDER_THRESHOLD_NTD:
            return (
                "收到，不過這筆金額已超過約 NT$12M，方向應該是 tender。\n"
                "Opex、budgeted、ASL 是後續 approval 和供應商資格要確認的事。\n"
                "下一步建議準備 tender scope、5 家供應商名單、評分標準和 tender committee review。"
            )
        if latest_amount is not None and latest_amount > 100000:
            return (
                "收到，供應商在 ASL，金額也抓到了。\n"
                "因為金額超過 NT$100,000，下一步建議開 PR 給 NTP 走 quotation process。\n"
                "通常至少要準備 3 家書面報價；CapEx/Opex、是否 budgeted 還是要補清楚。"
            )
        return (
            "收到，這樣流程會比較單純。\n"
            "如果金額未超過 NT$100,000、不是拆單、供應商在 ASL，通常不用走 NTP sourcing。\n"
            "但如果有 IT/CapEx 或年度累計超門檻，還是要再確認。"
        )

    if _looks_contextual(text) and has_procurement_context:
        latest_amount = amount or _extract_amount(context)
        if latest_amount and latest_amount >= TENDER_THRESHOLD_NTD:
            return _tender_threshold_reply(latest_amount)
        if latest_amount and latest_amount > 100000:
            return (
                "如果你是指前面那筆，因為已超過 NT$100,000，建議先進 NTP sourcing。\n"
                "也就是先開 PR 給 NTP，再看 quotation 文件。\n"
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
    return any(marker in stripped for marker in ["那", "這樣", "這個", "那如果", "那這個", "可以嗎", "剛剛", "不是說"])


def _is_procurement_detail_followup(text: str) -> bool:
    lower = text.lower().strip()
    markers = ["opex", "capex", "budgeted", "unbudgeted", "asl", "asl上", "asl 內", "asl內"]
    return any(marker in lower for marker in markers)


def _is_procurement_case_text(text: str) -> bool:
    return any(word in text for word in PROCUREMENT_CASE_KEYWORDS)


def _tender_threshold_reply(amount: float) -> str:
    return (
        f"這筆 NT${amount:,.0f} 已經很明顯落在 tender 等級。\n"
        "NTP policy 的門檻是 estimated / previous purchase amount 超過 HK$3M 要 tender；NTD 只是換算概念，正式仍看幣別和公司匯率。\n"
        "下一步先準備 scope、5 家獨立供應商、評分標準，並送 tender committee 先確認。\n"
        "CapEx/Opex、budgeted、ASL 也要看，但它們不會把 tender 變成 quotation。"
    )


def _is_threshold_challenge(text: str) -> bool:
    lower = text.lower()
    challenge = any(word in lower for word in ["為什麼", "難道", "不是", "應該", "直接跟我說"])
    threshold = any(word in lower for word in ["tender", "12m", "12 m", "12000000", "超過12", "超過 12"])
    return challenge and threshold


def _is_tender_threshold_question(text: str) -> bool:
    lower = text.lower()
    has_tender = "tender" in lower or "招標" in text
    asks_amount = any(
        marker in lower
        for marker in ["多少錢", "多少金额", "多少金額", "超過多少", "門檻", "threshold", "amount"]
    )
    return has_tender and asks_amount


def _is_amount_threshold_followup(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in ["多少錢", "多少金額", "超過多少", "門檻", "threshold", "amount"])


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
            f"這筆 NT${amount:,.0f} 低於 NT$100,000，通常不用走 NTP sourcing。\n"
            "但平板多半算 IT / hardware，也可能牽涉 CapEx。\n"
            f"先確認：{notes}。\n"
            "這三點 OK 再往下走，會比較安全。"
        )

    return (
        f"這筆 NT${amount:,.0f} 低於 NT$100,000，通常不用走 NTP sourcing。\n"
        f"先確認：{notes}。\n"
        "如果任何一項不確定，就先不要直接下單。"
    )


def _contract_renewal_reply(text: str, history: list[Any] | None = None) -> str | None:
    if any(word in text.lower() for word in ["平板", "ipad", "電腦", "筆電", "系統", "software", "it"]):
        return None
    lower = text.lower()
    context = _history_text(history or [])
    context_lower = context.lower()
    is_renewal = _is_contract_renewal_request(lower) or (
        _is_contract_renewal_context(context_lower)
        and _looks_renewal_followup(text)
        and not _is_procurement_case_text(text)
        and not _is_procurement_detail_followup(text)
    )
    if not is_renewal:
        return None

    combined = f"{context}\n{text}".lower()
    is_agency = any(
        word in combined
        for word in ["agency", "代理", "廣告", "pr agency", "media agency", "consultant", "kol", "third party"]
    )
    amount = _extract_amount(text) or _extract_amount(context)
    years = _extract_years(text) or _extract_years(context)

    if amount and amount >= TENDER_THRESHOLD_NTD:
        duration_note = f"、期間 {years:g} 年" if years else ""
        return (
            f"這份續約金額約 NT${amount:,.0f}{duration_note}，已超過約 NT$12M，不能當一般續約處理。\n"
            "方向應先抓 re-tender / tender review，至少要看 5 家供應商或有清楚 exception 理由。\n"
            "CapEx/Opex、budgeted、ASL 仍要確認，但不會改變 tender 門檻判斷。"
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


def _extract_years(text: str) -> float | None:
    lower = text.lower()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:yrs?|years?|年)", lower)
    if match:
        return float(match.group(1))
    chinese_years = {
        "一年": 1,
        "二年": 2,
        "兩年": 2,
        "三年": 3,
        "四年": 4,
        "五年": 5,
    }
    for label, value in chinese_years.items():
        if label in text:
            return float(value)
    return None


def _is_contract_renewal_request(text: str) -> bool:
    lower = text.lower()
    if any(word in lower for word in ["續約", "展延", "延長合約", "renew", "renewal", "extend contract", "contract renewal"]):
        return True
    return "合約到期" in lower and any(word in lower for word in ["怎麼", "處理", "要注意", "可以", "renew", "extend"])


def _is_contract_renewal_context(text: str) -> bool:
    lower = text.lower()
    return any(word in lower for word in ["續約", "展延", "延長合約", "renew", "renewal", "extend contract", "contract renewal"])


def _looks_renewal_followup(text: str) -> bool:
    lower = text.lower().strip()
    if _extract_amount(lower) is not None or _extract_years(lower) is not None:
        return True
    return any(word in lower for word in ["金額", "期間", "三年", "兩年", "一年", "asl", "budgeted", "opex", "capex"])


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
        timeout=25,
    )
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    return data["choices"][0]["message"]["content"]


def _messages_to_prompt(messages: list[dict[str, str]]) -> str:
    return "\n\n".join(
        f"{item['role'].upper()}:\n{item['content']}" for item in messages
    )
