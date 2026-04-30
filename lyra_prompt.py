from __future__ import annotations

import os
import re
from typing import Any

from config import settings
from knowledge_loader import load_knowledge

try:
    from opencc import OpenCC

    _opencc = OpenCC("s2twp")
except Exception as exc:
    print(f"OpenCC unavailable, replies will not be converted: {exc}")
    _opencc = None


ESCALATION_REPLY = "這題可能會影響正式判斷，我先幫你轉給 NTP team 確認，比較安全。"
TENDER_THRESHOLD_NTD = 12_000_000


def build_system_prompt() -> str:
    bundle = load_knowledge()
    return f"""
你是 Lyra，Watsons Taiwan 內部 Non-Trade Procurement 小助理。
你使用台灣繁體中文回答。
你用台灣職場自然語氣，不要太像客服罐頭。
你協助同事理解 NTP policy、LOA、tender 流程、議價策略與 IC summary 草稿。

回答原則：
1. 若知識庫有明確依據，直接回答。
2. 若知識庫沒有明確依據，說明不確定，並建議轉 NTP team。
3. 不要編造政策、金額、核准人。
4. 不要代表公司核准。
5. 不要說已送簽、已通知、已建檔。
6. 需要正式判斷時，回覆最前面使用 [ESCALATE:原因]。
7. 回答要短，適合 LINE 閱讀。
8. 可以先給結論，再給理由。
9. 若使用者要英文 wording，可以提供英文草稿。
10. 若使用者貼 tender 資訊，請整理成：重點判斷、風險提醒、建議追問、可用 wording。

LINE 回覆風格：
- 每次回覆最多 5 行，除非使用者要求完整說明、正式 memo、IC summary 或英文 wording。
- 不要一開始列太多政策；先回答使用者最直接的問題。
- 資料不足時，最多問 2-3 個最關鍵問題。
- 不要自我介紹，除非使用者問「你是誰」、第一次互動只傳 greeting，或系統明確判斷這是第一次互動。
- 不要重複說「我是 Lyra」。
- 不要使用「結論：」「但仍要注意：」這種報告格式，除非使用者要求正式版。
- 口吻自然，像 NTP 同事在 LINE 裡提醒。
- 若使用者前面已經提過金額、品項、供應商或合約狀況，要沿用上下文，不要當成新對話。
- 若使用者說「如果我要…」「那這個…」「這樣可以嗎」「那要怎麼做」，必須參考前文後再回答。
- 優先用自然短句，不要長篇條列。

續約情境規則：
- 當使用者詢問「續約」「展延」「renew contract」「extend contract」時，不要直接判斷可否續約。
- 先確認原合約金額與本次預估續約金額、合約期間、是否已超過或接近 3 年。
- 也要確認是否曾做 market testing / quotation / tender、供應商是否為 ASL。
- 若是 advertising agency、PR agency、media agency、consultant、KOL 或 third party representative，要提醒可能涉及 Significant Expenditures 或 Investment Policy。
- 如果資訊不足，用自然 LINE 口吻問 2-3 個最重要缺口。

必須轉人工的情境：
- 使用者要求例外核准。
- 使用者要求 Lyra 判定某案是否合規，但資料不足。
- 涉及 Internal Audit / Legal / HR / 個資 / 客訴。
- 合約爭議或供應商申訴。
- 金額級距不清楚，可能影響核准層級。
- 使用者要求 Lyra 直接批准、送簽、通知供應商。
- 使用者貼出機密或敏感資訊。
- 你對答案沒有把握。

轉人工標準回覆：
[ESCALATE:原因]
{ESCALATION_REPLY}

Tender 小幫手格式：
判斷：
風險：
建議追問：
可用 wording：

IC summary 草稿格式：
Background:
Commercial Comparison:
Key Risks:
Recommendation:
Decision Required:

價格初判規則：
- 漲價超過 10%：提醒需要成本拆解與 benchmark。
- 漲價超過 20%：標示 high increase。
- 報價低於 current 20% 以上：提醒 suspicious low。
- 多供應商價差超過 50%：提醒 high variance。
- 不做最終決策。

以下是知識庫，僅可依此作為政策依據。

--- guardrails.md ---
{bundle.guardrails}

--- knowledge_base.md ---
{bundle.knowledge_base}

--- tender_playbook.md ---
{bundle.tender_playbook}
""".strip()


def build_messages(user_message: str, history: list[Any]) -> list[dict[str, str]]:
    messages = [{"role": "system", "content": build_system_prompt()}]
    for item in _clean_history(history)[-10:]:
        user = item["user_message"] if hasattr(item, "keys") else item[0]
        bot = item["bot_reply"] if hasattr(item, "keys") else item[1]
        messages.append({"role": "user", "content": str(user)})
        messages.append({"role": "assistant", "content": str(bot)})
    messages.append({"role": "user", "content": user_message})
    return messages


def ask_lyra(user_message: str, history: list[Any]) -> str:
    clean_history = _clean_history(history)

    deterministic = _quick_price_analysis(user_message)
    if deterministic:
        return to_taiwan_traditional(deterministic)

    deterministic = _contract_renewal_reply(user_message, clean_history)
    if deterministic:
        return to_taiwan_traditional(deterministic)

    deterministic = _amount_based_procurement_reply(user_message)
    if deterministic:
        return to_taiwan_traditional(deterministic)

    deterministic = _contextual_followup_reply(user_message, clean_history)
    if deterministic:
        return to_taiwan_traditional(deterministic)

    if not settings.openai_api_key:
        return (
            "[ESCALATE:OPENAI_API_KEY 未設定]\n"
            "我目前還沒有 AI 金鑰可以查知識庫後回答，先幫你轉給 NTP team 確認。"
        )

    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_api_key)
    response = client.chat.completions.create(
        model=settings.openai_model,
        messages=build_messages(user_message, clean_history),
        temperature=0.2,
    )
    content = response.choices[0].message.content or ""
    return to_taiwan_traditional(content.strip())


def to_taiwan_traditional(text: str) -> str:
    if _opencc is None or not text:
        return text
    try:
        return _opencc.convert(text)
    except Exception as exc:
        print(f"OpenCC conversion failed: {exc}")
        return text


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

    current_new = re.search(
        r"current\s*(\d+(?:\.\d+)?).*?new\s*(\d+(?:\.\d+)?)",
        lower,
        re.S,
    )
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
        if len(numbers) == 2 and numbers[1] < numbers[0] * 0.8:
            risks.append("新報價低於 current 20% 以上，需留意 suspicious low 與服務範圍是否縮水。")

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


def _amount_based_procurement_reply(text: str) -> str | None:
    amount = _extract_amount(text)
    if amount is None:
        return None
    if not any(word in text for word in ["採購", "買", "平板", "設備", "需要走", "程序"]):
        return None

    if amount >= TENDER_THRESHOLD_NTD:
        return _tender_threshold_reply(amount)

    if amount <= 100000:
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

    return (
        f"這筆約 NT${amount:,.0f}，已超過 NT$100,000，原則上要進 NTP sourcing。\n"
        "但還沒到 tender 門檻，通常會先往 quotation process 看。\n"
        "下一步確認供應商是否在 ASL，以及至少 3 家書面報價是否可取得。\n"
        "CapEx/Opex、budgeted 會影響 approval 路徑，不是判斷 tender 的主因。"
    )


def _contract_renewal_reply(text: str, history: list[Any]) -> str | None:
    lower = text.lower()
    if not any(word in lower for word in ["續約", "展延", "延長合約", "合約到期", "renew contract", "extend contract"]):
        return None

    context = _history_text(history) if _looks_contextual(text) else ""
    combined = f"{context}\n{text}".lower()
    is_agency = any(
        word in combined
        for word in ["agency", "代理", "廣告", "pr agency", "media agency", "consultant", "kol", "third party"]
    )
    amount = _extract_amount(text) or (_extract_amount(context) if context else None)

    lead = "可以先看，但我不會直接判斷能不能續。"
    if amount:
        lead = f"金額我先抓到約 NT${amount:,.0f}，但續約不能只看金額。"

    supplier_question = "這家是否在 ASL？之前有做 market testing 或其他報價嗎？"
    if is_agency:
        supplier_question += " agency 類也可能要一起看 Significant Expenditures / Investment Policy。"

    return "\n".join(
        [
            lead,
            "我先需要這 3 個資訊：",
            "1. 原合約期間多久？本次想續多久？",
            "2. 原合約金額和這次預估續約金額是多少？",
            f"3. {supplier_question}",
        ]
    )


def _extract_amount(text: str) -> float | None:
    matches = re.findall(r"(?:nt\$?|twd|台幣|新台幣)?\s*(\d{4,}(?:\.\d+)?)", text.lower().replace(",", ""))
    if not matches:
        return None
    return float(matches[0])


def _contextual_followup_reply(text: str, history: list[Any]) -> str | None:
    if not _looks_contextual(text) or not history:
        if not history:
            return None

    context = _history_text(history).lower()
    amount = _extract_amount(text) or _extract_amount(context)
    has_procurement_context = any(word in context for word in ["採購", "平板", "展示桌", "設備", "買"])

    if has_procurement_context and _is_threshold_challenge(text):
        return (
            "你說得對，金額如果超過約 NT$12M，應該先判斷為 tender，不是先問 CapEx/Opex。\n"
            "CapEx/Opex、budgeted、ASL 會影響 approval、供應商資格和後續文件，但不會把 tender 需求變成 quotation。\n"
            "這種案子下一步應抓 tender committee、5 家供應商、scoring criteria 和 sourcing report。"
        )

    if has_procurement_context and _is_procurement_detail_followup(text):
        latest_amount = _extract_amount(context)
        if latest_amount is not None and latest_amount >= TENDER_THRESHOLD_NTD:
            return (
                "收到，不過這筆金額已超過約 NT$12M，方向應該是 tender。\n"
                "Opex、budgeted、ASL 是後續 approval 和供應商資格要確認的事。\n"
                "下一步建議準備 tender scope、5 家供應商名單、評分標準和 tender committee review。"
            )
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

    if amount is not None and any(word in context for word in ["採購", "平板", "設備", "買"]):
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
            "原則上要進 NTP sourcing，通常會往 quotation process 看。\n"
            "先確認 CapEx/Opex、是否已 budgeted、供應商是否在 ASL。"
        )

    if any(word in context for word in ["續約", "展延", "renew contract", "extend contract"]):
        return (
            "如果你是指前面那份合約，現在還不能直接判斷能不能續。\n"
            "先補合約期間、原/新金額、是否做過 market testing。\n"
            "如果是 agency 或 consultant，也要一起確認 Investment Policy 風險。"
        )

    return None


def _looks_contextual(text: str) -> bool:
    stripped = text.strip().lower()
    contextual_markers = [
        "那",
        "這樣",
        "這個",
        "那如果",
        "那這個",
        "這樣可以嗎",
        "那要怎麼做",
        "可以嗎",
    ]
    return any(marker in stripped for marker in contextual_markers)


def _tender_threshold_reply(amount: float) -> str:
    return (
        f"這筆約 NT${amount:,.0f}，已超過約 NT$12M，應先抓 tender 路徑。\n"
        "也就是至少 5 家互相獨立供應商、tender committee、評分標準和 sourcing report。\n"
        "CapEx/Opex、budgeted、ASL 仍要確認，但它們影響 approval 和供應商資格，不是決定要不要 tender 的主因。"
    )


def _is_threshold_challenge(text: str) -> bool:
    lower = text.lower()
    challenge = any(word in lower for word in ["為什麼", "難道", "不是", "應該", "直接跟我說"])
    threshold = any(word in lower for word in ["tender", "12m", "12 m", "12000000", "超過12", "超過 12"])
    return challenge and threshold


def _is_procurement_detail_followup(text: str) -> bool:
    lower = text.lower().strip()
    markers = ["opex", "capex", "budgeted", "unbudgeted", "asl", "asl上", "asl 內", "asl內"]
    return any(marker in lower for marker in markers)


def _clean_history(history: list[Any]) -> list[Any]:
    cleaned = []
    for item in history:
        user = item["user_message"] if hasattr(item, "keys") else item[0]
        bot = item["bot_reply"] if hasattr(item, "keys") else item[1]
        user_text = str(user or "").strip()
        bot_text = str(bot or "").strip()
        if not user_text or not bot_text:
            continue
        if bot_text in {"(human takeover: AI silent)", "人工接手中，AI 不回"}:
            continue
        cleaned.append(item)
    return cleaned


def _history_text(history: list[Any]) -> str:
    parts: list[str] = []
    for item in history[-10:]:
        user = item["user_message"] if hasattr(item, "keys") else item[0]
        bot = item["bot_reply"] if hasattr(item, "keys") else item[1]
        if user:
            parts.append(str(user))
        if bot:
            parts.append(str(bot))
    return "\n".join(parts)
