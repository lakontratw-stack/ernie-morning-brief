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

--- knowledge_base.md ---
{bundle.knowledge_base}

--- tender_playbook.md ---
{bundle.tender_playbook}
""".strip()


def build_messages(user_message: str, history: list[Any]) -> list[dict[str, str]]:
    messages = [{"role": "system", "content": build_system_prompt()}]
    for item in history[-10:]:
        user = item["user_message"] if hasattr(item, "keys") else item[0]
        bot = item["bot_reply"] if hasattr(item, "keys") else item[1]
        if user:
            messages.append({"role": "user", "content": str(user)})
        if bot:
            messages.append({"role": "assistant", "content": str(bot)})
    messages.append({"role": "user", "content": user_message})
    return messages


def ask_lyra(user_message: str, history: list[Any]) -> str:
    deterministic = _quick_price_analysis(user_message)
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
        messages=build_messages(user_message, history),
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
