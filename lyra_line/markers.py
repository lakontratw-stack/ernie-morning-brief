from __future__ import annotations

import re


ESCALATE_RE = re.compile(r"\s*\[ESCALATE(?::\s*([^\]]*))?\]\s*(.*)", re.S)
NOTIFY_RE = re.compile(r"\[NOTIFY:\s*([^\]]+)\]", re.S)


def parse_escalation(reply: str) -> tuple[bool, str | None, str]:
    if not reply:
        return False, None, reply
    match = ESCALATE_RE.match(reply)
    if not match:
        return False, None, reply
    reason = (match.group(1) or "").strip() or "AI 自行判斷"
    cleaned = (match.group(2) or "").strip()
    return True, reason, cleaned


def parse_notify(reply: str) -> tuple[str | None, str]:
    if not reply:
        return None, reply
    match = NOTIFY_RE.search(reply)
    if not match:
        return None, reply
    summary = match.group(1).strip()
    cleaned = NOTIFY_RE.sub("", reply, count=1).strip()
    return summary, cleaned
