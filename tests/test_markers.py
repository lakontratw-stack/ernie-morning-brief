from lyra_line.markers import parse_escalation, parse_notify


def test_parse_escalation_at_start():
    escalated, reason, cleaned = parse_escalation("[ESCALATE:查詢訂單]\n我幫您轉專員")
    assert escalated is True
    assert reason == "查詢訂單"
    assert cleaned == "我幫您轉專員"


def test_parse_notify_anywhere():
    summary, cleaned = parse_notify("好喔，等您過來。[NOTIFY:客戶下午三點到門市]")
    assert summary == "客戶下午三點到門市"
    assert cleaned == "好喔，等您過來。"


def test_plain_reply():
    escalated, reason, cleaned = parse_escalation("您好")
    assert escalated is False
    assert reason is None
    assert cleaned == "您好"
