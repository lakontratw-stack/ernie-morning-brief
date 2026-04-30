from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from config import settings
from db import connect
from telegram_notifier import format_notification, send_telegram_message


def enqueue_notification(
    event_type: str,
    payload: dict[str, Any],
    target_chat_id: str | None = None,
    dedup_key: str | None = None,
) -> int | None:
    chat_id = target_chat_id or settings.telegram_work_group_chat_id or "telegram-not-configured"
    now = datetime.utcnow().isoformat()
    with connect() as conn:
        if dedup_key:
            existing = conn.execute(
                "SELECT id FROM notification_outbox WHERE dedup_key=? LIMIT 1",
                (dedup_key,),
            ).fetchone()
            if existing:
                return None
        cur = conn.execute(
            """
            INSERT INTO notification_outbox(
                event_type, target_chat_id, payload_json, status,
                attempts, next_attempt_at, dedup_key
            )
            VALUES (?, ?, ?, 'pending', 0, ?, ?)
            """,
            (event_type, str(chat_id), json.dumps(payload, ensure_ascii=False), now, dedup_key),
        )
        return int(cur.lastrowid)


def process_pending_notifications(limit: int = 20) -> int:
    now = datetime.utcnow().isoformat()
    processed = 0
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM notification_outbox
             WHERE status='pending'
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
             ORDER BY id ASC
             LIMIT ?
            """,
            (now, limit),
        ).fetchall()

    for row in rows:
        payload = json.loads(row["payload_json"] or "{}")
        text = format_notification(row["event_type"], payload)
        try:
            send_telegram_message(text)
            with connect() as conn:
                conn.execute(
                    """
                    UPDATE notification_outbox
                       SET status='sent', sent_at=?, last_error=NULL
                     WHERE id=?
                    """,
                    (datetime.utcnow().isoformat(), row["id"]),
                )
        except Exception as exc:
            attempts = int(row["attempts"] or 0) + 1
            status = "failed" if attempts >= 5 else "pending"
            with connect() as conn:
                conn.execute(
                    """
                    UPDATE notification_outbox
                       SET status=?, attempts=?, last_error=?, next_attempt_at=?
                     WHERE id=?
                    """,
                    (status, attempts, str(exc)[:500], datetime.utcnow().isoformat(), row["id"]),
                )
            print(f"telegram notification failed: {exc}")
        processed += 1
    return processed
