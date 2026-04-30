from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator

from .config import settings


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(settings.app_db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS line_users (
                line_user_id TEXT PRIMARY KEY,
                display_name TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                takeover_at TEXT,
                takeover_by TEXT,
                last_seen_at TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS chat_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                user_message TEXT,
                bot_reply TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_chat_logs_session
                ON chat_logs(session_id, channel, id DESC);

            CREATE TABLE IF NOT EXISTS notification_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                target_chat_id TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                next_attempt_at TEXT,
                last_error TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                sent_at TEXT,
                claimed_by TEXT,
                claimed_at TEXT,
                dedup_key TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS notification_outbox_dedup
                ON notification_outbox(dedup_key) WHERE dedup_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS notification_outbox_status
                ON notification_outbox(status, next_attempt_at);
            """
        )


def ensure_line_user(user_id: str, display_name: str | None = None) -> None:
    now = datetime.utcnow().isoformat()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO line_users(line_user_id, display_name, last_seen_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(line_user_id) DO UPDATE SET
                display_name=COALESCE(excluded.display_name, line_users.display_name),
                last_seen_at=excluded.last_seen_at,
                updated_at=excluded.updated_at
            """,
            (user_id, display_name, now, now),
        )


def get_line_status(user_id: str) -> str:
    with connect() as conn:
        row = conn.execute("SELECT status FROM line_users WHERE line_user_id=?", (user_id,)).fetchone()
    return row["status"] if row else "active"


def set_takeover(user_id: str, staff_name: str | None = None) -> None:
    now = datetime.utcnow().isoformat()
    with connect() as conn:
        conn.execute(
            """
            UPDATE line_users
               SET status='human_taken_over', takeover_at=?, takeover_by=?, updated_at=?
             WHERE line_user_id=?
            """,
            (now, staff_name, now, user_id),
        )


def auto_recover_takeovers() -> list[sqlite3.Row]:
    cutoff = datetime.utcnow() - timedelta(hours=settings.takeover_hours)
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT line_user_id, display_name FROM line_users
             WHERE status='human_taken_over' AND takeover_at <= ?
            """,
            (cutoff.isoformat(),),
        ).fetchall()
        for row in rows:
            conn.execute(
                """
                UPDATE line_users
                   SET status='active', takeover_at=NULL, takeover_by=NULL, updated_at=?
                 WHERE line_user_id=?
                """,
                (datetime.utcnow().isoformat(), row["line_user_id"]),
            )
    return rows


def log_chat(session_id: str, user_message: str, bot_reply: str, channel: str = "line") -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO chat_logs(session_id, channel, user_message, bot_reply)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, channel, user_message, bot_reply),
        )


def recent_history(session_id: str, limit: int = 15) -> list[sqlite3.Row]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT user_message, bot_reply FROM chat_logs
             WHERE session_id=? AND channel='line'
             ORDER BY id DESC LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()
    return list(reversed(rows))


def enqueue(event_type: str, target_chat_id: str, payload: dict, dedup_key: str | None = None) -> int | None:
    now = datetime.utcnow().isoformat()
    payload_json = json.dumps(payload, ensure_ascii=False)
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
            INSERT INTO notification_outbox(event_type, target_chat_id, payload_json, dedup_key, next_attempt_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, str(target_chat_id), payload_json, dedup_key, now),
        )
        return int(cur.lastrowid)
