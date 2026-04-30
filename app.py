from __future__ import annotations

import threading
import time

from flask import Flask, jsonify, request

import db
from config import settings
from line_handler import handle_line_webhook
from notification_queue import process_pending_notifications
from telegram_notifier import handle_telegram_callback

APP_VERSION = "procurement-mvp-2026-04-30"


def create_app() -> Flask:
    db.init_db()
    app = Flask(__name__)

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "version": APP_VERSION})

    @app.post("/webhook/line")
    def webhook_line():
        body, status = handle_line_webhook(request)
        return body, status

    @app.post("/webhook")
    def webhook_legacy():
        body, status = handle_line_webhook(request)
        return body, status

    @app.post("/admin/process-notifications")
    def admin_process_notifications():
        count = process_pending_notifications()
        return jsonify({"status": "ok", "processed": count})

    @app.post("/admin/recover-takeovers")
    def admin_recover_takeovers():
        recovered = db.auto_recover_takeovers(hours=settings.takeover_hours)
        return jsonify({"status": "ok", "recovered": len(recovered)})

    @app.post("/webhook/telegram")
    def webhook_telegram():
        handle_telegram_callback(request.get_json(silent=True) or {})
        return jsonify({"ok": True})

    return app


def _notification_worker() -> None:
    while True:
        try:
            db.auto_recover_takeovers(hours=settings.takeover_hours)
            process_pending_notifications()
        except Exception as exc:
            print(f"notification worker error: {exc}")
        time.sleep(5)


app = create_app()

worker = threading.Thread(target=_notification_worker, daemon=True)
worker.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=settings.app_port, debug=settings.flask_env == "development")
