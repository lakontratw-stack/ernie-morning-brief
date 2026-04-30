from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    flask_env: str = os.getenv("FLASK_ENV", "development")
    app_port: int = int(os.getenv("APP_PORT", os.getenv("PORT", "8000")))

    line_channel_secret: str = os.getenv("LINE_CHANNEL_SECRET", "")
    line_channel_access_token: str = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_work_group_chat_id: str = os.getenv("TELEGRAM_WORK_GROUP_CHAT_ID", "")

    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    db_path: str = os.getenv("DB_PATH", "./lyra_procurement.db")
    takeover_hours: int = int(os.getenv("TAKEOVER_HOURS", "12"))


settings = Settings()
