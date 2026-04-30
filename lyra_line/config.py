from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    line_channel_secret: str = os.getenv("LINE_CHANNEL_SECRET", "")
    line_channel_access_token: str = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
    line_sender_name: str = os.getenv("LINE_SENDER_NAME", "Lyra")

    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_work_group_chat_id: str = os.getenv("TELEGRAM_WORK_GROUP_CHAT_ID", "")

    app_db_path: str = os.getenv("APP_DB_PATH", "./lyra_line.db")
    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "")
    takeover_hours: int = int(os.getenv("TAKEOVER_HOURS", "12"))

    lyra_provider: str = os.getenv("LYRA_PROVIDER", "mock")
    store_hours_csv_url: str = os.getenv("STORE_HOURS_CSV_URL", "")
    promotions_csv_url: str = os.getenv("PROMOTIONS_CSV_URL", "")
    watsons_store_list_url: str = os.getenv(
        "WATSONS_STORE_LIST_URL",
        "https://www.watsons.com.tw/storedescription",
    )
    watsons_openchat_url: str = os.getenv(
        "WATSONS_OPENCHAT_URL",
        "https://www.watsons.com.tw/openchat",
    )
    watsons_fetch_timeout: int = int(os.getenv("WATSONS_FETCH_TIMEOUT", "90"))
    watsons_fetch_retries: int = int(os.getenv("WATSONS_FETCH_RETRIES", "2"))
    hermes_bin: str = os.getenv("HERMES_BIN", "hermes")
    hermes_workdir: str = os.getenv("HERMES_WORKDIR", ".")

    openai_base_url: str = os.getenv("OPENAI_COMPATIBLE_BASE_URL", "https://api.openai.com/v1")
    openai_api_key: str = os.getenv("OPENAI_COMPATIBLE_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_COMPATIBLE_MODEL", "gpt-4.1-mini")


settings = Settings()
