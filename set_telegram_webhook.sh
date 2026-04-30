#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if [[ -x .venv/bin/python ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

"$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import os
import sys

import requests
from dotenv import load_dotenv

if not os.path.exists(".env"):
    print("找不到 .env，請先建立 TELEGRAM_BOT_TOKEN 和 PUBLIC_BASE_URL。")
    sys.exit(1)

load_dotenv(".env")
token = os.getenv("TELEGRAM_BOT_TOKEN")
base_url = os.getenv("PUBLIC_BASE_URL")
if not token or not base_url:
    print("缺少 TELEGRAM_BOT_TOKEN 或 PUBLIC_BASE_URL。")
    sys.exit(1)

webhook_url = base_url.rstrip("/") + "/webhook/telegram"
print(f"Setting Telegram webhook to: {webhook_url}")

set_resp = requests.post(
    f"https://api.telegram.org/bot{token}/setWebhook",
    data={"url": webhook_url},
    timeout=20,
)
print(set_resp.text)

print("Current Telegram webhook info:")
info_resp = requests.get(
    f"https://api.telegram.org/bot{token}/getWebhookInfo",
    timeout=20,
)
print(info_resp.text)
PY
