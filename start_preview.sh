#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

.venv/bin/pip install -r requirements.txt
APP_PORT="${PORT:-8000}" .venv/bin/python app.py
