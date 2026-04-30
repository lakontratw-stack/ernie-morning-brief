#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn lyra_line.app:app --host 127.0.0.1 --port "${PORT:-8000}"
