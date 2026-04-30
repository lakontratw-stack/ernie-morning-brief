from __future__ import annotations

import os
import sys

import requests
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_COMPATIBLE_BASE_URL")
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_COMPATIBLE_API_KEY") or os.getenv("NVIDIA_API_KEY")
    model = os.getenv("OPENAI_MODEL") or os.getenv("OPENAI_COMPATIBLE_MODEL")

    if not base_url or not api_key or not model:
        print("Missing OPENAI_BASE_URL, OPENAI_API_KEY, or OPENAI_MODEL.")
        return 2

    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": "Reply in Taiwan Traditional Chinese."},
                    {"role": "user", "content": "請用一句話回答：你可以協助 NTP 採購問題嗎？"},
                ],
                "temperature": 0.2,
                "max_tokens": 120,
            },
            timeout=60,
        )
    except requests.RequestException as exc:
        print(f"Provider connection failed: {exc}")
        return 1
    if response.status_code >= 400:
        print(f"Provider returned HTTP {response.status_code}: {response.text[:1000]}")
        return 1

    data = response.json()
    print(data["choices"][0]["message"]["content"].strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
