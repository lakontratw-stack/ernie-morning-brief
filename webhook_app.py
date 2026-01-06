import os
import json
import base64
from typing import Set

import requests
from fastapi import FastAPI, Request, HTTPException

# ✅ 直接重用你現有的 digest 產生邏輯
from src.run_daily import generate_today_digest

app = FastAPI()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # e.g. lakontratw-stack/ernie-morning-brief
USERS_PATH = "data/users.json"

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")


def _gh_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load_users_from_github() -> tuple[Set[str], str | None]:
    """
    Return (users_set, sha)
    If file not exists, return (empty_set, None)
    """
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return set(), None

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{USERS_PATH}"
    r = requests.get(url, headers=_gh_headers(), timeout=20)
    if r.status_code == 404:
        return set(), None
    r.raise_for_status()
    data = r.json()
    content_b64 = data.get("content", "")
    sha = data.get("sha")
    raw = base64.b64decode(content_b64).decode("utf-8") if content_b64 else "[]"
    try:
        users = set(json.loads(raw))
    except Exception:
        users = set()
    return users, sha


def save_users_to_github(users: Set[str], sha: str | None):
    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{USERS_PATH}"
    content = json.dumps(sorted(list(users)), ensure_ascii=False, indent=2)
    payload = {
        "message": "chore: update LINE users list",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=20)
    r.raise_for_status()


def push_text_to_user(user_id: str, message: str):
    """
    Push a text message to a LINE user.
    Requires LINE_CHANNEL_ACCESS_TOKEN in env.
    """
    if not LINE_CHANNEL_ACCESS_TOKEN:
        raise RuntimeError("Missing LINE_CHANNEL_ACCESS_TOKEN in webhook service env")

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


@app.get("/")
def health():
    return {"ok": True, "ver": "v-users-2-welcome-digest"}


@app.post("/webhook")
async def webhook(req: Request):
    body = await req.json()
    events = body.get("events", [])

    # 如果沒設 GitHub 寫入，就先回錯，避免你以為收集成功但其實沒存到 repo
    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise HTTPException(status_code=500, detail="Missing GITHUB_TOKEN/GITHUB_REPO in Render env")

    users, sha = load_users_from_github()

    updated = False
    pushed = 0
    push_failed = 0

    for e in events:
        if e.get("type") != "follow":
            continue

        source = e.get("source", {}) or {}
        uid = source.get("userId")
        if not uid:
            continue

        # 1) 先確保 users.json 有記錄
        if uid not in users:
            users.add(uid)
            updated = True

        # 2) ✅ 立刻補送「新用戶版」日報（即使已存在也送，避免重加好友後仍沒內容）
        try:
            msg = generate_today_digest("config.yml", for_new_user=True)
            push_text_to_user(uid, msg)
            pushed += 1
        except Exception as ex:
            push_failed += 1
            print("新用戶補送失敗:", uid, str(ex))

    if updated:
        save_users_to_github(users, sha)

    return {"ok": True, "users": len(users), "updated": updated, "pushed": pushed, "push_failed": push_failed}


@app.get("/users")
def users_count():
    users, _ = load_users_from_github()
    return {"count": len(users)}
