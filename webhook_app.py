import os
import json
import base64
from typing import Set

import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # e.g. lakontratw-stack/ernie-morning-brief
USERS_PATH = "data/users.json"


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


@app.get("/")
def health():
    return {"ok": True}


@app.post("/webhook")
async def webhook(req: Request):
    body = await req.json()
    events = body.get("events", [])

    # 如果沒設 GitHub 寫入，就先回錯，避免你以為收集成功但其實沒存到 repo
    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise HTTPException(status_code=500, detail="Missing GITHUB_TOKEN/GITHUB_REPO in Render env")

    users, sha = load_users_from_github()

    changed = False
    for e in events:
        if e.get("type") != "follow":
            continue
        source = e.get("source", {}) or {}
        uid = source.get("userId")
        if uid and uid not in users:
            users.add(uid)
            changed = True

    if changed:
        save_users_to_github(users, sha)

    return {"ok": True, "users": len(users), "updated": changed}
