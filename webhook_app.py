import os
import json
import base64
from typing import Set, Tuple, Optional

import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # e.g. lakontratw-stack/ernie-morning-brief
USERS_PATH = "data/users.json"
FAILURES_PATH = "data/push_failures.json"


def _gh_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _get_file_from_github(path: str) -> Tuple[str, Optional[str]]:
    """
    Return (raw_content, sha). If file not exists => ("", None)
    """
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return "", None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=_gh_headers(), timeout=20)
    if r.status_code == 404:
        return "", None
    r.raise_for_status()
    data = r.json()
    content_b64 = data.get("content", "")
    sha = data.get("sha")
    raw = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    return raw, sha


def _put_file_to_github(path: str, content_utf8: str, message: str, sha: Optional[str]):
    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO")
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_utf8.encode("utf-8")).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=20)
    r.raise_for_status()


def load_users_from_github() -> Tuple[Set[str], Optional[str]]:
    """
    Return (users_set, sha). If file not exists => (empty_set, None)
    """
    raw, sha = _get_file_from_github(USERS_PATH)
    if sha is None:
        return set(), None
    try:
        users = set(json.loads(raw or "[]"))
    except Exception:
        users = set()
    return users, sha


def save_users_to_github(users: Set[str], sha: Optional[str]):
    content = json.dumps(sorted(list(users)), ensure_ascii=False, indent=2)
    _put_file_to_github(
        USERS_PATH,
        content,
        message="chore: update LINE users list",
        sha=sha,
    )


@app.get("/")
def health():
    return {"ok": True, "ver": "v-users-2"}


@app.post("/webhook")
async def webhook(req: Request):
    body = await req.json()
    events = body.get("events", [])

    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise HTTPException(status_code=500, detail="Missing GITHUB_TOKEN/GITHUB_REPO in Render env")

    users, sha = load_users_from_github()
    changed = False

    for e in events:
        et = e.get("type")

        # follow
        if et == "follow":
            source = e.get("source", {}) or {}
            uid = source.get("userId")
            if uid and uid not in users:
                users.add(uid)
                changed = True

        # unfollow
        if et == "unfollow":
            source = e.get("source", {}) or {}
            uid = source.get("userId")
            if uid and uid in users:
                users.remove(uid)
                changed = True

    if changed:
        save_users_to_github(users, sha)

    return {"ok": True, "users": len(users), "updated": changed}


@app.get("/users")
def users_count():
    users, _ = load_users_from_github()
    return {"count": len(users)}


@app.get("/failures")
def failures():
    raw, _sha = _get_file_from_github(FAILURES_PATH)
    if not raw:
        return {"count": 0, "items": {}}
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    return {"count": len(data), "items": data}
