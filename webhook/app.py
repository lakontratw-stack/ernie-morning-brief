import json
from pathlib import Path
from typing import Set

from fastapi import FastAPI, Request

app = FastAPI()

STORE_PATH = Path("data")
STORE_PATH.mkdir(exist_ok=True)
USERS_FILE = STORE_PATH / "users.json"


def load_users() -> Set[str]:
    if USERS_FILE.exists():
        try:
            return set(json.loads(USERS_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def save_users(users: Set[str]) -> None:
    USERS_FILE.write_text(json.dumps(sorted(list(users))), encoding="utf-8")


@app.get("/")
def health():
    return {"ok": True}


@app.post("/webhook")
async def webhook(req: Request):
    body = await req.json()

    events = body.get("events", [])
    users = load_users()

    for e in events:
        etype = e.get("type")
        source = e.get("source", {}) or {}
        user_id = source.get("userId")

        if etype == "follow" and user_id:
            users.add(user_id)

    save_users(users)
    return {"ok": True, "users": len(users)}
