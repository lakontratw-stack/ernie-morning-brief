from __future__ import annotations

import json
import csv
from functools import lru_cache
from io import StringIO
from pathlib import Path

import requests

from .config import settings

KNOWLEDGE_PATH = Path(__file__).resolve().parent.parent / "data" / "business_knowledge.json"


@lru_cache(maxsize=1)
def load_knowledge() -> dict:
    sheet_data = _load_sheet_knowledge()
    if sheet_data.get("stores") or sheet_data.get("promotions"):
        return _merge_with_json_fallback(sheet_data)

    if not KNOWLEDGE_PATH.exists():
        return {}
    return json.loads(KNOWLEDGE_PATH.read_text(encoding="utf-8"))


def refresh_knowledge() -> None:
    load_knowledge.cache_clear()


def _load_sheet_knowledge() -> dict:
    data: dict = {"stores": [], "promotions": []}
    if settings.store_hours_csv_url:
        try:
            data["stores"] = _load_store_hours_csv(settings.store_hours_csv_url)
        except Exception as exc:
            print(f"store hours sheet load failed: {exc}")
    if settings.promotions_csv_url:
        try:
            data["promotions"] = _load_promotions_csv(settings.promotions_csv_url)
        except Exception as exc:
            print(f"promotions sheet load failed: {exc}")
    return data


def _merge_with_json_fallback(sheet_data: dict) -> dict:
    fallback_data = {}
    if KNOWLEDGE_PATH.exists():
        fallback_data = json.loads(KNOWLEDGE_PATH.read_text(encoding="utf-8"))
    return {
        **fallback_data,
        "stores": sheet_data.get("stores") or fallback_data.get("stores", []),
        "promotions": sheet_data.get("promotions") or fallback_data.get("promotions", []),
    }


def _fetch_csv(url: str) -> list[dict[str, str]]:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    text = response.text.lstrip("\ufeff")
    return list(csv.DictReader(StringIO(text)))


def _load_store_hours_csv(url: str) -> list[dict]:
    rows = _fetch_csv(url)
    stores: dict[str, dict] = {}
    for row in rows:
        name = (row.get("store_name") or "").strip()
        if not name:
            continue
        store = stores.setdefault(
            name,
            {
                "name": name,
                "aliases": _split_aliases(row.get("aliases")),
                "address": (row.get("address") or "").strip(),
                "phone": (row.get("phone") or "").strip(),
                "source_url": (row.get("source_url") or "").strip(),
                "hours": [],
                "notes": (row.get("notes") or "").strip(),
            },
        )
        store["hours"].append(
            {
                "days": (row.get("days") or "每日").strip(),
                "open": (row.get("open") or "").strip(),
                "close": (row.get("close") or "").strip(),
            }
        )
    return list(stores.values())


def _load_promotions_csv(url: str) -> list[dict]:
    rows = _fetch_csv(url)
    promotions = []
    for row in rows:
        title = (row.get("title") or "").strip()
        if not title:
            continue
        promotions.append(
            {
                "title": title,
                "period": (row.get("period") or "").strip(),
                "summary": (row.get("summary") or "").strip(),
                "details": (row.get("details") or "").strip(),
                "status": (row.get("status") or "active").strip() or "active",
            }
        )
    return promotions


def _split_aliases(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(",", ";").split(";") if item.strip()]


def find_store(text: str) -> dict | None:
    data = load_knowledge()
    for store in data.get("stores", []):
        names = [store.get("name", ""), *store.get("aliases", [])]
        if any(name and name in text for name in names):
            return store
    return None


def active_promotions() -> list[dict]:
    data = load_knowledge()
    return [
        promo
        for promo in data.get("promotions", [])
        if promo.get("status", "active") != "inactive"
    ]


def fallback(key: str) -> str:
    return load_knowledge().get("fallbacks", {}).get(key, "")


def format_store_hours(store: dict) -> str:
    hours = store.get("hours", [])
    if not hours:
        return f"{store.get('name', '這間門市')}目前沒有設定營業時間，建議幫您轉專員確認。"
    hour_lines = [
        f"{item.get('days', '營業日')}：{item.get('open')} 到 {item.get('close')}"
        for item in hours
    ]
    parts = [
        f"{store.get('name')}的營業時間是：",
        *hour_lines,
    ]
    if store.get("address") and not store["address"].startswith("請在這裡"):
        parts.append(f"地址：{store['address']}")
    if store.get("phone") and not store["phone"].startswith("請在這裡"):
        parts.append(f"電話：{store['phone']}")
    if store.get("notes"):
        parts.append(store["notes"])
    if store.get("source_url"):
        parts.append(f"官方門市頁：{store['source_url']}")
    return "\n".join(parts)


def format_promotions(promotions: list[dict]) -> str:
    if not promotions:
        return fallback("promotion") or "目前沒有設定中的活動資訊。"
    lines = ["目前可以先提供這些活動資訊："]
    for promo in promotions[:3]:
        lines.append(f"{promo.get('title')}")
        if promo.get("period"):
            lines.append(f"期間：{promo['period']}")
        if promo.get("summary"):
            lines.append(promo["summary"])
        if promo.get("details"):
            lines.append(f"注意事項：{promo['details']}")
    return "\n".join(lines)
