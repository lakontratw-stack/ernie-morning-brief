from __future__ import annotations

import json
import csv
import re
from functools import lru_cache
from html import unescape
from io import StringIO
from pathlib import Path

import requests

from .config import settings

KNOWLEDGE_PATH = Path(__file__).resolve().parent.parent / "data" / "business_knowledge.json"


@lru_cache(maxsize=1)
def load_knowledge() -> dict:
    fallback_data = _load_json_knowledge()
    official_data = _load_official_knowledge()
    sheet_data = _load_sheet_knowledge()

    return {
        **fallback_data,
        "stores": _merge_store_lists(
            official_data.get("stores", []),
            fallback_data.get("stores", []),
            sheet_data.get("stores", []),
        ),
        "promotions": sheet_data.get("promotions") or fallback_data.get("promotions", []),
    }


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


def _load_json_knowledge() -> dict:
    if not KNOWLEDGE_PATH.exists():
        return {}
    return json.loads(KNOWLEDGE_PATH.read_text(encoding="utf-8"))


def _load_official_knowledge() -> dict:
    if not settings.watsons_store_list_url:
        return {"stores": []}
    try:
        return {"stores": _load_watsons_store_list(settings.watsons_store_list_url)}
    except Exception as exc:
        print(f"Watsons official store list load failed: {exc}")
        return {"stores": []}


def _merge_store_lists(*store_lists: list[dict]) -> list[dict]:
    stores: dict[str, dict] = {}
    for store_list in store_lists:
        for incoming in store_list:
            name = (incoming.get("name") or "").strip()
            if not name:
                continue
            current = stores.setdefault(name, {"name": name, "aliases": [], "hours": []})
            current["aliases"] = _unique([*current.get("aliases", []), *incoming.get("aliases", [])])
            for key in ("address", "phone", "notes", "source_url"):
                value = incoming.get(key)
                if value:
                    current[key] = value
            if incoming.get("hours"):
                current["hours"] = incoming["hours"]
    return list(stores.values())


def _fetch_csv(url: str) -> list[dict[str, str]]:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    text = response.text.lstrip("\ufeff")
    return list(csv.DictReader(StringIO(text)))


def _fetch_text(url: str) -> str:
    response = requests.get(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
        timeout=20,
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def _load_watsons_store_list(url: str) -> list[dict]:
    html = _fetch_text(url)
    text = _html_to_text(html)
    rows = _parse_watsons_store_rows(text, url)
    if not rows:
        raise ValueError("no Watsons store rows found")
    return rows


def _html_to_text(html: str) -> str:
    html = re.sub(r"(?i)<(br|/p|/tr|/li|/h[1-6])\b[^>]*>", "\n", html)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _parse_watsons_store_rows(text: str, source_url: str) -> list[dict]:
    pattern = re.compile(
        r"^(?P<name>[\w\u4e00-\u9fff一-龥A-Za-z0-9~-]+)\s+"
        r"(?P<city>[\u4e00-\u9fff]{2,3}[市縣])\s+"
        r"(?P<district>[\u4e00-\u9fff]{2,4}[區鄉鎮市])\s+"
        r"(?P<address>.+)$"
    )
    stores: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        name = match.group("name").strip()
        address = match.group("address").strip()
        key = (name, address)
        if key in seen:
            continue
        seen.add(key)
        stores.append(
            {
                "name": name,
                "aliases": [f"屈臣氏{name}店", f"{name}門市", name],
                "address": address,
                "phone": "",
                "source_url": source_url,
                "hours": [],
                "notes": "此筆由 Watsons 官方全門市清單匯入；營業時間需以 Google Sheet 或官方門市詳細頁補齊。",
            }
        )
    return stores


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
        open_time = (row.get("open") or "").strip()
        close_time = (row.get("close") or "").strip()
        if open_time or close_time:
            store["hours"].append(
                {
                    "days": (row.get("days") or "每日").strip(),
                    "open": open_time,
                    "close": close_time,
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


def _unique(items: list[str]) -> list[str]:
    result: list[str] = []
    for item in items:
        item = item.strip()
        if item and item not in result:
            result.append(item)
    return result


def find_store(text: str) -> dict | None:
    data = load_knowledge()
    for store in data.get("stores", []):
        names = [store.get("name", ""), *store.get("aliases", [])]
        normalized_names = _store_name_variants(names)
        if any(name and name in text for name in normalized_names):
            return store
    return None


def find_store_candidates(text: str, limit: int = 5) -> list[dict]:
    data = load_knowledge()
    candidates: list[dict] = []
    for store in data.get("stores", []):
        haystack = " ".join(
            [
                store.get("name", ""),
                store.get("address", ""),
                *store.get("aliases", []),
            ]
        )
        if any(token and token in haystack for token in _query_tokens(text)):
            candidates.append(store)
        if len(candidates) >= limit:
            break
    return candidates


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
        parts = [
            "[ESCALATE:門市營業時間尚未同步]",
            f"{store.get('name', '這間門市')}我有查到門市資料，但目前還沒有同步營業時間。",
        ]
        if store.get("address"):
            parts.append(f"地址：{store['address']}")
        if store.get("source_url"):
            parts.append(f"官方門市清單：{store['source_url']}")
        parts.append("我先幫您轉專員確認，避免回覆錯誤時間。")
        return "\n".join(parts)
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


def format_store_summary(store: dict) -> str:
    parts = [f"我查到的是 {store.get('name', '這間門市')}："]
    if store.get("address"):
        parts.append(f"地址：{store['address']}")
    if store.get("phone"):
        parts.append(f"電話：{store['phone']}")
    if store.get("hours"):
        hour_lines = [
            f"{item.get('days', '營業日')}：{item.get('open')} 到 {item.get('close')}"
            for item in store["hours"]
        ]
        parts.append("營業時間：")
        parts.extend(hour_lines)
    else:
        parts.append("目前還沒有同步這間門市的營業時間；如果您要確認時間，我可以幫您轉專員。")
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


def format_store_candidates(candidates: list[dict]) -> str:
    lines = ["我找到幾間可能相關的門市，請問您要查哪一間？"]
    for store in candidates:
        line = store.get("name", "門市")
        if store.get("address"):
            line = f"{line}：{store['address']}"
        lines.append(line)
    return "\n".join(lines)


def _query_tokens(text: str) -> list[str]:
    compact = text.replace("屈臣氏", "").replace("門市", "").strip()
    tokens = re.split(r"[\s,，。？?、]+", compact)
    tokens.extend(re.findall(r"[\u4e00-\u9fff]{2,3}[市縣]", compact))
    tokens.extend(re.findall(r"[\u4e00-\u9fff]{2,4}[區鄉鎮市]", compact))
    return _unique([token for token in tokens if len(token) >= 2])


def _store_name_variants(names: list[str]) -> list[str]:
    variants: list[str] = []
    for name in names:
        variants.append(name)
        if name.endswith("店"):
            variants.append(name[:-1])
        else:
            variants.append(f"{name}店")
    return _unique(variants)
