import re
import os
import textwrap
import time
import yaml
import feedparser
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any, Optional
import json
from pathlib import Path

TAIPEI_TZ = timezone(timedelta(hours=8))


# -----------------------------
# Config / Fetch
# -----------------------------
def load_config(path: str = "config.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_rss(urls: List[str], lookback_hours: int = 48) -> List[dict]:
    cutoff = datetime.now(TAIPEI_TZ) - timedelta(hours=lookback_hours)
    items: List[dict] = []

    for url in urls:
        d = feedparser.parse(url)
        for e in d.entries[:160]:
            if hasattr(e, "published_parsed") and e.published_parsed:
                published = datetime.fromtimestamp(
                    time.mktime(e.published_parsed), tz=timezone.utc
                ).astimezone(TAIPEI_TZ)
            else:
                published = datetime.now(TAIPEI_TZ)

            if published < cutoff:
                continue

            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            summary = getattr(e, "summary", "").strip()

            if title and link:
                items.append(
                    {
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "published": published,
                    }
                )

    # de-dup by link
    seen = set()
    deduped = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        deduped.append(it)

    return deduped


def strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _text_blob(item: dict) -> str:
    title = str(item.get("title", ""))
    summary = strip_html(str(item.get("summary", "")))
    return f"{title} {summary}".lower()


# -----------------------------
# Guards
# -----------------------------
def guard_pass(item: dict, guard: dict) -> bool:
    """
    Hard constraint filter for a topic.

    - must_include_any: if provided, at least one term must appear in title/summary.
    - must_not_include_any: if any term appears, reject.
    """
    if not guard:
        return True

    blob = _text_blob(item)
    must = [s.lower() for s in (guard.get("must_include_any", []) or []) if s]
    blocked = [s.lower() for s in (guard.get("must_not_include_any", []) or []) if s]

    if must:
        if not any(m in blob for m in must):
            return False

    if blocked:
        if any(b in blob for b in blocked):
            return False

    return True


# -----------------------------
# Threads Radar (stub: safe default)
# -----------------------------
def fetch_threads_trending_tw() -> List[str]:
    # TODO: Replace with real collector (TW)
    return []


def fetch_threads_trending_global() -> List[str]:
    # TODO: Replace with real collector (Global)
    return []


def map_threads_terms_to_topics(
    terms: List[str], topics: List[dict], max_per_topic: int = 3
) -> Dict[str, List[str]]:
    """
    Map Threads terms to topic ids by simple overlap with topic keywords/guard.must_include_any.
    Rule-based to avoid hallucination.
    """
    topic_terms: Dict[str, List[str]] = {}
    enabled_topics = [t for t in topics if t.get("enabled", True)]
    for t in enabled_topics:
        tid = t.get("id", t.get("name", "topic"))
        topic_terms[tid] = []

    for term in terms:
        term_l = term.lower().strip()
        if not term_l:
            continue

        for t in enabled_topics:
            tid = t.get("id", t.get("name", "topic"))
            keys = (t.get("keywords") or []) + (t.get("guard", {}).get("must_include_any") or [])
            related = False
            for k in keys:
                kl = str(k).lower().strip()
                if not kl:
                    continue
                if kl in term_l or term_l in kl:
                    related = True
                    break

            if related and len(topic_terms[tid]) < max_per_topic:
                topic_terms[tid].append(term)

    return topic_terms


# -----------------------------
# Scoring
# -----------------------------
def score_item(
    item: dict, base_keywords: List[str], radar_terms: Optional[List[str]] = None
) -> Tuple[float, List[str], List[str]]:
    """
    Keyword scoring with optional Threads radar terms.

    - base keyword title hit: +2
    - base keyword text hit: +1
    - radar term title hit: +0.8
    - radar term text hit: +0.4
    """
    radar_terms = radar_terms or []

    title = (item.get("title") or "").lower()
    text = _text_blob(item)

    base_hits: List[str] = []
    radar_hits: List[str] = []
    score = 0.0

    def _add_hit(hit_list: List[str], term: str):
        if term not in hit_list:
            hit_list.append(term)

    for k in base_keywords or []:
        kl = str(k).lower().strip()
        if not kl:
            continue
        if kl in title:
            score += 2.0
            _add_hit(base_hits, str(k))
        elif kl in text:
            score += 1.0
            _add_hit(base_hits, str(k))

    for rt in radar_terms:
        rl = str(rt).lower().strip()
        if not rl:
            continue
        if rl in title:
            score += 0.8
            _add_hit(radar_hits, str(rt))
        elif rl in text:
            score += 0.4
            _add_hit(radar_hits, str(rt))

    return score, base_hits, radar_hits


# -----------------------------
# Fallback Strategy
# -----------------------------
def _is_ai_official(link: str) -> bool:
    lk = (link or "").lower()
    allow_domains = [
        "openai.com/",
        "blog.google/",
        "research.google/",
        "deepmind.google/",
        "nvidianews.nvidia.com/",
        "nvidia.com/",
        "microsoft.com/",
        "azure.microsoft.com/",
        "anthropic.com/",
        "meta.com/",
        "about.meta.com/",
    ]
    return any(d in lk for d in allow_domains)


def pick_fallback_item(items: List[dict], topic: dict, used_links: set) -> Optional[dict]:
    """
    Pick ONE low-risk fallback item for a topic when strict rules find nothing.

    Policy:
    - watsons_tw: allow competitor fallback (drugstore/retail competitors only)
    - accounting: NO semantic fallback (return None)
    - ai_major: ONLY official AI company sources
    """
    tid = topic.get("id", "")
    text_items = [(it, _text_blob(it)) for it in items if it.get("link") not in used_links]

    if tid == "accounting":
        return None

    if tid == "ai_major":
        for it, _ in text_items:
            if _is_ai_official(it.get("link", "")):
                return it
        return None

    if tid == "watsons_tw":
        competitors = [
            "康是美", "寶雅", "松本清", "tomod", "tomod's", "日藥本舖", "大樹", "大樹藥局", "杏一"
        ]
        context = [
            "藥妝", "藥局", "通路", "門市", "展店", "開幕", "關店", "營收", "財報", "零售", "據點", "商圈"
        ]
        for it, blob in text_items:
            if any(c.lower() in blob for c in [x.lower() for x in competitors]) and any(
                k.lower() in blob for k in [x.lower() for x in context]
            ):
                return it
        return None

    return None


# -----------------------------
# Picker
# -----------------------------
def pick_by_topic(
    items: List[dict],
    topics: List[dict],
    max_items: int,
    min_per_topic: int,
    topic_radar_terms: Dict[str, List[str]],
) -> List[dict]:
    """
    Select items per topic (topic-by-topic).
    Ensures each enabled topic has at least min_per_topic items if possible.
    If not available, try fallback (policy-based). If still not, use placeholder.

    Each picked entry:
      {
        "topic_id": ...,
        "topic_name": ...,
        "score": ...,
        "item": {...} or None,
        "base_hits": [...],
        "radar_hits": [...],
        "used_radar_terms": [...],
        "is_fallback": bool
      }
    """
    picked_entries: List[dict] = []
    enabled_topics = [t for t in topics if t.get("enabled", True)]
    if not enabled_topics:
        return picked_entries

    per_topic_ranked: Dict[str, List[dict]] = {}

    for t in enabled_topics:
        tid = t.get("id", t.get("name", "topic"))
        tname = t.get("name", tid)
        tmin = float(t.get("min_score", 0))
        tkeywords = t.get("keywords") or []
        tguard = t.get("guard") or {}

        radar_terms = topic_radar_terms.get(tid, [])

        ranked = []
        for it in items:
            if not guard_pass(it, tguard):
                continue

            s, base_hits, radar_hits = score_item(it, tkeywords, radar_terms=radar_terms)
            if s < tmin:
                continue

            ranked.append(
                {
                    "topic_id": tid,
                    "topic_name": tname,
                    "score": s,
                    "item": it,
                    "base_hits": base_hits,
                    "radar_hits": radar_hits,
                    "used_radar_terms": radar_terms,
                    "is_fallback": False,
                }
            )

        ranked.sort(key=lambda x: x["score"], reverse=True)
        per_topic_ranked[tid] = ranked

    used_links = set()

    # Pass 1: guarantee min_per_topic per topic, with fallback
    for t in enabled_topics:
        tid = t.get("id", t.get("name", "topic"))
        tname = t.get("name", tid)
        ranked = per_topic_ranked.get(tid, [])

        count = 0
        for cand in ranked:
            link = cand["item"]["link"]
            if link in used_links:
                continue
            picked_entries.append(cand)
            used_links.add(link)
            count += 1
            if count >= min_per_topic:
                break

        if count < min_per_topic:
            fb = pick_fallback_item(items, t, used_links)
            if fb:
                picked_entries.append(
                    {
                        "topic_id": tid,
                        "topic_name": tname,
                        "score": 0.5,
                        "item": fb,
                        "base_hits": [],
                        "radar_hits": [],
                        "used_radar_terms": topic_radar_terms.get(tid, []),
                        "is_fallback": True,
                    }
                )
                used_links.add(fb["link"])
            else:
                picked_entries.append(
                    {
                        "topic_id": tid,
                        "topic_name": tname,
                        "score": 0.0,
                        "item": None,
                        "base_hits": [],
                        "radar_hits": [],
                        "used_radar_terms": topic_radar_terms.get(tid, []),
                        "is_fallback": False,
                    }
                )

    # Pass 2: fill remaining slots with best strict items
    def _real_count() -> int:
        return len([p for p in picked_entries if p.get("item") is not None])

    if _real_count() < max_items:
        remaining = []
        for _, ranked in per_topic_ranked.items():
            for cand in ranked:
                link = cand["item"]["link"]
                if link in used_links:
                    continue
                remaining.append(cand)

        remaining.sort(key=lambda x: x["score"], reverse=True)

        for cand in remaining:
            if _real_count() >= max_items:
                break
            link = cand["item"]["link"]
            if link in used_links:
                continue
            picked_entries.append(cand)
            used_links.add(link)

    return picked_entries


# -----------------------------
# Delay Tracking Storage
# -----------------------------
def _read_json(path: str, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: str, data):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def update_delayed_watchlist(
    *,
    cfg: dict,
    topics: List[dict],
    picks: List[dict],
    now: datetime,
    threads_terms_all: List[str],
) -> Dict[str, Any]:
    """
    延遲追蹤機制（48h）：
    1) 今
