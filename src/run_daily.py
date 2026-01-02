import re
import os
import textwrap
import time
import yaml
import feedparser
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any

TAIPEI_TZ = timezone(timedelta(hours=8))


def load_config(path="config.yml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_rss(urls, lookback_hours=36):
    cutoff = datetime.now(TAIPEI_TZ) - timedelta(hours=lookback_hours)
    items = []

    for url in urls:
        d = feedparser.parse(url)
        for e in d.entries[:120]:
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


def guard_pass(item: dict, guard: dict) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Hard constraint filter for a topic.

    - must_include_any: if provided, at least one term must appear in title/summary.
    - must_not_include_any: if any term appears, reject.
    """
    if not guard:
        return True, {"must_hit": [], "blocked_hit": []}

    blob = _text_blob(item)

    must = [s.lower() for s in (guard.get("must_include_any", []) or []) if s]
    blocked = [s.lower() for s in (guard.get("must_not_include_any", []) or []) if s]

    must_hit = [s for s in must if s in blob]
    blocked_hit = [s for s in blocked if s in blob]

    if must and not must_hit:
        return False, {"must_hit": must_hit, "blocked_hit": blocked_hit}

    if blocked_hit:
        return False, {"must_hit": must_hit, "blocked_hit": blocked_hit}

    return True, {"must_hit": must_hit, "blocked_hit": blocked_hit}


def score_item(item: dict, topic_keywords: List[str]) -> Tuple[int, List[str]]:
    """
    Simple keyword scoring:
      - title hit: +2
      - summary/text hit: +1

    Returns (score, hits)
    """
    title = (item.get("title") or "").lower()
    text = _text_blob(item)

    hits = []
    score = 0
    for k in topic_keywords or []:
        kl = str(k).lower().strip()
        if not kl:
            continue
        if kl in title:
            score += 2
            hits.append(k)
        elif kl in text:
            score += 1
            hits.append(k)

    # De-dup hits while preserving order
    seen = set()
    uniq_hits = []
    for h in hits:
        if h in seen:
            continue
        seen.add(h)
        uniq_hits.append(h)

    return score, uniq_hits


def pick_by_topic(items: List[dict], topics: List[dict], max_items: int, min_per_topic: int) -> List[dict]:
    """
    Select items per topic (topic-by-topic).
    Ensures each enabled topic has at least min_per_topic items if possible.
    If not available, a placeholder will be created for that topic.

    Returns a list of "picked entries" dict:
      {
        "topic_id": ...,
        "topic_name": ...,
        "score": ...,
        "item": {...} or None,
        "hits": [...],
        "guard_hits": [...]
      }
    """
    picked_entries: List[dict] = []

    enabled_topics = [t for t in topics if t.get("enabled", True)]
    if not enabled_topics:
        return picked_entries

    # Build candidates per topic
    per_topic_ranked: Dict[str, List[dict]] = {}

    for t in enabled_topics:
        tid = t.get("id", t.get("name", "topic"))
        tname = t.get("name", tid)
        tmin = int(t.get("min_score", 0))
        tkeywords = t.get("keywords") or []
        tguard = t.get("guard") or {}

        ranked = []
        for it in items:
            ok, gdetail = guard_pass(it, tguard)
            if not ok:
                continue

            s, hits = score_item(it, tkeywords)
            if s < tmin:
                continue

            ranked.append(
                {
                    "topic_id": tid,
                    "topic_name": tname,
                    "score": s,
                    "item": it,
                    "hits": hits,
                    "guard_hits": gdetail.get("must_hit", []),
                }
            )

        ranked.sort(key=lambda x: x["score"], reverse=True)
        per_topic_ranked[tid] = ranked

    # First pass: guarantee min_per_topic per topic (or placeholder)
    used_links = set()
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
            # placeholder to avoid hallucination/irrelevant fill
            picked_entries.append(
                {
                    "topic_id": tid,
                    "topic_name": tname,
                    "score": 0,
                    "item": None,
                    "hits": [],
                    "guard_hits": [],
                }
            )

    # Second pass: fill remaining slots up to max_items with best remaining across topics
    if len([p for p in picked_entries if p["item"] is not None]) < max_items:
        remaining = []
        for tid, ranked in per_topic_ranked.items():
            for cand in ranked:
                link = cand["item"]["link"]
                if link in used_links:
                    continue
                remaining.append(cand)

        remaining.sort(key=lambda x: x["score"], reverse=True)

        for cand in remaining:
            if len([p for p in picked_entries if p["item"] is not None]) >= max_items:
                break
            link = cand["item"]["link"]
            if link in used_links:
                continue
            picked_entries.append(cand)
            used_links.add(link)

    return picked_entries


def format_digest(picks: List[dict]) -> str:
    today = datetime.now(TAIPEI_TZ)
    real_count = len([p for p in picks if p.get("item") is not None])

    header = (
        f"☀️ Ernie 早安AI日報 ☀️\n"
        f"📅 {today.year}年{today.month}月{today.day}日\n\n"
        f"今天有 {real_count} 則最近值得關注的資訊分享給你 👇\n"
    )

    body_lines = []
    sources = []

    idx = 0
    for p in picks:
        topic = p["topic_name"]
        it = p.get("item")

        if it is None:
            body_lines.append(f"— {topic}\n💡 今日無符合條件的新聞（已啟用主題篩選，避免塞入無關內容）\n")
            continue

        idx += 1
        title = it["title"]
        link = it["link"]
        summary = strip_html(it.get("summary", ""))
        summary = " ".join(summary.split())
        short = textwrap.shorten(summary, width=120, placeholder="…") if summary else ""
        b1 = f"💡 主題：{topic}"
        b2 = f"💡 {short}" if short else "💡（無摘要，建議直接點開來源）"

        # explainability: show score + hits (limited length)
        hits = p.get("hits", [])[:6]
        hits_str = "、".join(hits) if hits else "—"
        score = p.get("score", 0)

        b3 = f"🔎 命中：{hits_str}｜score={score}"

        body_lines.append(f"{idx}️⃣ {title}\n{b1}\n{b2}\n{b3}\n")
        sources.append(f"[{idx}] {link}")

    footer = "━━━━━━━━━━━━━━\n📰 新聞來源：\n" + ("\n".join(sources) if sources else "（本次無可推播之來源連結）")
    return header + "\n".join(body_lines) + "\n" + footer


def line_push(message: str):
    token = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    user_id = os.environ["LINE_USER_ID"]
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def main():
    cfg = load_config("config.yml")
    rss_urls = cfg.get("sources", {}).get("rss", [])
    lookback = int(cfg.get("digest", {}).get("lookback_hours", 36))
    max_items = int(cfg.get("digest", {}).get("max_items", 5))
    min_per_topic = int(cfg.get("digest", {}).get("min_per_topic", 1))
    topics = cfg.get("topics", []) or []

    items = fetch_rss(rss_urls, lookback_hours=lookback)

    picks = pick_by_topic(items, topics, max_items=max_items, min_per_topic=min_per_topic)

    msg = format_digest(picks)
    line_push(msg)

    pushed = len([p for p in picks if p.get("item") is not None])
    print("Pushed to LINE:", pushed)


if __name__ == "__main__":
    main()
