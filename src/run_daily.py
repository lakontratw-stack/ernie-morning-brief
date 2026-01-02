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


# -----------------------------
# Threads Radar (v0: conservative stub)
# -----------------------------
def fetch_threads_trending() -> List[str]:
    """
    Return a list of trending terms from Threads.

    v0 implementation is a conservative stub to validate product behavior:
    - No post content
    - No author info
    - Just short terms (names/brands/topics)

    Replace this function with a real collector once format is validated.
    """
    return [
        "OpenAI",
        "Sam Altman",
        "AI 法",
        "資料中心",
        "NVIDIA",
        "屈臣氏",
        "康是美",
        "IFRS",
    ]


def map_threads_terms_to_topics(terms: List[str], topics: List[dict], max_per_topic: int = 3) -> Dict[str, List[str]]:
    """
    Map Threads terms to topic ids by simple overlap with topic keywords/guard.must_include_any.

    This is intentionally rule-based (no LLM) to avoid hallucination.
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
            # If term contains keyword or keyword contains term => related
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
def score_item(item: dict, base_keywords: List[str], radar_terms: List[str] = None) -> Tuple[float, List[str], List[str]]:
    """
    Keyword scoring with optional Threads radar terms.

    - base keyword title hit: +2
    - base keyword text hit: +1
    - radar term title hit: +0.8
    - radar term text hit: +0.4

    Returns (score, base_hits, radar_hits)
    """
    radar_terms = radar_terms or []

    title = (item.get("title") or "").lower()
    text = _text_blob(item)

    base_hits = []
    radar_hits = []
    score = 0.0

    def _add_hit(hit_list: List[str], term: str):
        if term not in hit_list:
            hit_list.append(term)

    # base keywords
    for k in base_keywords or []:
        kl = str(k).lower().strip()
        if not kl:
            continue
        if kl in title:
            score += 2.0
            _add_hit(base_hits, k)
        elif kl in text:
            score += 1.0
            _add_hit(base_hits, k)

    # radar terms (lower weight)
    for rt in radar_terms:
        rl = str(rt).lower().strip()
        if not rl:
            continue
        if rl in title:
            score += 0.8
            _add_hit(radar_hits, rt)
        elif rl in text:
            score += 0.4
            _add_hit(radar_hits, rt)

    return score, base_hits, radar_hits


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
    If not available, a placeholder will be created for that topic.

    Returns a list of "picked entries" dict:
      {
        "topic_id": ...,
        "topic_name": ...,
        "score": ...,
        "item": {...} or None,
        "base_hits": [...],
        "radar_hits": [...],
        "used_radar_terms": [...]
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
        tmin = float(t.get("min_score", 0))
        tkeywords = t.get("keywords") or []
        tguard = t.get("guard") or {}

        radar_terms = topic_radar_terms.get(tid, [])

        ranked = []
        for it in items:
            ok, _ = guard_pass(it, tguard)
            if not ok:
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
            picked_entries.append(
                {
                    "topic_id": tid,
                    "topic_name": tname,
                    "score": 0.0,
                    "item": None,
                    "base_hits": [],
                    "radar_hits": [],
                    "used_radar_terms": topic_radar_terms.get(tid, []),
                }
            )

    # Second pass: fill remaining slots up to max_items with best remaining across topics
    if len([p for p in picked_entries if p.get("item") is not None]) < max_items:
        remaining = []
        for tid, ranked in per_topic_ranked.items():
            for cand in ranked:
                link = cand["item"]["link"]
                if link in used_links:
                    continue
                remaining.append(cand)

        remaining.sort(key=lambda x: x["score"], reverse=True)

        for cand in remaining:
            if len([p for p in picked_entries if p.get("item") is not None]) >= max_items:
                break
            link = cand["item"]["link"]
            if link in used_links:
                continue
            picked_entries.append(cand)
            used_links.add(link)

    return picked_entries


def format_digest(picks: List[dict], threads_terms: List[str], topic_threads_terms: Dict[str, List[str]]) -> str:
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
            # still show topic and its mapped radar terms to help you tune
            mapped = topic_threads_terms.get(p.get("topic_id", ""), [])[:5]
            mapped_str = "、".join(mapped) if mapped else "（無）"
            body_lines.append(
                f"— {topic}\n"
                f"💡 今日無符合條件的新聞（已啟用主題篩選，避免塞入無關內容）\n"
                f"🔥 Threads 線索（此主題）：{mapped_str}\n"
            )
            continue

        idx += 1
        title = it["title"]
        link = it["link"]
        summary = strip_html(it.get("summary", ""))
        summary = " ".join(summary.split())
        short = textwrap.shorten(summary, width=120, placeholder="…") if summary else ""
        b1 = f"💡 主題：{topic}"
        b2 = f"💡 {short}" if short else "💡（無摘要，建議直接點開來源）"

        base_hits = p.get("base_hits", [])[:6]
        radar_hits = p.get("radar_hits", [])[:4]
        base_hits_str = "、".join(base_hits) if base_hits else "—"
        radar_hits_str = "、".join(radar_hits) if radar_hits else "—"
        score = p.get("score", 0.0)

        b3 = f"🔎 命中：{base_hits_str}｜score={score:.1f}"
        b4 = f"⚡ Threads 觸發：{radar_hits_str}"

        body_lines.append(f"{idx}️⃣ {title}\n{b1}\n{b2}\n{b3}\n{b4}\n")
        sources.append(f"[{idx}] {link}")

    # Footer: show global Threads trending terms
    threads_block = ""
    if threads_terms:
        threads_block = (
            "\n━━━━━━━━━━━━━━\n"
            "🔥 Threads 目前熱詞（雷達用，不直接當新聞）\n"
            + "、".join(threads_terms[:12])
            + "\n"
        )
    else:
        threads_block = (
            "\n━━━━━━━━━━━━━━\n"
            "🔥 Threads 目前熱詞（雷達用，不直接當新聞）\n"
            "（本次未取得）\n"
        )

    footer = "━━━━━━━━━━━━━━\n📰 新聞來源：\n" + ("\n".join(sources) if sources else "（本次無可推播之來源連結）")

    return header + "\n".join(body_lines) + threads_block + footer


def line_push(message: str):
    token = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    user_id = os.environ["LINE_USER_ID"]
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()

def generate_today_digest(cfg_path: str = "config.yml", for_new_user: bool = False) -> str:
    """
    Generate digest text only (no LINE push).
    for_new_user=True will generate a shorter digest (1 per topic) to avoid flooding.
    """
    cfg = load_config(cfg_path)
    rss_urls = cfg.get("sources", {}).get("rss", [])
    lookback = int(cfg.get("digest", {}).get("lookback_hours", 36))
    max_items = int(cfg.get("digest", {}).get("max_items", 5))
    min_per_topic = int(cfg.get("digest", {}).get("min_per_topic", 1))
    topics = cfg.get("topics", []) or []

    # New user: keep it short (1 per topic), and cap total items
    if for_new_user:
        min_per_topic = 1
        max_items = min(3, max_items)

    items = fetch_rss(rss_urls, lookback_hours=lookback)

    # Threads radar (if your current run_daily.py has it)
    threads_terms = []
    topic_threads_terms = {}
    radar_cfg = cfg.get("radar", {}).get("threads", {})
    radar_enabled = bool(radar_cfg.get("enabled", False))

    if radar_enabled:
        # If you already implemented TW/Global split, keep your existing variables here.
        # Otherwise we reuse your current fetch_threads_trending() if present.
        if "fetch_threads_trending" in globals():
            threads_terms = fetch_threads_trending()
            topic_threads_terms = map_threads_terms_to_topics(
                threads_terms,
                topics,
                max_per_topic=int(radar_cfg.get("max_terms_per_topic", 3)),
            )
        else:
            # If you have TW/Global, your format function should handle it separately.
            # Leave empty if not available.
            threads_terms = []
            topic_threads_terms = {}

    # If your code uses pick_by_topic(), keep it.
    # If not, keep your pick_top() and later we adjust.
    if "pick_by_topic" in globals():
        topic_radar_terms = topic_threads_terms if radar_enabled else {t.get("id"): [] for t in topics}
        picks = pick_by_topic(
            items,
            topics,
            max_items=max_items,
            min_per_topic=min_per_topic,
            topic_radar_terms=topic_radar_terms,
        )

        # Prefer your existing formatter if present
        if "format_digest" in globals():
            try:
                return format_digest(picks, threads_terms=threads_terms, topic_threads_terms=topic_threads_terms)
            except TypeError:
                # fallback for older signature
                return format_digest(picks)
        return format_digest(picks)

    # Fallback to older v1 logic
    picked = pick_top(items, topics, max_items=max_items)
    return format_digest(picked)


def push_digest_to_user(user_id: str, message: str):
    """
    Push digest to a specific LINE user_id.
    """
    token = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def main():
    msg = generate_today_digest("config.yml", for_new_user=False)
    line_push(msg)
    print("Pushed to LINE.")


    items = fetch_rss(rss_urls, lookback_hours=lookback)

    # Threads Radar
    threads_terms = fetch_threads_trending()
    topic_threads_terms = map_threads_terms_to_topics(
        threads_terms,
        topics,
        max_per_topic=int(cfg.get("radar", {}).get("threads", {}).get("max_terms_per_topic", 3))
        if cfg.get("radar", {}).get("threads", {}).get("enabled", False)
        else 3,
    )

    # If radar is disabled in config, still show the block (empty) but do not influence scoring.
    radar_enabled = bool(cfg.get("radar", {}).get("threads", {}).get("enabled", False))
    topic_radar_terms = topic_threads_terms if radar_enabled else {t.get("id"): [] for t in topics}

def pick_fallback_item(items: List[dict], topic: dict) -> dict | None:
    """
    Pick ONE low-risk fallback item for a topic when strict rules find nothing.
    This does NOT use keywords scoring, only broad semantic guards.
    """
    tid = topic.get("id", "")
    text_items = [(it, _text_blob(it)) for it in items]

    if tid == "accounting":
        hints = ["財經", "公司", "財務", "金融", "監管"]
    elif tid == "ai_major":
        hints = ["科技", "ai", "人工智慧", "晶片", "半導體"]
    elif tid == "watsons_tw":
        hints = ["零售", "通路", "藥局", "門市", "消費"]
    else:
        return None

    for it, blob in text_items:
        if any(h in blob for h in hints):
            return it

    return None

    
    picks = pick_by_topic(
        items,
        topics,
        max_items=max_items,
        min_per_topic=min_per_topic,
        topic_radar_terms=topic_radar_terms,
    )

    msg = format_digest(picks, threads_terms=threads_terms, topic_threads_terms=topic_threads_terms)
    line_push(msg)

    pushed = len([p for p in picks if p.get("item") is not None])
    print("Pushed to LINE:", pushed)


if __name__ == "__main__":
    main()
