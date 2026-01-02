import os
import textwrap
import time
import yaml
import feedparser
import requests
from datetime import datetime, timezone, timedelta

TAIPEI_TZ = timezone(timedelta(hours=8))


def load_config(path="config.yml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_rss(urls, lookback_hours=36):
    cutoff = datetime.now(TAIPEI_TZ) - timedelta(hours=lookback_hours)
    items = []
    for url in urls:
        d = feedparser.parse(url)
        for e in d.entries[:80]:
            # published_parsed may be missing; fallback to now
            if hasattr(e, "published_parsed") and e.published_parsed:
                published = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=timezone.utc).astimezone(TAIPEI_TZ)
            else:
                published = datetime.now(TAIPEI_TZ)

            if published < cutoff:
                continue

            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            summary = getattr(e, "summary", "").strip()

            if title and link:
                items.append({
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "published": published.isoformat()
                })
    # de-dup by link
    seen = set()
    deduped = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        deduped.append(it)
    return deduped


def score_item(item, topic_keywords):
    text = (item["title"] + " " + item.get("summary", "")).lower()
    hits = sum(1 for k in topic_keywords if k.lower() in text)
    # very simple scoring for free MVP
    return hits


def pick_top(items, topics, max_items=5):
    scored = []
    for it in items:
        best = None
        for t in topics:
            if not t.get("enabled", True):
                continue
            s = score_item(it, t.get("keywords", []))
            if s >= t.get("min_score", 0):
                if best is None or s > best["score"]:
                    best = {"topic": t["name"], "score": s}
        if best:
            scored.append((best["score"], best["topic"], it))

    scored.sort(key=lambda x: x[0], reverse=True)
    picked = scored[:max_items]
    return picked


def format_digest(picked):
    today = datetime.now(TAIPEI_TZ)
    header = f"☀️ Ernie 早安AI日報 ☀️\n📅 {today.year}年{today.month}月{today.day}日\n\n今天有 {len(picked)} 則最近值得關注的 AI 自動化收集的最新資訊分享給你 👇\n"
    body_lines = []
    sources = []
    for idx, (score, topic, it) in enumerate(picked, start=1):
        title = it["title"]
        link = it["link"]
        # make 2 short bullets from title/summary (rule-based)
        s = it.get("summary", "")
        s = " ".join(s.split())
        short = textwrap.shorten(s, width=120, placeholder="…") if s else ""
        b1 = f"💡 主題：{topic}"
        b2 = f"💡 {short}" if short else "💡（無摘要，建議直接點開來源）"
        body_lines.append(f"{idx}️⃣ {title}\n{b1}\n{b2}\n")
        sources.append(f"[{idx}] {link}")

    footer = "━━━━━━━━━━━━━━\n📰 新聞來源：\n" + "\n".join(sources)
    return header + "\n".join(body_lines) + "\n" + footer


def line_push(message: str):
    token = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    user_id = os.environ["LINE_USER_ID"]
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}  # LINE text length safety
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def main():
    cfg = load_config("config.yml")
    rss_urls = cfg.get("sources", {}).get("rss", [])
    lookback = int(cfg.get("digest", {}).get("lookback_hours", 36))
    max_items = int(cfg.get("digest", {}).get("max_items", 5))
    topics = cfg.get("topics", [])

    items = fetch_rss(rss_urls, lookback_hours=lookback)
    picked = pick_top(items, topics, max_items=max_items)
    msg = format_digest(picked)
    line_push(msg)
    print("Pushed to LINE:", len(picked))


if __name__ == "__main__":
    main()
