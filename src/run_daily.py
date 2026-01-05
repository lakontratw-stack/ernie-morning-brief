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
import base64

TAIPEI_TZ = timezone(timedelta(hours=8))

# GitHub repo (for persistence)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # e.g. lakontratw-stack/ernie-morning-brief


# -----------------------------
# GitHub Contents API helpers
# -----------------------------
def _gh_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def gh_get_text(path: str) -> Tuple[str, Optional[str]]:
    """
    Return (raw_text, sha). If 404 => ("", None)
    """
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return "", None

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=_gh_headers(), timeout=25)
    if r.status_code == 404:
        return "", None
    r.raise_for_status()
    data = r.json()
    content_b64 = data.get("content", "") or ""
    sha = data.get("sha")
    raw = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    return raw, sha


def gh_put_text(path: str, text_utf8: str, message: str, sha: Optional[str] = None):
    if not (GITHUB_TOKEN and GITHUB_REPO):
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(text_utf8.encode("utf-8")).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=25)
    r.raise_for_status()


def gh_read_json(path: str, default):
    raw, _sha = gh_get_text(path)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def gh_write_json(path: str, data, message: str):
    raw_old, sha = gh_get_text(path)
    content = json.dumps(data, ensure_ascii=False, indent=2)
    gh_put_text(path, content, message=message, sha=sha)


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
    item: dict,
    base_keywords: List[str],
    radar_terms: Optional[List[str]] = None,
    low_weight_keywords: Optional[List[str]] = None,
) -> Tuple[float, List[str], List[str]]:
    """
    Keyword scoring with optional Threads radar terms.

    - base keyword title hit: +2
    - base keyword text hit: +1
    - low-weight keyword title hit: +0.5
    - low-weight keyword text hit: +0.2
    - radar term title hit: +0.8
    - radar term text hit: +0.4
    """
    radar_terms = radar_terms or []
    low_weight_keywords = low_weight_keywords or []

    title = (item.get("title") or "").lower()
    text = _text_blob(item)

    base_hits: List[str] = []
    radar_hits: List[str] = []
    score = 0.0

    def _add_hit(hit_list: List[str], term: str):
        if term not in hit_list:
            hit_list.append(term)

    low_set = {str(x).lower().strip() for x in low_weight_keywords if str(x).strip()}

    for k in base_keywords or []:
        kl_raw = str(k)
        kl = kl_raw.lower().strip()
        if not kl:
            continue

        is_low = kl in low_set

        if kl in title:
            score += (0.5 if is_low else 2.0)
            _add_hit(base_hits, kl_raw)
        elif kl in text:
            score += (0.2 if is_low else 1.0)
            _add_hit(base_hits, kl_raw)

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
        tlow = t.get("low_weight_keywords") or []
        tguard = t.get("guard") or {}

        radar_terms = topic_radar_terms.get(tid, [])

        ranked = []
        for it in items:
            if not guard_pass(it, tguard):
                continue

            s, base_hits, radar_hits = score_item(it, tkeywords, radar_terms=radar_terms, low_weight_keywords=tlow)
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
# Delay Tracking (GitHub persistence)
# -----------------------------
def update_delayed_watchlist(
    *,
    cfg: dict,
    picks: List[dict],
    now: datetime,
    threads_terms_all: List[str],
) -> Dict[str, Any]:
    delay_cfg = (cfg.get("radar", {}) or {}).get("delay_tracking", {}) or {}
    enabled = bool(delay_cfg.get("enabled", False))
    if not enabled:
        return {"enabled": False}

    storage_path = str(delay_cfg.get("storage_path", "data/delayed_watch.json"))
    delay_hours = int(delay_cfg.get("delay_hours", 48))
    max_candidates_per_topic = int(delay_cfg.get("max_candidates_per_topic", 3))
    min_score_to_watch = float(delay_cfg.get("min_score_to_watch", 2.5))

    state = gh_read_json(storage_path, {"watching": [], "done": []})
    watching: List[dict] = state.get("watching", []) or []
    done: List[dict] = state.get("done", []) or []

    seen_links = set()
    for it in watching:
        if it.get("link"):
            seen_links.add(it["link"])
    for it in done:
        if it.get("link"):
            seen_links.add(it["link"])

    # 1) collect candidates from today's picks
    topic_counts: Dict[str, int] = {}
    for p in picks:
        it = p.get("item")
        if not it:
            continue
        score = float(p.get("score", 0.0))
        if score < min_score_to_watch:
            continue

        link = it.get("link")
        if not link or link in seen_links:
            continue

        tid = p.get("topic_id") or "unknown"
        topic_counts[tid] = topic_counts.get(tid, 0) + 1
        if topic_counts[tid] > max_candidates_per_topic:
            continue

        due_at = (now + timedelta(hours=delay_hours)).isoformat()
        watching.append(
            {
                "topic_id": tid,
                "topic_name": p.get("topic_name"),
                "title": it.get("title"),
                "link": link,
                "published": (
                    it.get("published").isoformat()
                    if hasattr(it.get("published"), "isoformat")
                    else None
                ),
                "saved_at": now.isoformat(),
                "due_at": due_at,
                "score": score,
                "base_hits": p.get("base_hits", []),
            }
        )
        seen_links.add(link)

    # 2) evaluate due items (simple string match; your collector can improve later)
    terms = [str(x).strip() for x in (threads_terms_all or []) if str(x).strip()]
    terms_l = [t.lower() for t in terms]

    remained: List[dict] = []
    matured = 0
    fermented = 0

    for w in watching:
        due_at_s = w.get("due_at")
        try:
            due_at = datetime.fromisoformat(due_at_s)
        except Exception:
            remained.append(w)
            continue

        if due_at > now:
            remained.append(w)
            continue

        matured += 1
        blob = f"{w.get('title','')} {w.get('link','')}".lower()
        matched_terms = []
        for t in terms_l:
            if t and t in blob:
                matched_terms.append(t)

        is_fermented = bool(matched_terms)
        if is_fermented:
            fermented += 1

        w2 = dict(w)
        w2["checked_at"] = now.isoformat()
        w2["threads_matched_terms"] = matched_terms[:10]
        w2["fermented"] = is_fermented
        done.append(w2)

    # cap history
    state2 = {"watching": remained, "done": done[-500:]}
    gh_write_json(storage_path, state2, message="chore: update delayed watchlist")

    return {
        "enabled": True,
        "storage_path": storage_path,
        "added_today": sum(topic_counts.values()),
        "matured_checked": matured,
        "fermented": fermented,
        "watching_now": len(remained),
    }


# -----------------------------
# Push Failure Tracking (GitHub persistence)
# -----------------------------
USERS_PATH = "data/users.json"


def load_repo_users() -> List[str]:
    users = gh_read_json(USERS_PATH, [])
    if isinstance(users, list):
        return users
    return []


def save_repo_users(users: List[str]):
    gh_write_json(USERS_PATH, sorted(list(dict.fromkeys(users))), message="chore: update LINE users list")


def record_push_failure(cfg: dict, uid: str, err: Exception, now: datetime):
    ft = (cfg.get("push", {}) or {}).get("failure_tracking", {}) or {}
    if not bool(ft.get("enabled", True)):
        return

    path = str(ft.get("storage_path", "data/push_failures.json"))
    data = gh_read_json(path, {})

    rec = (data.get(uid, {}) or {}) if isinstance(data, dict) else {}
    count = int(rec.get("fail_count", 0)) + 1

    if not isinstance(data, dict):
        data = {}

    data[uid] = {
        "fail_count": count,
        "last_failed_at": now.isoformat(),
        "last_error": str(err)[:500],
    }

    gh_write_json(path, data, message="chore: update push failures")

    auto_remove = bool(ft.get("auto_remove_user", False))
    threshold = int(ft.get("auto_remove_threshold", 3))
    if auto_remove and count >= threshold:
        users = load_repo_users()
        if uid in users:
            users = [x for x in users if x != uid]
            save_repo_users(users)


# -----------------------------
# Formatter
# -----------------------------
def format_digest(
    picks: List[dict],
    threads_tw: List[str],
    threads_global: List[str],
    topic_threads_terms: Dict[str, List[str]],
    delay_status: Optional[Dict[str, Any]] = None,
) -> str:
    today = datetime.now(TAIPEI_TZ)

    strict_cnt = len([p for p in picks if p.get("item") is not None and not p.get("is_fallback", False)])
    fallback_cnt = len([p for p in picks if p.get("item") is not None and p.get("is_fallback", False)])
    blank_topic_cnt = len([p for p in picks if p.get("item") is None])
    real_count = len([p for p in picks if p.get("item") is not None])

    header = (
        f"☀️ Ernie 早安AI日報 ☀️\n"
        f"📅 {today.year}年{today.month}月{today.day}日\n"
        f"📌 今日狀態摘要：嚴格命中 {strict_cnt} 則｜保底 {fallback_cnt} 則｜空白 {blank_topic_cnt} 主題\n\n"
        f"今天有 {real_count} 則最近值得關注的資訊分享給你 👇\n"
    )

    body_lines: List[str] = []
    sources: List[str] = []
    idx = 0

    for p in picks:
        topic = p["topic_name"]
        it = p.get("item")

        if it is None:
            mapped = topic_threads_terms.get(p.get("topic_id", ""), [])[:5]
            mapped_str = "、".join(mapped) if mapped else "（無）"
            body_lines.append(
                f"— {topic}\n"
                f"💡 今日無符合條件的新聞（此主題採嚴格篩選，避免塞入無關內容）\n"
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

        score = float(p.get("score", 0.0))
        base_hits = p.get("base_hits", [])[:6]
        radar_hits = p.get("radar_hits", [])[:4]
        base_hits_str = "、".join(base_hits) if base_hits else "—"
        radar_hits_str = "、".join(radar_hits) if radar_hits else "—"

        lines = [f"{idx}️⃣ {title}", b1, b2]

        if p.get("is_fallback", False):
            if p.get("topic_id") == "ai_major":
                lines.append("🟡 保底快訊（官方來源，未命中嚴格關鍵字）")
            else:
                lines.append("🟡 保底新聞（補足主題資訊，未命中嚴格關鍵字）")

        lines.append(f"🔎 命中：{base_hits_str}｜score={score:.1f}")
        lines.append(f"⚡ Threads 觸發：{radar_hits_str}")
        body_lines.append("\n".join(lines) + "\n")

        sources.append(f"[{idx}] {link}")

    delay_block = ""
    if delay_status and delay_status.get("enabled"):
        delay_block = (
            "\n━━━━━━━━━━━━━━\n"
            "🕒 延遲追蹤（48h）狀態\n"
            f"今日新增：{delay_status.get('added_today', 0)}｜到期檢查：{delay_status.get('matured_checked', 0)}｜判定發酵：{delay_status.get('fermented', 0)}｜待追數：{delay_status.get('watching_now', 0)}\n"
        )

    threads_block = (
        "\n━━━━━━━━━━━━━━\n"
        "🔥 Threads 熱詞（雷達用，不直接當新聞）\n"
        f"台灣：{('、'.join(threads_tw[:12]) if threads_tw else '（本次未取得）')}\n"
        f"全球：{('、'.join(threads_global[:12]) if threads_global else '（本次未取得）')}\n"
    )

    footer = "━━━━━━━━━━━━━━\n📰 新聞來源：\n" + ("\n".join(sources) if sources else "（本次無可推播之來源連結）")
    return header + "\n".join(body_lines) + delay_block + threads_block + footer


# -----------------------------
# LINE Push
# -----------------------------
def push_text_to_user(user_id: str, message: str):
    token = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": message[:4900]}]}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


# -----------------------------
# Digest generation
# -----------------------------
def generate_today_digest(cfg_path: str = "config.yml", for_new_user: bool = False) -> str:
    cfg = load_config(cfg_path)
    rss_urls = cfg.get("sources", {}).get("rss", []) or []
    topics = cfg.get("topics", []) or []

    lookback = int(cfg.get("digest", {}).get("lookback_hours", 48))
    max_items = int(cfg.get("digest", {}).get("max_items", 8))
    min_per_topic = int(cfg.get("digest", {}).get("min_per_topic", 1))

    if for_new_user:
        min_per_topic = 1
        max_items = min(3, max_items)

    items = fetch_rss(rss_urls, lookback_hours=lookback)

    # Threads radar (optional)
    radar_cfg = cfg.get("radar", {}).get("threads", {}) or {}
    radar_enabled = bool(radar_cfg.get("enabled", False))
    max_terms_per_topic = int(radar_cfg.get("max_terms_per_topic", 3))

    threads_tw: List[str] = []
    threads_global: List[str] = []
    topic_threads_terms: Dict[str, List[str]] = {}

    if radar_enabled:
        threads_tw = fetch_threads_trending_tw()
        threads_global = fetch_threads_trending_global()
        merged = list(dict.fromkeys((threads_tw or []) + (threads_global or [])))  # de-dup keep order
        topic_threads_terms = map_threads_terms_to_topics(merged, topics, max_per_topic=max_terms_per_topic)

    topic_radar_terms = topic_threads_terms if radar_enabled else {t.get("id", ""): [] for t in topics}

    picks = pick_by_topic(
        items,
        topics,
        max_items=max_items,
        min_per_topic=min_per_topic,
        topic_radar_terms=topic_radar_terms,
    )

    # Delay tracking (works now; will be more meaningful once you implement threads collectors)
    now = datetime.now(TAIPEI_TZ)
    threads_all = list(dict.fromkeys((threads_tw or []) + (threads_global or [])))
    delay_status = update_delayed_watchlist(
        cfg=cfg,
        picks=picks,
        now=now,
        threads_terms_all=threads_all,
    )

    return format_digest(
        picks=picks,
        threads_tw=threads_tw,
        threads_global=threads_global,
        topic_threads_terms=topic_threads_terms,
        delay_status=delay_status,
    )


def main():
    cfg = load_config("config.yml")
    msg = generate_today_digest("config.yml", for_new_user=False)

    users = load_repo_users()

    ok = 0
    fail = 0
    now = datetime.now(TAIPEI_TZ)

    if not users:
        # fallback: single-user test push if you still keep LINE_USER_ID in secrets
        user_id = os.getenv("LINE_USER_ID", "")
        if user_id:
            try:
                push_text_to_user(user_id, msg)
                print("沒有 users.json 或無使用者名單，先用 LINE_USER_ID 測試推播")
            except Exception as e:
                print("測試推播失敗:", str(e))
        else:
            print("沒有 users.json，且未提供 LINE_USER_ID")
        return

    for uid in users:
        try:
            push_text_to_user(uid, msg)
            ok += 1
        except Exception as e:
            fail += 1
            print("推播失敗:", uid, str(e))
            record_push_failure(cfg, uid, e, now)

    print(f"推播完成：成功 {ok} 人，失敗 {fail} 人")


if __name__ == "__main__":
    main()
