import streamlit as st
import yaml
from datetime import datetime
import requests
import base64
import json


# =========================
# Page
# =========================
st.set_page_config(page_title="Ernie Morning Brief Dashboard", layout="wide")
st.title("☀️ Ernie 早安 AI 日報 – 設定面板")


# =========================
# Config
# =========================
# 你目前的 repo
DEFAULT_REPO = "lakontratw-stack/ernie-morning-brief"
CONFIG_PATH_IN_REPO = "config.yml"


def get_github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_file_from_github(repo_full: str, path: str, token: str) -> tuple[str, str]:
    """
    Return (decoded_text, sha)
    """
    api_url = f"https://api.github.com/repos/{repo_full}/contents/{path}"
    r = requests.get(api_url, headers=get_github_headers(token), timeout=30)
    r.raise_for_status()
    data = r.json()

    content_b64 = data.get("content", "")
    if not content_b64:
        raise RuntimeError("GitHub API 回傳沒有 content，可能是檔案不存在或權限不足。")

    decoded = base64.b64decode(content_b64).decode("utf-8", errors="replace")
    sha = data.get("sha")
    if not sha:
        raise RuntimeError("GitHub API 回傳沒有 sha，無法更新檔案。")

    return decoded, sha


def commit_file_to_github(repo_full: str, path: str, token: str, new_text: str, sha: str, message: str) -> None:
    api_url = f"https://api.github.com/repos/{repo_full}/contents/{path}"

    payload = {
        "message": message,
        "content": base64.b64encode(new_text.encode("utf-8")).decode("utf-8"),
        "sha": sha,
    }

    r = requests.put(api_url, headers=get_github_headers(token), data=json.dumps(payload), timeout=30)
    r.raise_for_status()


# =========================
# Secrets check
# =========================
if "GITHUB_TOKEN" not in st.secrets:
    st.error("找不到 Streamlit Secrets 的 GITHUB_TOKEN。請到 Streamlit Cloud → App → Settings → Secrets 新增。")
    st.stop()

token = st.secrets["GITHUB_TOKEN"]


# =========================
# Repo selector (optional)
# =========================
with st.sidebar:
    st.header("⚙️ 基本設定")
    repo_full = st.text_input("GitHub Repo（owner/repo）", value=DEFAULT_REPO)
    st.caption("例：lakontratw-stack/ernie-morning-brief")
    st.divider()
    st.caption("提示：Save 會直接 commit 回 GitHub 的 config.yml")


# =========================
# Load config.yml from GitHub
# =========================
try:
    raw_yaml, file_sha = fetch_file_from_github(repo_full, CONFIG_PATH_IN_REPO, token)
except requests.HTTPError as e:
    st.error(f"讀取 GitHub 失敗：{e}")
    st.info("請確認：Token 權限（Contents: Read/Write）與 repo 是否正確。")
    st.stop()
except Exception as e:
    st.error(f"讀取設定失敗：{e}")
    st.stop()

try:
    config = yaml.safe_load(raw_yaml) or {}
except Exception as e:
    st.error(f"config.yml YAML 解析失敗：{e}")
    st.stop()

topics = config.get("topics", [])
if not isinstance(topics, list):
    st.error("config.yml 的 topics 格式不是 list，請檢查檔案內容。")
    st.stop()


# =========================
# UI - Topics editable
# =========================
st.subheader("📌 主題設定")

edited_topics = []

for idx, t in enumerate(topics):
    topic_id = t.get("id", f"topic_{idx}")
    topic_name = t.get("name", topic_id)

    with st.expander(topic_name, expanded=False):
        enabled = st.checkbox(
            "啟用此主題",
            value=bool(t.get("enabled", True)),
            key=f"enabled_{idx}",
        )

        min_score = st.number_input(
            "最低分數門檻（min_score）",
            min_value=0,
            max_value=50,
            value=int(t.get("min_score", 2)),
            step=1,
            key=f"min_score_{idx}",
        )

        # 把 keywords 轉成單行 query（以空白分隔）
        # 你現在希望用「搜尋 query」概念去擴展，所以 UI 用 query 編輯更直覺
        keywords = t.get("keywords", [])
        if keywords is None:
            keywords = []
        if not isinstance(keywords, list):
            keywords = []

        default_query = " ".join([str(x).strip() for x in keywords if str(x).strip()])

        query = st.text_area(
            "搜尋 Query（以空白分隔，會取代 keywords）",
            value=default_query,
            height=120,
            key=f"query_{idx}",
        )

        # 轉回 keywords list（給原本 run_daily.py 使用）
        new_keywords = [q.strip() for q in query.split() if q.strip()]

        edited_topics.append(
            {
                **t,
                "id": topic_id,
                "name": topic_name,
                "enabled": enabled,
                "min_score": int(min_score),
                "keywords": new_keywords,
            }
        )

st.divider()


# =========================
# Save to GitHub (commit)
# =========================
col1, col2 = st.columns([1, 2])

with col1:
    do_save = st.button("💾 Save 設定（寫回 GitHub）", use_container_width=True)

with col2:
    st.caption("按下 Save 後會直接更新 GitHub 的 config.yml（commit），明天 06:00 的 Actions 就會套用新設定。")

if do_save:
    config["topics"] = edited_topics
    config["last_updated"] = datetime.utcnow().isoformat()

    new_yaml_text = yaml.dump(config, allow_unicode=True, sort_keys=False)

    try:
        commit_file_to_github(
            repo_full=repo_full,
            path=CONFIG_PATH_IN_REPO,
            token=token,
            new_text=new_yaml_text,
            sha=file_sha,
            message="update config via dashboard",
        )
        st.success("✅ 已成功寫回 GitHub（config.yml 已更新）")
        st.info("如果你要立刻驗證：到 GitHub repo 看 config.yml 的最新 commit。")
        st.stop()
    except requests.HTTPError as e:
        st.error(f"寫回 GitHub 失敗：{e}")
        st.info("常見原因：Token 權限不足（Contents 未給 Read/Write）或 Token 對不到這個 repo。")
        st.stop()
    except Exception as e:
        st.error(f"寫回 GitHub 發生錯誤：{e}")
        st.stop()


# =========================
# Debug / Preview
# =========================
with st.expander("🔎 Preview（將要寫回的 config.yml）", expanded=False):
    preview_config = dict(config)
    preview_config["topics"] = edited_topics
    preview_config["last_updated"] = datetime.utcnow().isoformat()
    st.code(yaml.dump(preview_config, allow_unicode=True, sort_keys=False), language="yaml")
