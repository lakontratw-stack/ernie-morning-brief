import streamlit as st
import yaml
from pathlib import Path

st.set_page_config(page_title="Ernie Morning Brief Dashboard", layout="wide")

st.title("☀️ Ernie 早安 AI 日報 – 設定面板")

CONFIG_PATH = Path("config.yml")

if not CONFIG_PATH.exists():
    st.error("找不到 config.yml")
    st.stop()

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

topics = config.get("topics", [])

st.subheader("📌 主題設定")

edited_topics = []

for idx, t in enumerate(topics):
    with st.expander(t.get("name", t.get("id")), expanded=False):
        enabled = st.checkbox(
            "啟用此主題",
            value=t.get("enabled", True),
            key=f"enabled_{idx}",
        )

        min_score = st.number_input(
            "最低分數門檻（min_score）",
            min_value=0,
            max_value=10,
            value=t.get("min_score", 1),
            step=1,
            key=f"min_score_{idx}",
        )

        query = st.text_area(
            "搜尋 Query（以空白分隔，會取代 keywords）",
            value=" ".join(t.get("keywords", [])),
            height=120,
            key=f"query_{idx}",
        )

        edited_topics.append(
            {
                **t,
                "enabled": enabled,
                "min_score": int(min_score),
                "keywords": [q for q in query.split() if q.strip()],
            }
        )

st.divider()
st.info("⬆️ 上方設定尚未儲存（下一步會加入 Save）")
