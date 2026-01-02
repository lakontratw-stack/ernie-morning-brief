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

st.subheader("📌 主題設定（暫時只顯示）")

topics = config.get("topics", [])

for t in topics:
    with st.expander(t.get("name", t.get("id"))):
        st.write("ID:", t.get("id"))
        st.write("Enabled:", t.get("enabled"))
        st.write("Min score:", t.get("min_score"))
        st.write("Keywords:")
        st.code("\n".join(t.get("keywords", [])))

st.success("Dashboard 啟動成功 🎉（下一步會加上勾選與 Save）")
