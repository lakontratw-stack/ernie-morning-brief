# Lyra 部署方式比較

價格以官方頁面在 2026-04-29 查到的美元價格為準，台幣用約 `1 USD = 31.6 TWD` 粗估，實際會隨匯率和用量變動。

## 建議結論

第一版建議用 Render 或 Railway 跑雲端版，`LYRA_PROVIDER=openai_compatible`。

如果你非常想用本機 Hermes agent，建議用「自己的電腦 + Cloudflare Tunnel」，但要接受電腦不能關機、網路斷線服務就中斷。

## 方案比較

| 方案 | 適合情境 | 優點 | 缺點 | 粗估月費 |
|---|---|---|---|---|
| Render Web Service | 已經有 Render URL，想最快接上 LINE OA | 你現在的 webhook URL 就在 Render；部署簡單；支援 Python web service；可用環境變數放 token | Free/低階資源會受限；SQLite 檔案若無 persistent disk 不適合長期存重要資料；背景 drainer 最好之後拆 worker | Free 可測；Starter web service 約 USD 7/月，約 NT$220/月；若加 Postgres Basic 約 USD 6/月起 |
| Railway | 想要簡單部署、環境變數、資料庫一站式 | 操作直覺；Hobby USD 5/月含用量；適合小型 webhook | 成本是用量制，流量或資源高會增加；正式長期要留意帳單上限 | Hobby 最低 USD 5/月，約 NT$160/月 |
| Fly.io | 想要更細控資源與區域 | 小 VM 很便宜；可控制 machine、region、volume | 部署和維運比 Render/Railway 技術一點；要自己處理更多平台細節 | shared-cpu-1x 512MB 約 USD 3-5/月，約 NT$95-160/月，不含額外儲存/流量 |
| VPS | 想要穩定、可跑 Hermes、本地模型或更多服務 | 最自由；可以同機跑 app、drainer、資料庫、Hermes；長期成本固定 | 需要自己維護 Linux、安全更新、HTTPS、備份、監控 | 常見小 VPS 約 USD 5-12/月，約 NT$160-380/月 |
| 自己電腦 + Cloudflare Tunnel | 想保留 Hermes 在本機，模型也可本機跑 | 可直接使用你現有 Hermes；Cloudflare Tunnel 通常可免費開始；不一定需要雲端主機 | 電腦關機、睡眠、網路斷線就停；正式客服風險較高；家用網路與電力穩定性要顧 | Tunnel 可從免費開始；電腦電費另計 |
| ngrok | 臨時測試 webhook | 設定很快；適合開發測試 | 免費方案有流量/請求限制和 interstitial；正式客服不建議 | Free 可測；Hobby 約 USD 8-10/月 |

## 我的建議路線

### 測試期

用目前專案的 `LYRA_PROVIDER=mock` 先部署到 Render，確認：

- LINE webhook 驗簽成功
- 客戶傳訊息 Lyra 會回
- 關鍵字會轉人工
- Telegram 群組會收到通知
- 「我先處理」按鈕會更新 takeover 狀態

### 第一版正式

改成：

```text
LYRA_PROVIDER=openai_compatible
```

接便宜、穩定的雲端模型。資料庫建議改 PostgreSQL，不要長期依賴 Render ephemeral SQLite。

### 要接 Hermes

若你要堅持「Hermes agent 是核心」，建議兩種做法：

1. 本機 Hermes + Cloudflare Tunnel：最快，但可靠度取決於你的電腦。
2. VPS 安裝 Hermes：較正式，但需要維運。

Render 也可以嘗試安裝 Hermes CLI，但它比較像互動式 agent 工具，不是最適合拿來當高併發客服回覆引擎。雲端部署時，用 OpenAI-compatible API 會穩定很多。

## 參考價格來源

- Render pricing: https://render.com/pricing
- Railway pricing: https://docs.railway.com/pricing
- Fly.io pricing: https://fly.io/docs/about/pricing/
- ngrok pricing: https://ngrok.com/pricing
- Cloudflare Tunnel docs/pricing入口: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/
