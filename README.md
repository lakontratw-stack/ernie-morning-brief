# Lyra LINE OA Integration

這個專案是 LINE OA 與 Lyra/Hermes agent 的整合層：

- 接 LINE webhook：`/webhook` 與 `/webhook/line`
- 驗 LINE signature
- 用 Lyra 回覆一般問題
- 用 `[ESCALATE:原因]` 轉人工
- 用 Telegram 工作群組通知同事
- Telegram inline button「我先處理」會把客戶狀態設為人工接手
- 人工接手期間 Lyra 完全靜默

## 重要安全事項

你提供過的 LINE 與 Telegram token 已經出現在對話中。正式上線前請在 LINE Developers 和 BotFather 重新產生 token，避免外洩風險。

不要把真 token commit 到 Git。請只放在部署平台的 Environment Variables。

## 本機啟動

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn lyra_line.app:app --reload --port 8000
```

打開瀏覽器看預覽頁：

```text
http://127.0.0.1:8000/
```

這個頁面可以先測 Lyra 的客服回覆邏輯，不會真的傳 LINE，也不會通知 Telegram。

LINE webhook 測試期可先用 Cloudflare Tunnel 或 ngrok 指到：

```text
http://localhost:8000/webhook
```

## 必填環境變數

```text
LINE_CHANNEL_SECRET
LINE_CHANNEL_ACCESS_TOKEN
TELEGRAM_BOT_TOKEN
TELEGRAM_WORK_GROUP_CHAT_ID
PUBLIC_BASE_URL
LYRA_PROVIDER
```

## Lyra Provider

### mock

適合先測 LINE/Telegram 流程，不會花模型費。

```text
LYRA_PROVIDER=mock
```

### hermes_cli

透過本機 Hermes CLI 產生 Lyra 回覆。適合跑在你自己的電腦或已安裝 Hermes 的 VPS。

```text
LYRA_PROVIDER=hermes_cli
HERMES_BIN=hermes
HERMES_WORKDIR=/path/to/workdir
```

### openai_compatible

適合部署到 Render/Railway/Fly.io。只要模型供應商支援 OpenAI-compatible Chat Completions API 即可。

```text
LYRA_PROVIDER=openai_compatible
OPENAI_COMPATIBLE_BASE_URL=https://api.openai.com/v1
OPENAI_COMPATIBLE_API_KEY=...
OPENAI_COMPATIBLE_MODEL=gpt-4.1-mini
```

## LINE 設定

LINE Developers 後台 Webhook URL 可填：

```text
https://your-service.onrender.com/webhook
```

或：

```text
https://your-service.onrender.com/webhook/line
```

## Telegram 設定

若要讓「我先處理」按鈕生效，需要設定 Telegram webhook：

```bash
curl "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook" \
  -d "url=$PUBLIC_BASE_URL/webhook/telegram"
```

## 部署到 Render

1. 把專案推到 GitHub。
2. Render 新增 Web Service。
3. Build command:
   ```text
   pip install -r requirements.txt
   ```
4. Start command:
   ```text
   uvicorn lyra_line.app:app --host 0.0.0.0 --port $PORT
   ```
5. 設定 Environment Variables。
6. LINE Webhook URL 指到 `https://你的服務.onrender.com/webhook`。
7. 設定 Telegram webhook 到 `https://你的服務.onrender.com/webhook/telegram`。

如果要推到目前 Render 連接的 repo，可以在本機執行：

```bash
./deploy_to_github.sh
```

預設會推到：

```text
https://github.com/lakontratw-stack/ernie-morning-brief.git
```

## 目前範例店家資料

現在 prompt 先用「台灣屈臣氏」作為測試範例，位於 `lyra_line/prompts.py` 的 `WATSONS_TAIWAN_EXAMPLE`。

正式上線時請換成你的店家資料、服務項目、價格和一定轉人工的規則。

## 常見問題知識庫

第一版可編輯知識庫在：

```text
data/business_knowledge.json
```

目前支援：

- 門市營業時間
- 門市地址
- 促銷活動訊息

更新這個檔案後，重新部署 Render，Lyra 就會用新的資料回答。

## Google Sheet 中階知識庫

中階版本可以改用 Google Sheet 維護，不需要每次改程式。

請建立一份 Google Sheet，至少兩個分頁：

```text
store_hours
promotions
```

欄位格式可直接參考：

```text
templates/store_hours.csv
templates/promotions.csv
```

`store_hours` 欄位：

```text
store_name, aliases, address, phone, days, open, close, notes, source_url
```

`promotions` 欄位：

```text
title, period, summary, details, status
```

Google Sheet 設定方式：

1. Google Sheet 右上角點「共用」，設成知道連結的人可檢視。
2. 針對每個分頁取得 CSV 發佈連結。
3. 到 Render Environment Variables 填：

```text
STORE_HOURS_CSV_URL=<store_hours 分頁的 CSV URL>
PROMOTIONS_CSV_URL=<promotions 分頁的 CSV URL>
```

部署後，Lyra 會優先讀 Google Sheet；如果 Sheet 讀不到，才會 fallback 到 `data/business_knowledge.json`。

如果你更新 Google Sheet 後想立刻清快取，可呼叫：

```bash
curl -X POST https://ernie-morning-brief.onrender.com/admin/refresh-knowledge
```

### Watsons 全台門市資料

你提供的 Watsons 門市查詢頁：

```text
https://www.watsons.com.tw/store-finder?page=0&dataIndex=0
```

這個頁面適合給人查詢，但不是穩定的資料 API。系統目前會預設讀取 Watsons 公開全門市清單：

```text
WATSONS_STORE_LIST_URL=https://www.watsons.com.tw/storedescription
```

這份清單可以讓 Lyra 查到全台門市名稱與地址。中階版本建議做法是：

1. 先用 Google Sheet 當客服知識庫。
2. 每間門市一列資料，填入門市名稱、別名、地址、電話、營業時間。
3. `source_url` 放 Watsons 官方門市頁，方便日後查核與更新。

資料優先順序：

```text
Google Sheet > data/business_knowledge.json > Watsons 官方全門市清單
```

也就是說，Watsons 官方清單會補上「所有門市名稱與地址」；Google Sheet 則用來補上真正客服需要的營業時間、電話、活動備註或特殊公告。

目前測試資料已先放入屈臣氏民權店：

```text
source_url=https://www.watsons.com.tw/store/wtctw_83_zh_TW
營業時間=一~日 10:00 到 23:00
```

如果要先批次匯入 Watsons 門市名稱與地址，可在本機執行：

```bash
python scripts/import_watsons_storedescription.py
```

它會產生：

```text
templates/store_hours.generated.csv
```

注意：Watsons 的公開全門市清單主要有門市名稱與地址，沒有每間門市的營業時間。若使用者問某間門市營業時間，但那間尚未在 Google Sheet 補上時間，Lyra 會回覆已查到門市地址，並自動轉人工確認，避免編造不確定資訊。
