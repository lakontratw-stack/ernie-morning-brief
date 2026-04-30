# Lyra Procurement Assistant MVP

Lyra is an internal LINE OA assistant for Watsons Taiwan Non-Trade Procurement. It helps colleagues understand NTP policy basics, LOA checks, tender flow, quotation comparison, negotiation wording, and IC summary drafting.

This MVP uses:

- Python + Flask
- SQLite
- LINE Reply API
- Telegram notification outbox
- Local Markdown knowledge files
- OpenAI chat completion

It intentionally does not use ERP integration, external websites, or RAG/vector search.

## File Structure

```text
app.py                    Flask app and background notification worker
config.py                 Environment settings
db.py                     SQLite schema and data access
line_handler.py           LINE webhook, Reply API, markers, takeover logic
lyra_prompt.py            System prompt, OpenAI call, OpenCC conversion
knowledge_loader.py       Markdown knowledge loader
guardrails.md             Lyra safety and escalation rules
telegram_notifier.py      Telegram message sender
notification_queue.py     SQLite outbox enqueue/drain functions
knowledge_base.md         NTP policy/process knowledge
tender_playbook.md        Tender analysis and wording guide
.env.example              Environment variable template
requirements.txt          Python dependencies
```

## Setup

Create `.env` from the example:

```bash
cp .env.example .env
```

Fill in:

```text
LINE_CHANNEL_SECRET=
LINE_CHANNEL_ACCESS_TOKEN=
TELEGRAM_BOT_TOKEN=
TELEGRAM_WORK_GROUP_CHAT_ID=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1-mini
DB_PATH=./lyra_procurement.db
```

Install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Start locally:

```bash
.venv/bin/python app.py
```

Health check:

```text
http://localhost:8000/healthz
```

## LINE Webhook

Set LINE webhook URL to:

```text
https://your-domain.com/webhook/line
```

The app:

- Verifies `X-Line-Signature`
- Handles text messages
- Replies to stickers with a short prompt
- Escalates image, file, audio, and video messages
- Uses LINE Reply API for AI replies
- Does not use Push API for AI auto replies

## Telegram Test

If Telegram env vars are missing, notifications are printed to logs instead of crashing.

To manually process queued notifications:

```bash
curl -X POST http://localhost:8000/admin/process-notifications
```

In production, the Flask app starts a simple background thread that drains pending notifications every 5 seconds.

## Knowledge Files

Edit escalation and safety behavior here:

```text
guardrails.md
```

Edit policy/process guidance here:

```text
knowledge_base.md
```

Edit tender analysis, negotiation wording, and IC summary guidance here:

```text
tender_playbook.md
```

On app startup and every AI call, Lyra loads `guardrails.md`, `knowledge_base.md`, and `tender_playbook.md` into the system prompt. This keeps the MVP simple and easy to maintain. Future versions can replace `knowledge_loader.py` with RAG without changing the LINE webhook flow.

## AI Markers

If Lyra replies with:

```text
[ESCALATE:reason]
```

The marker is removed before replying to LINE, the user is set to `human_taken_over`, and Telegram is notified.

If Lyra replies with:

```text
[NOTIFY:summary]
```

The marker is removed, LINE gets the normal reply, Telegram receives an information notification, and takeover is not activated.

## Takeover

When a user is `human_taken_over`:

- AI does not reply
- New LINE messages are logged
- New LINE messages are forwarded to Telegram
- LLM is not called

The app automatically recovers takeover users after `TAKEOVER_HOURS`, default 12 hours.

Manual recovery endpoint:

```bash
curl -X POST http://localhost:8000/admin/recover-takeovers
```

## Common Questions

### Lyra is silent in LINE

The user may be in `human_taken_over`. Wait for auto recovery, or call `/admin/recover-takeovers`.

### Telegram does not send

Check `TELEGRAM_BOT_TOKEN` and `TELEGRAM_WORK_GROUP_CHAT_ID`. If missing, messages print to logs.

### Lyra escalates too often

Review `KEYWORD_ESCALATIONS` in `line_handler.py` and the escalation rules in `lyra_prompt.py`.

### Lyra does not know a policy

Add the policy wording to `knowledge_base.md`. If the exact policy is not in the knowledge file, Lyra should not guess.

### OpenCC fails

The app prints a warning and continues without conversion.

## Production Notes

- Use HTTPS for LINE webhook.
- Store secrets in Render environment variables, not in git.
- Use a persistent disk or managed database if chat logs must survive redeploys.
- Move `process_pending_notifications()` into a separate worker/drainer process for production reliability.
- Add admin authentication before exposing operational endpoints publicly.
- Do not use Lyra output as final approval, legal advice, audit conclusion, or system record.
