# boot-bot

Monitors Clash Royale river race participation and posts a Discord report
covering boot/demote candidates, promotion candidates, and top performers
for the most recently completed war section.

## How it works

1. **`ingest.py`** — run every hour via cron. Fetches the current river
   race and clan member list from the API, saves one timestamped raw JSON
   archive per run under `data/`,
   and upserts snapshots into SQLite.

2. **`report.py`** — run after ingest has observed the next API phase/day.
   Reads SQLite and posts a Discord report with four sections:
   - **Boot / Demote / Flag for review** — members with fewer than `MIN_DECKS_PER_DAY` decks, grouped by action and ordered newest member first
   - **Promote to Elder** — `member`-role players with ≥ `PROMOTE_FAME` fame across the last `PROMOTE_WAR_COUNT` completed war days
   - **Top Performers** — 🥇🥈🥉 podium by fame (ties share a rank)

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your CLAN_TAG, CR_API_TOKEN, DISCORD_WEBHOOK_DEV / DISCORD_WEBHOOK_PROD
```

## Usage

```bash
# Fetch and store the current API section snapshot
python ingest.py

# Report the latest completed stored war section (sends to Discord DEV webhook)
python report.py

# Preview without sending to Discord
python report.py --dry-run

# Report a specific API section
python report.py --section 25:warDay:3
```

## Environments

Two webhooks are supported so you can test safely before posting to your real channel:

| `ENVIRONMENT` value | Webhook used |
|---|---|
| _(unset or anything else)_ | `DISCORD_WEBHOOK_DEV` |
| `production` | `DISCORD_WEBHOOK_PROD` |

## Crontab example

```cron
# Ingest every hour
0 * * * * cd /home/matt/public_html/boot-bot && venv/bin/python ingest.py

# Report after ingest has rolled into the next API section/phase
5 10 * * * cd /home/matt/public_html/boot-bot && ENVIRONMENT=production venv/bin/python report.py
```

## Project structure

```
boot-bot/
  db.py           # SQLite schema and helper functions
  ingest.py       # API fetch → data/ + SQLite
  report.py       # SQLite → console + Discord
  test_boot_bot.py
  requirements.txt
  Makefile
  .env.example
   data/           # per-run timestamped ingest archives (created at runtime)
   logs/
      ingest/       # timestamped ingest logs
      report/       # timestamped report logs
```

## Rules

- Members on their **first ever** stored section are skipped (grace period).
- Tags listed in `EXEMPT_MEMBERS` are never flagged.
- The report only runs on **warDay** snapshots — training / colosseum sections are ignored.
- Snapshots are keyed directly by API `periodIndex`, `periodType`, and `sectionIndex`.
- Stored participant values are the latest raw values from the API response; no smoothing or deck-delta inference is applied.
