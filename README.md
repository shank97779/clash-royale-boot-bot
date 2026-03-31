# boot-bot

Monitors Clash Royale river race participation and posts a Discord report
covering boot/demote candidates, promotion candidates, top performers, and
worst fame performers for the most recently completed war section.

## How it works

1. **`ingest.py`** — run every hour via cron. Fetches the current river
   race and clan member list from the API, saves one timestamped raw JSON
   archive per run under `data/`, and upserts snapshots into SQLite.

2. **`report.py`** — run after ingest has observed the next API phase/day.
   Reads SQLite and posts a Discord report with five sections:
   - **Boot / Demote / Flag for review** — members with fewer than `MIN_DECKS` decks used, grouped by action
   - **Promote to Elder** — `member`-role players averaging ≥ `PROMOTE_FAME_PER_DAY` fame/day over the last `BEST_PERFORMERS_DAYS` war days
   - **Top Performers** — 🥇🥈🥉 podium by fame for the current section (ties share a rank)
   - **Worst Performers** — bottom `WORST_PERFORMERS_SHOW` members by average fame/day over the last `WORST_PERFORMERS_DAYS` war days

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

# Tune the rolling windows and list sizes
python report.py --worst-days 16 --worst-show 5 --best-days 16 --best-show 5
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CLAN_TAG` | `#PJ8Q8P` | Clan tag |
| `ENVIRONMENT` | _(unset)_ | Set to `production` to use `DISCORD_WEBHOOK_PROD` |
| `DISCORD_WEBHOOK_DEV` | | Webhook for non-production runs |
| `DISCORD_WEBHOOK_PROD` | | Webhook for production runs |
| `CR_API_TOKEN` | | Clash Royale API token |
| `DB_PATH` | _(default)_ | Path to the SQLite database |
| `MIN_DECKS` | `4` | Minimum decks used per day before flagging |
| `PROMOTE_FAME_PER_DAY` | `500` | Minimum avg fame/day to be a promotion candidate |
| `WORST_PERFORMERS_DAYS` | `16` | Rolling war day window for worst performers |
| `WORST_PERFORMERS_SHOW` | `5` | Number of worst performers to show |
| `BEST_PERFORMERS_DAYS` | `16` | Rolling war day window for promotion candidates |
| `BEST_PERFORMERS_SHOW` | `5` | Max promotion candidates to show |
| `EXEMPT_TAGS` | _(unset)_ | Comma-separated player tags never flagged (e.g. `#ABC123,#DEF456`) |

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
  db.py             # SQLite schema and helper functions
  ingest.py         # API fetch → data/ + SQLite
  report.py         # SQLite → console + Discord
  test_boot_bot.py
  requirements.txt
  Makefile
  .env.example
  data/             # per-run timestamped ingest archives (created at runtime)
  logs/
    ingest/         # timestamped ingest logs
    report/         # timestamped report logs
```

## Rules

- Members on their **first ever** stored section are skipped (grace period).
- Tags listed in `EXEMPT_TAGS` are never flagged.
- The report only runs on **warDay** snapshots — training / colosseum sections are ignored.
- Snapshots are keyed directly by API `periodIndex`, `periodType`, and `sectionIndex`.
- Stored participant values are the latest raw values from the API response; no smoothing or deck-delta inference is applied.
- Fame is cumulative within a war weekend; per-day values are derived by diffing consecutive days in the same weekend.
