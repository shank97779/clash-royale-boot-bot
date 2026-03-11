# Clash Royale Boot Bot

Fetches daily Clash Royale river race data for a clan, stores a per-member snapshot in a local SQLite database, and posts an action report to a Discord webhook.

---

## How it works

Every run (typically once per day via cron):

1. Hits the **`/currentriverrace`** endpoint to get fame, repair points, boat attacks, and deck usage per participant.
2. Hits the **`/members`** endpoint to get the live roster — only current members are evaluated; ex-members still in the race data are silently ignored.
3. Stores both snapshots in `bootbot.db`.
4. On **war days**, evaluates each member who has been in the clan for at least one prior day and flags anyone who is under-participating.
5. Posts the results to Discord.

### Boot / demotion rules (war days only)

| Role | Action |
|---|---|
| Leader | Flag for review |
| Co-Leader | Demote to Elder |
| Elder | Demote to Member |
| Member | Boot |

A member is flagged if **either** condition is true:

- `decksUsedToday == 0` — didn't play at all today.
- Cumulative `decksUsed < MIN_PARTICIPATION_PCT × (prior_war_days × MIN_DECKS_PER_DAY)` — overall participation is too low (default: below 50 % of expected decks).

**New-member grace:** any player whose tag wasn't seen in a previous snapshot is skipped entirely — they may not have had all 4 decks available on their first race day.

---

## Setup

### 1. Clone / copy files

```
/home/matt/public_html/clash-royale-boot-bot/
├── bootbot.py
├── requirements.txt
├── .env.example
└── venv/
```

### 2. Create the virtual environment

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
```

Edit `.env`:

| Variable | Description | Default |
|---|---|---|
| `CR_API_TOKEN` | JWT from [developer.clashroyale.com](https://developer.clashroyale.com) | *(required)* |
| `CLAN_TAG` | Clan tag including `#` | `#PJ8Q8P` |
| `DISCORD_WEBHOOK` | Incoming webhook URL from your Discord server | *(optional)* |
| `DB_PATH` | Path to the SQLite database file | `bootbot.db` |
| `MIN_DECKS_PER_DAY` | Expected deck uses per war day | `4` |
| `MIN_PARTICIPATION_PCT` | Flag if cumulative usage is below this fraction of expected | `0.5` |

### 4. Run manually

```bash
# Normal run
venv/bin/python bootbot.py

# With verbose logging (shows full roster table, API calls, per-member evaluation)
venv/bin/python bootbot.py --verbose
```

### 5. Schedule with cron

```bash
crontab -e
```

Add (runs at 4:55 AM daily):

```
55 4 * * * cd /PATH/TO/clash-royale-boot-bot/ && venv/bin/python bootbot.py >> tracker.log 2>&1
```

---

## Verbose output

`--verbose` / `-v` prints:

- API URLs, HTTP status codes, and response sizes
- DB file being used
- Full member roster table (sorted by role) with fame, deck counts, and race status
- Grace skips (new joiners)
- Each flagged member with their role, recommended action, and reason
- Summary counts

---

## Discord report

- **Grey embed** — training day, no evaluation.
- **Green embed** — war day, everyone is participating.
- **Red embed** — war day, lists each under-performing member with their recommended action, stats, and last-seen date.

---

## Database schema

**`snapshots`** — daily race stats per participant:

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | TEXT | ISO date (`YYYY-MM-DD`) |
| `period_type` | TEXT | `warDay` or `trainingDay` |
| `player_tag` | TEXT | `#XXXXXXX` |
| `player_name` | TEXT | |
| `fame` | INTEGER | |
| `repair_points` | INTEGER | |
| `boat_attacks` | INTEGER | |
| `decks_used` | INTEGER | Cumulative for the race week |
| `decks_used_today` | INTEGER | |

**`member_snapshots`** — daily roster snapshot:

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | TEXT | ISO date |
| `player_tag` | TEXT | |
| `player_name` | TEXT | |
| `role` | TEXT | `leader`, `coLeader`, `elder`, `member` |
| `exp_level` | INTEGER | |
| `trophies` | INTEGER | |
| `donations` | INTEGER | |
| `last_seen` | TEXT | Timestamp string from the API |
