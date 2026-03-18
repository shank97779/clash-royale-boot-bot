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

### Live run behavior around reset

When running without `--date`, the bot now behaves as follows:

- **Before reset (10:00 UTC by default):** fetches and stores snapshots only; no Discord post.
- **After reset:** sends **one** Discord report for the **previous Clash day** (`yesterday` in Clash-day terms).
- Later runs the same day do not repost the same date (deduplicated in SQLite).

### Clash day timestamping

The bot stores snapshots under a computed Clash "day" (`YYYY-MM-DD`) using UTC reset time,
not server local time. By default, day rollover is `10:00 UTC`.

If your run schedule is close to the reset boundary, you can tune this with:

- `CLASH_RESET_UTC_HOUR` (default `10`)
- `CLASH_RESET_UTC_MINUTE` (default `0`)

### Top performers shoutout (war days only)

After boot evaluation, the top `TOP_PERFORMERS_N` distinct fame tiers are identified. Every player tied at a given tier earns the same medal. Within a tier, players are sorted by fewest decks used (most efficient first). The shoutout is posted as a gold embed in Discord — good for morale!

### Boat attack shaming (war days only)

Boat attacks earn only **125 fame per tower** hit, compared to **200 fame** for a regular battle win or **250 fame** for a dual-battle win. Any member who used boat attacks during the race is called out in an orange embed so they know to switch strategy. Set `REPORT_BOAT_ATTACKS=false` to disable this.

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
| `CLASH_RESET_UTC_HOUR` | UTC hour used for Clash day rollover | `10` |
| `CLASH_RESET_UTC_MINUTE` | UTC minute used for Clash day rollover | `0` |
| `MIN_DECKS_PER_DAY` | Expected deck uses per war day | `4` |
| `MIN_PARTICIPATION_PCT` | Flag if cumulative usage is below this fraction of expected | `0.5` |
| `MIN_CLAN_SIZE` | Never boot members below this headcount; demotions are unaffected | `40` |
| `TOP_PERFORMERS_N` | Number of distinct fame tiers to shoutout | `3` |
| `REPORT_BOAT_ATTACKS` | Post an orange warning embed for members who used boat attacks | `true` |

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

Recommended hourly schedule:

```
0 * * * * cd /home/matt/public_html/BornGifted && /home/matt/public_html/BornGifted/venv/bin/python /home/matt/public_html/BornGifted/bootbot.py >> /home/matt/public_html/BornGifted/tracker.log 2>&1
```

This is safe to run hourly because the bot is reset-aware:

- Before `10:00 UTC`, it fetches and stores snapshots only.
- After `10:00 UTC`, the first run sends the report for the previous Clash day.
- Later runs the same day do not repost the same report.

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
- **Yellow embed** — war day, lists members who are under-performing but are **protected from booting** because the clan is at or near `MIN_CLAN_SIZE`. They would be actioned once recruiting improves. Demotions (co-leader / elder) are never protected and always appear in the red embed.
- **Gold embed** — top performers shoutout (up to `TOP_PERFORMERS_N` fame tiers).
- **Orange embed** — boat attack warning for members who used boat attacks instead of regular or dual battles.

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
