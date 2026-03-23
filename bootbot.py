#!/usr/bin/env python3
"""
Clash Royale Boot Bot
==============================
Fetches current river race data from the Clash Royale API, stores a daily
snapshot per member, and reports boot candidates to a Discord webhook.
"""

import argparse
import os
import sys
import sqlite3
import requests
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# Clash "day" rolls over at the river race reset time in UTC.
# Defaults preserve existing behavior: 10:00 UTC reset.
CLASH_RESET_UTC_HOUR   = int(os.getenv("CLASH_RESET_UTC_HOUR", "10"))
CLASH_RESET_UTC_MINUTE = int(os.getenv("CLASH_RESET_UTC_MINUTE", "0"))

# ── Configuration ──────────────────────────────────────────────────────────────
CLAN_TAG        = os.getenv("CLAN_TAG",         "#PJ8Q8P")   # BornGifted tag
API_TOKEN       = os.getenv("CR_API_TOKEN",     "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK",  "")
DB_PATH         = os.getenv("DB_PATH",          "bootbot.db")

# A member is a boot candidate if either condition is met:
#   1. They used 0 decks today on a war day (and aren't a brand-new joiner).
#   2. Their cumulative decks_used is below MIN_PARTICIPATION_PCT of expected.
MIN_DECKS_PER_DAY     = int(os.getenv("MIN_DECKS_PER_DAY",    "4"))
MIN_PARTICIPATION_PCT = float(os.getenv("MIN_PARTICIPATION_PCT", "0.5"))  # 50 %
TOP_PERFORMERS_N      = int(os.getenv("TOP_PERFORMERS_N",      "3"))
REPORT_BOAT_ATTACKS   = os.getenv("REPORT_BOAT_ATTACKS", "true").strip().lower() == "true"
MIN_CLAN_SIZE         = int(os.getenv("MIN_CLAN_SIZE",         "40"))  # never boot below this headcount; boot down to it by worst performers

# Comma-separated player tags that are NEVER flagged as boot/demotion candidates
# (e.g. EXEMPT_MEMBERS=#ABC123,#DEF456).  Tags are case-insensitive.
EXEMPT_MEMBERS = {
    tag.strip().upper()
    for tag in os.getenv("EXEMPT_MEMBERS", "").split(",")
    if tag.strip()
}

CR_API_BASE = "https://api.clashroyale.com/v1"

VERBOSE = False  # set to True by --verbose flag


def vlog(*args, **kwargs) -> None:
    """Print only when --verbose is active."""
    if VERBOSE:
        print("  [v]", *args, **kwargs)


def clash_day_from_utc(now_utc: datetime | None = None) -> str:
    """
    Returns the Clash race day label (YYYY-MM-DD), based on UTC reset time.

    If reset is 10:00 UTC, then:
      - 2026-03-15 09:59 UTC -> clash day 2026-03-14
      - 2026-03-15 10:00 UTC -> clash day 2026-03-15
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    shifted = now_utc - timedelta(hours=CLASH_RESET_UTC_HOUR, minutes=CLASH_RESET_UTC_MINUTE)
    return shifted.date().isoformat()

# ── Database ───────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    vlog(f"Initialising DB schema in '{DB_PATH}' …")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date    TEXT    NOT NULL,
            period_type      TEXT    NOT NULL DEFAULT '',
            section_index    INTEGER NOT NULL DEFAULT 0,
            period_index     INTEGER NOT NULL DEFAULT 0,
            player_tag       TEXT    NOT NULL,
            player_name      TEXT    NOT NULL,
            fame             INTEGER NOT NULL DEFAULT 0,
            repair_points    INTEGER NOT NULL DEFAULT 0,
            boat_attacks     INTEGER NOT NULL DEFAULT 0,
            decks_used       INTEGER NOT NULL DEFAULT 0,
            decks_used_today INTEGER NOT NULL DEFAULT 0,
            UNIQUE(snapshot_date, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_snap_date ON snapshots(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_snap_tag  ON snapshots(player_tag);

        CREATE TABLE IF NOT EXISTS member_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT    NOT NULL,
            player_tag    TEXT    NOT NULL,
            player_name   TEXT    NOT NULL,
            role          TEXT    NOT NULL DEFAULT 'member',
            exp_level     INTEGER NOT NULL DEFAULT 0,
            trophies      INTEGER NOT NULL DEFAULT 0,
            donations     INTEGER NOT NULL DEFAULT 0,
            last_seen     TEXT    NOT NULL DEFAULT '',
            UNIQUE(snapshot_date, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_mem_date ON member_snapshots(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_mem_tag  ON member_snapshots(player_tag);

        CREATE TABLE IF NOT EXISTS report_history (
            report_date TEXT PRIMARY KEY,
            sent_at_utc TEXT NOT NULL
        );
    """)
    conn.commit()


def reset_datetime_utc(now_utc: datetime) -> datetime:
    """Returns today's reset timestamp in UTC."""
    return now_utc.replace(
        hour=CLASH_RESET_UTC_HOUR,
        minute=CLASH_RESET_UTC_MINUTE,
        second=0,
        microsecond=0,
    )


def snapshot_exists(conn: sqlite3.Connection, snapshot_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM snapshots WHERE snapshot_date = ? LIMIT 1",
        (snapshot_date,),
    ).fetchone()
    return row is not None


def report_was_sent(conn: sqlite3.Connection, report_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM report_history WHERE report_date = ? LIMIT 1",
        (report_date,),
    ).fetchone()
    return row is not None


def mark_report_sent(conn: sqlite3.Connection, report_date: str, now_utc: datetime) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO report_history (report_date, sent_at_utc)
        VALUES (?, ?)
        """,
        (report_date, now_utc.isoformat()),
    )
    conn.commit()


def previous_clash_day(clash_day: str) -> str:
    return (date.fromisoformat(clash_day) - timedelta(days=1)).isoformat()


def pick_report_date(today: str, period_type: str, before_reset: bool) -> str | None:
    """
    Chooses which snapshot date should be reported in live mode.

    Rules:
    - After reset: report the previous clash day (fully completed).
    - Before reset on war day: suppress report to avoid premature boot decisions.
    - Before reset on non-war day: report today's clash-day snapshot.
    """
    if before_reset:
        if period_type == "warDay":
            return None
        return today
    return previous_clash_day(today)


def prior_war_progress(
    conn: sqlite3.Connection,
    player_tag: str,
    today: str,
    section_index: int,
) -> tuple[int, int]:
    """Returns (prior_war_days, prior_cumulative_decks) for this race week."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS war_days, MAX(decks_used) AS cumulative_decks
        FROM snapshots
        WHERE player_tag = ?
          AND snapshot_date < ?
          AND period_type = 'warDay'
          AND section_index = ?
        """,
        (player_tag, today, section_index),
    ).fetchone()
    if not row:
        return 0, 0
    return row["war_days"] or 0, row["cumulative_decks"] or 0


def derive_decks_used_today(
    conn: sqlite3.Connection,
    today: str,
    period_type: str,
    section_index: int,
    player_tag: str,
    decks_used: int,
) -> int:
    """Computes today's deck usage from cumulative race progress."""
    if period_type != "warDay":
        return 0
    _, prior_decks_total = prior_war_progress(conn, player_tag, today, section_index)
    return max(0, decks_used - prior_decks_total)


def period_types_for_day(conn: sqlite3.Connection, snapshot_date: str) -> set[str]:
    """Returns distinct period types already stored for a clash day."""
    rows = conn.execute(
        "SELECT DISTINCT period_type FROM snapshots WHERE snapshot_date = ?",
        (snapshot_date,),
    ).fetchall()
    return {r["period_type"] for r in rows if r["period_type"]}

# ── API ────────────────────────────────────────────────────────────────────────

def fetch_river_race() -> dict:
    tag_encoded = CLAN_TAG.replace("#", "%23")
    url = f"{CR_API_BASE}/clans/{tag_encoded}/currentriverrace"
    vlog(f"GET {url}")
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {API_TOKEN}"},
        timeout=10,
    )
    vlog(f"  HTTP {resp.status_code}  ({len(resp.content)} bytes)")
    resp.raise_for_status()
    return resp.json()


def fetch_members() -> list:
    """Returns the current clan member list from the /members endpoint."""
    tag_encoded = CLAN_TAG.replace("#", "%23")
    url = f"{CR_API_BASE}/clans/{tag_encoded}/members"
    vlog(f"GET {url}")
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {API_TOKEN}"},
        timeout=10,
    )
    vlog(f"  HTTP {resp.status_code}  ({len(resp.content)} bytes)")
    resp.raise_for_status()
    return resp.json().get("items", [])

# ── Snapshot storage ───────────────────────────────────────────────────────────

def store_snapshot(
    conn: sqlite3.Connection,
    today: str,
    period_type: str,
    section_index: int,
    period_index: int,
    participants: list,
) -> None:
    vlog(f"Storing race snapshot for {len(participants)} participant(s) [{today}] …")

    existing_period_types = period_types_for_day(conn, today)
    if existing_period_types and period_type not in existing_period_types:
        existing_display = ",".join(sorted(existing_period_types))
        print(
            f"[Data Warning] Period type changed for clash day {today}: "
            f"existing={existing_display}, incoming={period_type}. "
            "Preserving warDay when present."
        )

    for p in participants:
        decks_used = p.get("decksUsed", 0)
        decks_used_today = derive_decks_used_today(
            conn,
            today,
            period_type,
            section_index,
            p["tag"],
            decks_used,
        )
        conn.execute(
            """
            INSERT INTO snapshots
                (snapshot_date, period_type, section_index, period_index,
                 player_tag, player_name,
                 fame, repair_points, boat_attacks, decks_used, decks_used_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, player_tag) DO UPDATE SET
                -- Preserve war-day classification once observed for a clash day.
                -- The API can flip to training near boundaries; don't downgrade.
                period_type      = CASE
                    WHEN snapshots.period_type = 'warDay' OR excluded.period_type = 'warDay'
                        THEN 'warDay'
                    ELSE excluded.period_type
                END,
                section_index    = excluded.section_index,
                period_index     = excluded.period_index,
                player_name      = excluded.player_name,
                -- Keep the highest values seen during hourly runs for the same
                -- clash day so transient API regressions don't overwrite progress.
                fame             = MAX(snapshots.fame, excluded.fame),
                repair_points    = MAX(snapshots.repair_points, excluded.repair_points),
                boat_attacks     = MAX(snapshots.boat_attacks, excluded.boat_attacks),
                decks_used       = MAX(snapshots.decks_used, excluded.decks_used),
                decks_used_today = MAX(snapshots.decks_used_today, excluded.decks_used_today)
            """,
            (
                today,
                period_type,
                section_index,
                period_index,
                p["tag"],
                p["name"],
                p.get("fame", 0),
                p.get("repairPoints", 0),
                p.get("boatAttacks", 0),
                decks_used,
                decks_used_today,
            ),
        )
    conn.commit()

def store_members_snapshot(conn: sqlite3.Connection, today: str, members: list) -> None:
    vlog(f"Storing member snapshot for {len(members)} member(s) [{today}] …")

    # Replace the day's roster so churn doesn't accumulate stale ex-members.
    # Without this, repeated runs can exceed 50 tracked members for one day.
    conn.execute("DELETE FROM member_snapshots WHERE snapshot_date = ?", (today,))

    for m in members:
        conn.execute(
            """
            INSERT INTO member_snapshots
                (snapshot_date, player_tag, player_name, role,
                 exp_level, trophies, donations, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                today,
                m["tag"],
                m["name"],
                m.get("role", "member"),
                m.get("expLevel", 0),
                m.get("trophies", 0),
                m.get("donations", 0),
                m.get("lastSeen", ""),
            ),
        )
    conn.commit()


# ── Boot logic ─────────────────────────────────────────────────────────────────

def get_prior_tags(conn: sqlite3.Connection, today: str) -> set:
    """Tags that appear in at least one snapshot *before* today — i.e. not brand-new."""
    rows = conn.execute(
        "SELECT DISTINCT player_tag FROM snapshots WHERE snapshot_date < ?",
        (today,),
    ).fetchall()
    return {r["player_tag"] for r in rows}


def find_boot_candidates(
    conn: sqlite3.Connection,
    today: str,
    period_type: str,
    section_index: int,
    participants: list,
    active_members: list,
    prior_tags: set,
) -> list:
    """
    Returns a list of dicts describing under-performing members.

    Only current clan members (from the /members endpoint) are evaluated —
    players who left the clan since the race started are ignored.

        Rules (only evaluated on war days):
      - NEW MEMBER GRACE: members whose tag wasn't seen before today are skipped.
        They may have had limited decks available on their first race day.
            - NO PARTICIPATION: active members not in the participants list at all get
                flagged immediately (0 decks, 0 fame).
            - ZERO DECKS TODAY: flagged if decksUsedToday == 0.

    """
    if period_type != "warDay":
        vlog("Period is not warDay — skipping boot evaluation.")
        return []   # Don't boot people for training-day non-participation

    # Build a fast lookup of race participants by tag
    participant_by_tag = {p["tag"]: p for p in participants}

    candidates = []
    for m in active_members:
        tag  = m["tag"]
        name = m["name"]

        # Permanently exempt members (owners / OGs) — never flag, ever.
        if tag.upper() in EXEMPT_MEMBERS:
            vlog(f"  EXEMPT {name} ({tag}) — permanently exempt, skipping")
            continue

        # Grace: skip members who joined today (first race day, may have used decks elsewhere)
        if tag not in prior_tags:
            vlog(f"  GRACE  {name} ({tag}) — first day, skipping")
            continue

        p = participant_by_tag.get(tag)  # None if they never entered the race

        decks_total = p.get("decksUsed", 0) if p else 0
        fame        = p.get("fame", 0) if p else 0
        prior_war_days, prior_decks = prior_war_progress(conn, tag, today, section_index)
        decks_today = max(0, decks_total - prior_decks) if p else 0

        reasons = []

        if p is None:
            reasons.append("**not participating** in the race at all")
        elif decks_today == 0:
            reasons.append("used **0 decks** today")

        if prior_war_days > 0:
            expected = prior_war_days * MIN_DECKS_PER_DAY
            if decks_total < expected * MIN_PARTICIPATION_PCT:
                reasons.append(
                    f"only **{decks_total}/{expected}** expected decks used overall"
                )

        if reasons:
            _, action = _role_action(m.get("role", "member"))
            vlog(f"  FLAG   {name} ({tag}) [{m.get('role','member')}] → {action} | {'; '.join(reasons)}")
            candidates.append(
                {
                    "name":             name,
                    "tag":              tag,
                    "role":             m.get("role", "member"),
                    "fame":             fame,
                    "repair_points":    p.get("repairPoints", 0) if p else 0,
                    "boat_attacks":     p.get("boatAttacks",  0) if p else 0,
                    "decks_used":       decks_total,
                    "decks_used_today": decks_today,
                    "prior_war_days":   prior_war_days,
                    "last_seen":        m.get("lastSeen", ""),
                    "reasons":          reasons,
                }
            )

    # Sort by role (leader → co-leader → elder → member), then newest members first
    if candidates:
        tags_placeholder = ",".join("?" * len(candidates))
        join_rows = conn.execute(
            f"""
            SELECT player_tag, MIN(snapshot_date) AS first_seen
            FROM member_snapshots
            WHERE player_tag IN ({tags_placeholder})
            GROUP BY player_tag
            """,
            [c["tag"] for c in candidates],
        ).fetchall()
        first_seen_by_tag = {r["player_tag"]: r["first_seen"] for r in join_rows}
        for c in candidates:
            c["joined_date"] = first_seen_by_tag.get(c["tag"], today)

    role_order = {"leader": 0, "coLeader": 1, "elder": 2, "member": 3}
    candidates.sort(key=lambda c: c.get("joined_date", ""), reverse=True)   # newest members first
    candidates.sort(key=lambda c: role_order.get(c["role"], 4))              # then by role (stable)
    vlog(f"Boot evaluation complete: {len(candidates)} candidate(s) from {len(active_members)} active member(s).")
    return candidates


def find_top_performers(participants: list, active_tags: set) -> dict:
    """
    Returns the top 3 distinct fame tiers, with every player tied at each tier.
    e.g. if four players share 2nd-place fame, all four appear under 🥈.
    Tiebreaker within a tier: fewer decks used = more efficient, listed first.
    """
    active  = [p for p in participants if p["tag"] in active_tags]
    by_fame = sorted(active, key=lambda p: (-p.get("fame", 0), p.get("decksUsed", 0)))

    # Build ordered list of up to TOP_PERFORMERS_N distinct fame values
    tiers = []  # list of (fame_value, [players])
    for p in by_fame:
        fame = p.get("fame", 0)
        if tiers and tiers[-1][0] == fame:
            tiers[-1][1].append(p)
        elif len(tiers) < TOP_PERFORMERS_N:
            tiers.append((fame, [p]))
        else:
            break  # already have TOP_PERFORMERS_N distinct tiers

    return {"tiers": tiers}


def find_boat_offenders(participants: list, active_tags: set) -> list:
    """
    Returns active members who used boat attacks, sorted by most attacks first.
    Boat attacks are discouraged: only 125 fame per tower vs 200-250 for
    regular/dual battles.
    """
    active = [p for p in participants if p["tag"] in active_tags]
    offenders = [p for p in active if p.get("boatAttacks", 0) > 0]
    offenders.sort(key=lambda p: p.get("boatAttacks", 0), reverse=True)
    return offenders

# ── Reporting ──────────────────────────────────────────────────────────────────

ACTION_MAP = {
    "leader":   (":crown:",              "Flag for review"),      # shouldn't normally trigger
    "coLeader": (":shield:",             "Demote to Elder"),
    "elder":    (":leaves:",             "Demote to Member"),
    "member":   (":bust_in_silhouette:", "Boot"),
}


def _role_action(role: str) -> tuple:
    """Returns (emoji, action_label) for a given clan role."""
    return ACTION_MAP.get(role, (":bust_in_silhouette:", "Boot"))


def _console_report(candidates: list, top: dict, boat_offenders: list, clan_name: str, today: str, period_type: str, member_count: int) -> None:
    print(f"\n=== River Race Boot Report  {today}  [{clan_name}]  period: {period_type}  members: {member_count} ===")
    if not candidates:
        print("  ✓ No action required — everyone is participating.")
    else:
        for c in candidates:
            if c.get("safe"):
                label = "WATCH"
            else:
                _, label = _role_action(c["role"])
                label = label.upper()
            print(
                f"  • [{label}] {c['name']} ({c['tag']}) [{c['role']}]"
                f" | fame={c['fame']}"
                f" | decks={c['decks_used']} (today={c['decks_used_today']})"
                f" | war days tracked={c['prior_war_days']}"
                f" | last seen={c['last_seen']}"
                f" | {'; '.join(c['reasons'])}"
            )

    if top and top.get("tiers"):
        print(f"\n=== Top Performers ===")
        medals = ["🥇", "🥈", "🥉"]
        for pos, (fame_val, players) in enumerate(top["tiers"]):
            medal = medals[pos]
            for p in players:
                print(f"  {medal} {p['name']} ({p['tag']})  fame={p.get('fame',0)}  decks={p.get('decksUsed',0)}")
        if boat_offenders:
            print(f"\n=== Boat Attack Warning ===")
            for b in boat_offenders:
                print(
                    f"  ⚠️  {b['name']} ({b['tag']})  boat_attacks={b.get('boatAttacks',0)}"
                    f"  (use regular/dual battles instead — up to 2x more fame per deck)"
                )
    print()


def send_discord_report(
    candidates: list,
    top: dict,
    boat_offenders: list,
    clan_name: str,
    today: str,
    period_type: str,
    member_count: int = 0,
) -> bool:
    _console_report(candidates, top, boat_offenders, clan_name, today, period_type, member_count)

    if not DISCORD_WEBHOOK:
        print("[Discord] DISCORD_WEBHOOK not set — skipping Discord notification.")
        return False

    payload_batches = []

    if period_type != "warDay":
        # Optionally send a quiet note on training days
        payload_batches = [{
            "embeds": [
                {
                    "title": f"River Race Snapshot — {today}",
                    "description": (
                        f"Snapshot date **{today}** is a **{period_type}** day for **{clan_name}** "
                        f"({member_count} active members). "
                        "No boot evaluation on non-war days."
                    ),
                    "color": 0x888888,
                    "footer": {"text": "Clash Royale Boot Bot"},
                }
            ]
        }]
    else:
        embeds = []

        # ── Shoutout embed (gold) ──────────────────────────────────────────────
        if top and top.get("tiers"):
            shoutout_fields = []
            medals = [":first_place:", ":second_place:", ":third_place:"]
            for pos, (fame_val, players) in enumerate(top["tiers"]):
                medal = medals[pos]
                for p in players:
                    shoutout_fields.append({
                        "name": f"{medal} {p['name']}  (`{p['tag']}`)",
                        "value": (
                            f"Fame: **{p.get('fame', 0)}** | "
                            f"Decks: **{p.get('decksUsed', 0)}**"
                        ),
                        "inline": False,
                    })
            shoutout_chunks = [shoutout_fields[i:i+10] for i in range(0, len(shoutout_fields), 10)]
            for i, chunk in enumerate(shoutout_chunks):
                embed = {
                    "title": (
                        f":trophy: Top Performers — {today}"
                        + (f" ({i+1}/{len(shoutout_chunks)})" if len(shoutout_chunks) > 1 else "")
                    ),
                    "color": 0xFFD700,
                    "fields": chunk,
                    "footer": {"text": "Clash Royale Boot Bot"},
                }
                if i == 0:
                    embed["description"] = f"Shoutout to the standout players in **{clan_name}** this race week!"
                embeds.append(embed)

        # ── Boat attack nudge embed (orange) ────────────────────────────────
        if boat_offenders:
            lines = "\n".join(
                f":boom: **{b['name']}** — {b.get('boatAttacks', 0)} boat attack(s)"
                for b in boat_offenders
            )
            embeds.append({
                "title": f":warning: Boat Attacks Used — {today}",
                "description": (
                    f"{lines}\n\n"
                    "_Boat attacks earn only 125 fame per tower (and some negligible damage to opponent bonus). "
                    "Duals give 250/100 (win/loss) and battles give 200/100 - please stick to those! "
                    "For example, winning dual plus two normal battles is 900 fame. That's the equivalent of 8 boat towers!_"
                ),
                "color": 0xFF8C00,
                "footer": {"text": "Clash Royale Boot Bot"},
            })

        # ── Boot / all-clear embed ─────────────────────────────────────────────
        action_candidates = [c for c in candidates if not c.get("safe")]
        watch_candidates  = [c for c in candidates if c.get("safe")]

        if not candidates:
            embeds.append({
                "title": f"River Race Report — {today}",
                "description": (
                    f":white_check_mark: All **{member_count}** members of **{clan_name}** "
                    "are participating - no action needed today!"
                ),
                "color": 0x00C851,
                "footer": {"text": "Clash Royale Boot Bot"},
            })
        else:
            # ── Actionable boots (red) ─────────────────────────────────────
            if action_candidates:
                fields = []
                for c in action_candidates:
                    role_icon, action = _role_action(c["role"])
                    last_seen_display = c["last_seen"][:8] if len(c["last_seen"]) >= 8 else c["last_seen"] or "unknown"
                    fields.append({
                        "name": f"{role_icon} {c['name']}  (`{c['tag']}`)  →  **{action}**",
                        "value": (
                            f"Fame: **{c['fame']}** | "
                            f"Boat attacks: **{c['boat_attacks']}** | "
                            f"Decks used: **{c['decks_used']}** (today: **{c['decks_used_today']}**) | "
                            f"Last seen: **{last_seen_display}**\n"
                            f"Reason: {' • '.join(c['reasons'])}"
                        ),
                        "inline": False,
                    })
                action_description = (
                    f"**{len(action_candidates)}** of **{member_count}** members in **{clan_name}** "
                    "are under-participating:\n"
                    ":crown: **Leader** — flag for review  |  "
                    ":shield: **Co-Leader** → Demote to Elder  |  "
                    ":leaves: **Elder** → Demote to Member  |  "
                    ":bust_in_silhouette: **Member** → Boot"
                )
                chunks = [fields[i:i+10] for i in range(0, len(fields), 10)]
                for i, chunk in enumerate(chunks):
                    embed = {
                        "title": (
                            f":warning: River Race Action Report — {today}"
                            + (f" ({i+1}/{len(chunks)})" if len(chunks) > 1 else "")
                        ),
                        "color": 0xFF4444,
                        "fields": chunk,
                        "footer": {"text": "Clash Royale Boot Bot"},
                    }
                    if i == 0:
                        embed["description"] = action_description
                        embed["timestamp"] = datetime.utcnow().isoformat() + "Z"
                    embeds.append(embed)

            # ── Watch list (yellow) — protected by MIN_CLAN_SIZE ──────────
            if watch_candidates:
                watch_fields = []
                for c in watch_candidates:
                    role_icon, action = _role_action(c["role"])
                    last_seen_display = c["last_seen"][:8] if len(c["last_seen"]) >= 8 else c["last_seen"] or "unknown"
                    watch_fields.append({
                        "name": f":eyes: {c['name']}  (`{c['tag']}`)  —  **Watch** _{action} if recruiting improves_",
                        "value": (
                            f"Fame: **{c['fame']}** | "
                            f"Boat attacks: **{c['boat_attacks']}** | "
                            f"Decks used: **{c['decks_used']}** (today: **{c['decks_used_today']}**) | "
                            f"Last seen: **{last_seen_display}**\n"
                            f"Reason: {' • '.join(c['reasons'])}"
                        ),
                        "inline": False,
                    })
                watch_description = (
                    f"**{len(watch_candidates)}** under-performing member(s) are **protected** from action "
                    f"because the clan is at or near the minimum size of **{MIN_CLAN_SIZE}**. "
                    "They would be actioned once recruiting improves."
                )
                watch_chunks = [watch_fields[i:i+10] for i in range(0, len(watch_fields), 10)]
                for i, chunk in enumerate(watch_chunks):
                    embed = {
                        "title": (
                            f":eyes: Watch List — {today}"
                            + (f" ({i+1}/{len(watch_chunks)})" if len(watch_chunks) > 1 else "")
                        ),
                        "color": 0xFFCC00,
                        "fields": chunk,
                        "footer": {"text": "Clash Royale Boot Bot"},
                    }
                    if i == 0:
                        embed["description"] = watch_description
                    embeds.append(embed)

        payload_batches = [{"embeds": embeds[i:i+10]} for i in range(0, len(embeds), 10)]

    for batch_idx, payload in enumerate(payload_batches, start=1):
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[Discord] Warning: HTTP {resp.status_code} on batch {batch_idx}/{len(payload_batches)} — {resp.text}")
            return False

    n_action = sum(1 for c in candidates if not c.get("safe"))
    n_watch  = sum(1 for c in candidates if c.get("safe"))
    print(
        f"[Discord] Report sent ({n_action} actionable, {n_watch} watch-only, "
        f"{sum(len(pl) for _, pl in top.get('tiers', []))} shoutout(s), "
        f"{len(boat_offenders)} boat offender(s), out of {member_count} members, "
        f"across {len(payload_batches)} message(s))."
    )
    return True

# ── Replay from DB ────────────────────────────────────────────────────────────

def load_snapshot_from_db(conn: sqlite3.Connection, target_date: str) -> dict:
    """
    Reconstructs participants, members, and period metadata from saved DB rows
    for target_date.  Used by --date replay mode (no API calls made).
    """
    rows = conn.execute(
        """
        SELECT player_tag, player_name, period_type, section_index, period_index,
               fame, repair_points, boat_attacks, decks_used, decks_used_today
        FROM snapshots
        WHERE snapshot_date = ?
        """,
        (target_date,),
    ).fetchall()

    if not rows:
        raise LookupError(
            f"No race snapshot found for {target_date}. "
            "Only dates that have already been fetched can be replayed."
        )

    participants = [
        {
            "tag":            r["player_tag"],
            "name":           r["player_name"],
            "fame":           r["fame"],
            "repairPoints":   r["repair_points"],
            "boatAttacks":    r["boat_attacks"],
            "decksUsed":      r["decks_used"],
            "decksUsedToday": r["decks_used_today"],
        }
        for r in rows
    ]

    mem_rows = conn.execute(
        """
        SELECT player_tag, player_name, role, exp_level, trophies, donations, last_seen
        FROM member_snapshots
        WHERE snapshot_date = ?
        """,
        (target_date,),
    ).fetchall()

    if not mem_rows:
        raise LookupError(f"No member snapshot found for {target_date}.")

    members = [
        {
            "tag":      r["player_tag"],
            "name":     r["player_name"],
            "role":     r["role"],
            "expLevel": r["exp_level"],
            "trophies": r["trophies"],
            "donations":r["donations"],
            "lastSeen": r["last_seen"],
        }
        for r in mem_rows
    ]

    return {
        "period_type":   rows[0]["period_type"],
        "section_index": rows[0]["section_index"],
        "period_index":  rows[0]["period_index"],
        "participants":  participants,
        "members":       members,
    }


def build_report_from_snapshot(
    conn: sqlite3.Connection,
    report_date: str,
    snapped: dict,
) -> dict:
    """Builds candidates/top/offender report data from a stored snapshot."""
    participants = snapped["participants"]
    members = snapped["members"]
    period_type = snapped["period_type"]
    section_index = snapped["section_index"]

    prior_tags = get_prior_tags(conn, report_date)
    candidates = find_boot_candidates(
        conn,
        report_date,
        period_type,
        section_index,
        participants,
        members,
        prior_tags,
    )

    active_tags = {m["tag"] for m in members}
    member_count = len(members)
    top = find_top_performers(participants, active_tags) if period_type == "warDay" else {}
    boat_offenders = find_boat_offenders(participants, active_tags) if (period_type == "warDay" and REPORT_BOAT_ATTACKS) else []

    # Demotions (co-leader, elder) don't reduce headcount — always actioned.
    # Only outright boots (member role) are guarded by MIN_CLAN_SIZE.
    bootable = [c for c in candidates if c["role"] == "member"]
    max_boots = max(0, member_count - MIN_CLAN_SIZE)
    if max_boots == 0:
        for c in candidates:
            c["safe"] = c["role"] == "member"
    elif max_boots < len(bootable):
        worst_first = sorted(bootable, key=lambda c: (c["fame"], c["decks_used"]))
        boot_tags = {c["tag"] for c in worst_first[:max_boots]}
        for c in candidates:
            c["safe"] = c["role"] == "member" and c["tag"] not in boot_tags
    else:
        for c in candidates:
            c["safe"] = False

    return {
        "report_date": report_date,
        "participants": participants,
        "members": members,
        "period_type": period_type,
        "section_index": section_index,
        "period_index": snapped["period_index"],
        "member_count": member_count,
        "candidates": candidates,
        "top": top,
        "boat_offenders": boat_offenders,
    }

# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    global VERBOSE
    parser = argparse.ArgumentParser(description="Clash Royale Boot Bot")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print detailed progress logging")
    parser.add_argument("--skip-discord", action="store_true", help="Print the report but do not send it to Discord")
    parser.add_argument(
        "--date", "-d",
        metavar="YYYY-MM-DD",
        help="Replay a historical date using saved DB snapshots (no API calls made)",
    )
    args = parser.parse_args()
    VERBOSE = args.verbose
    skip_discord = args.skip_discord

    if args.date:
        try:
            date.fromisoformat(args.date)
        except ValueError:
            sys.exit(f"Error: --date must be YYYY-MM-DD, got '{args.date}'.")

    now_utc = datetime.now(timezone.utc)
    now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Replay path (--date) ───────────────────────────────────────────────────
    if args.date:
        today = args.date
        print(f"[{now}] Replaying saved snapshot for {today} (no API calls) …")
        with get_db() as conn:
            init_db(conn)
            try:
                snapped = load_snapshot_from_db(conn, today)
            except LookupError as ex:
                sys.exit(f"Error: {ex}")
            report = build_report_from_snapshot(conn, today, snapped)

        if skip_discord:
            _console_report(
                report["candidates"],
                report["top"],
                report["boat_offenders"],
                CLAN_TAG,
                report["report_date"],
                report["period_type"],
                report["member_count"],
            )
            print("[Discord] Skipped (--skip-discord).")
        else:
            send_discord_report(
                report["candidates"],
                report["top"],
                report["boat_offenders"],
                CLAN_TAG,
                report["report_date"],
                report["period_type"],
                report["member_count"],
            )
        return

    # ── Live path ───────────────────────────────────────────────────────────────
    today = clash_day_from_utc(now_utc)
    reset_at = reset_datetime_utc(now_utc)
    before_reset = now_utc < reset_at

    if not API_TOKEN:
        sys.exit("Error: CR_API_TOKEN is not set. Copy .env.example to .env and fill it in.")

    print(
        f"[{now}] Fetching river race data for {CLAN_TAG} … "
        f"(clash day: {today}, reset: {CLASH_RESET_UTC_HOUR:02d}:{CLASH_RESET_UTC_MINUTE:02d} UTC)"
    )
    data = fetch_river_race()
    members = fetch_members()
    clan_info = data.get("clan", {})
    clan_name = clan_info.get("name", CLAN_TAG)
    participants = clan_info.get("participants", [])
    period_type = data.get("periodType", "unknown")
    section_index = data.get("sectionIndex", 0)
    period_index = data.get("periodIndex", 0)

    print(
        f"  Clan: {clan_name}  |  Active members: {len(members)}"
        f"  |  Race participants: {len(participants)}"
        f"  |  Period: {period_type}  (section={section_index}, period={period_index})"
    )

    report_to_send = None
    report_date = pick_report_date(today, period_type, before_reset)

    with get_db() as conn:
        init_db(conn)
        store_snapshot(conn, today, period_type, section_index, period_index, participants)
        store_members_snapshot(conn, today, members)

        if VERBOSE:
            participant_by_tag = {p["tag"]: p for p in participants}
            role_order = {"leader": 0, "coLeader": 1, "elder": 2, "member": 3}
            for m in sorted(members, key=lambda x: (role_order.get(x.get("role", "member"), 9), x["name"])):
                p = participant_by_tag.get(m["tag"])
                decks_total = p.get("decksUsed", 0) if p else 0
                fame = p.get("fame", 0) if p else 0
                in_race = "in race" if p else "NOT IN RACE"
                decks_today = derive_decks_used_today(
                    conn, today, period_type, section_index, m["tag"], decks_total
                )
                print(
                    f"  [v]   {m['name']:<20} ({m['tag']:<12}) [{m.get('role','?'):<9}]"
                    f"  fame={fame:<6}  decks={decks_total} today={decks_today}  {in_race}"
                )

        if report_date is None:
            print("[Discord] Before UTC reset on war day — data updated, no Discord report sent.")
            return

        if report_was_sent(conn, report_date):
            print(f"[Discord] Report for {report_date} already sent — skipping duplicate.")
            return

        try:
            snapped_report = load_snapshot_from_db(conn, report_date)
        except LookupError as ex:
            print(f"[Discord] Cannot build report for {report_date}: {ex}")
            return

        report_to_send = build_report_from_snapshot(conn, report_date, snapped_report)

    if skip_discord:
        _console_report(
            report_to_send["candidates"],
            report_to_send["top"],
            report_to_send["boat_offenders"],
            clan_name,
            report_to_send["report_date"],
            report_to_send["period_type"],
            report_to_send["member_count"],
        )
        print("[Discord] Skipped (--skip-discord).")
        return

    sent = send_discord_report(
        report_to_send["candidates"],
        report_to_send["top"],
        report_to_send["boat_offenders"],
        clan_name,
        report_to_send["report_date"],
        report_to_send["period_type"],
        report_to_send["member_count"],
    )
    if sent:
        with get_db() as conn:
            init_db(conn)
            mark_report_sent(conn, report_date, now_utc)


if __name__ == "__main__":
    main()
