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
import time
import requests
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# Fix timezone so midnight == 10:00am UTC == Clash game day reset.
# Etc/GMT+10 is POSIX notation for UTC-10 (signs are inverted in POSIX).
os.environ["TZ"] = "Etc/GMT+10"
time.tzset()

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
    """)
    conn.commit()

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
    for p in participants:
        conn.execute(
            """
            INSERT INTO snapshots
                (snapshot_date, period_type, section_index, period_index,
                 player_tag, player_name,
                 fame, repair_points, boat_attacks, decks_used, decks_used_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, player_tag) DO UPDATE SET
                period_type      = excluded.period_type,
                section_index    = excluded.section_index,
                period_index     = excluded.period_index,
                player_name      = excluded.player_name,
                fame             = excluded.fame,
                repair_points    = excluded.repair_points,
                boat_attacks     = excluded.boat_attacks,
                decks_used       = excluded.decks_used,
                decks_used_today = excluded.decks_used_today
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
                p.get("decksUsed", 0),
                p.get("decksUsedToday", 0),
            ),
        )
    conn.commit()

def store_members_snapshot(conn: sqlite3.Connection, today: str, members: list) -> None:
    vlog(f"Storing member snapshot for {len(members)} member(s) [{today}] …")
    for m in members:
        conn.execute(
            """
            INSERT INTO member_snapshots
                (snapshot_date, player_tag, player_name, role,
                 exp_level, trophies, donations, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, player_tag) DO UPDATE SET
                player_name = excluded.player_name,
                role        = excluded.role,
                exp_level   = excluded.exp_level,
                trophies    = excluded.trophies,
                donations   = excluded.donations,
                last_seen   = excluded.last_seen
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
      - LOW CUMULATIVE: flagged if decksUsed < (prior_war_days * MIN_DECKS_PER_DAY
        * MIN_PARTICIPATION_PCT). Prior war days = number of warDay snapshots
        recorded for that player before today.
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

        decks_today = p.get("decksUsedToday", 0) if p else 0
        decks_total = p.get("decksUsed",      0) if p else 0
        fame        = p.get("fame",           0) if p else 0

        # Count how many warDay snapshots we have for this member (prior to today)
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM snapshots
            WHERE player_tag = ?
              AND snapshot_date < ?
              AND period_type = 'warDay'
            """,
            (tag, today),
        ).fetchone()
        prior_war_days = row["cnt"] if row else 0

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
) -> None:
    _console_report(candidates, top, boat_offenders, clan_name, today, period_type, member_count)

    if not DISCORD_WEBHOOK:
        print("[Discord] DISCORD_WEBHOOK not set — skipping Discord notification.")
        return

    if period_type != "warDay":
        # Optionally send a quiet note on training days
        payload = {
            "embeds": [
                {
                    "title": f"River Race Snapshot — {today}",
                    "description": (
                        f"Today is a **{period_type}** day for **{clan_name}** "
                        f"({member_count} active members). "
                        "No boot evaluation on non-war days."
                    ),
                    "color": 0x888888,
                    "footer": {"text": "Clash Royale Boot Bot"},
                }
            ]
        }
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
            embeds.append({
                "title": f":trophy: Top Performers — {today}",
                "description": f"Shoutout to the standout players in **{clan_name}** this race week!",
                "color": 0xFFD700,
                "fields": shoutout_fields,
                "footer": {"text": "Clash Royale Boot Bot"},
            })

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
                embeds.append({
                    "title": f":warning: River Race Action Report — {today}",
                    "description": (
                        f"**{len(action_candidates)}** of **{member_count}** members in **{clan_name}** "
                        "are under-participating:\n"
                        ":crown: **Leader** — flag for review  |  "
                        ":shield: **Co-Leader** → Demote to Elder  |  "
                        ":leaves: **Elder** → Demote to Member  |  "
                        ":bust_in_silhouette: **Member** → Boot"
                    ),
                    "color": 0xFF4444,
                    "fields": fields[:25],
                    "footer": {"text": "Clash Royale Boot Bot"},
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                })

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
                embeds.append({
                    "title": f":eyes: Watch List — {today}",
                    "description": (
                        f"**{len(watch_candidates)}** under-performing member(s) are **protected** from action "
                        f"because the clan is at or near the minimum size of **{MIN_CLAN_SIZE}**. "
                        "They would be actioned once recruiting improves."
                    ),
                    "color": 0xFFCC00,
                    "fields": watch_fields[:25],
                    "footer": {"text": "Clash Royale Boot Bot"},
                })

        payload = {"embeds": embeds}

    resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[Discord] Warning: HTTP {resp.status_code} — {resp.text}")
    else:
        n_action = sum(1 for c in candidates if not c.get("safe"))
        n_watch  = sum(1 for c in candidates if c.get("safe"))
        print(
            f"[Discord] Report sent ({n_action} actionable, {n_watch} watch-only, "
            f"{sum(len(pl) for _, pl in top.get('tiers', []))} shoutout(s), "
            f"{len(boat_offenders)} boat offender(s), out of {member_count} members)."
        )

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
        sys.exit(
            f"Error: No race snapshot found for {target_date}. "
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
        sys.exit(f"Error: No member snapshot found for {target_date}.")

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

# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    global VERBOSE
    parser = argparse.ArgumentParser(description="Clash Royale Boot Bot")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print detailed progress logging")
    parser.add_argument(
        "--date", "-d",
        metavar="YYYY-MM-DD",
        help="Replay a historical date using saved DB snapshots (no API calls made)",
    )
    args = parser.parse_args()
    VERBOSE = args.verbose

    if args.date:
        try:
            date.fromisoformat(args.date)
        except ValueError:
            sys.exit(f"Error: --date must be YYYY-MM-DD, got '{args.date}'.")
        today = args.date
    else:
        today = date.today().isoformat()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Load data — either live from API or replayed from DB ───────────────────
    if args.date:
        print(f"[{now}] Replaying saved snapshot for {today} (no API calls) …")
        with get_db() as conn:
            init_db(conn)
            snapped       = load_snapshot_from_db(conn, today)
            prior_tags    = get_prior_tags(conn, today)
            vlog(f"Prior tag count (seen before {today}): {len(prior_tags)}")
            candidates    = find_boot_candidates(
                conn, today, snapped["period_type"], snapped["participants"], snapped["members"], prior_tags
            )
        participants  = snapped["participants"]
        members       = snapped["members"]
        clan_name     = CLAN_TAG
        period_type   = snapped["period_type"]
        section_index = snapped["section_index"]
        period_index  = snapped["period_index"]
    else:
        if not API_TOKEN:
            sys.exit("Error: CR_API_TOKEN is not set. Copy .env.example to .env and fill it in.")
        print(f"[{now}] Fetching river race data for {CLAN_TAG} …")
        data    = fetch_river_race()
        members = fetch_members()
        clan_info     = data.get("clan", {})
        clan_name     = clan_info.get("name", CLAN_TAG)
        participants  = clan_info.get("participants", [])
        period_type   = data.get("periodType", "unknown")
        section_index = data.get("sectionIndex", 0)
        period_index  = data.get("periodIndex", 0)
        with get_db() as conn:
            init_db(conn)
            prior_tags = get_prior_tags(conn, today)
            vlog(f"Prior tag count (seen before {today}): {len(prior_tags)}")
            store_snapshot(conn, today, period_type, section_index, period_index, participants)
            store_members_snapshot(conn, today, members)
            candidates = find_boot_candidates(
                conn, today, period_type, participants, members, prior_tags
            )

    # ── Shared post-load logic ─────────────────────────────────────────────────
    active_tags  = {m["tag"] for m in members}
    member_count = len(members)

    print(
        f"  Clan: {clan_name}  |  Active members: {member_count}"
        f"  |  Race participants: {len(participants)}"
        f"  |  Period: {period_type}  (section={section_index}, period={period_index})"
    )

    # Verbose: dump full member roster
    if VERBOSE:
        role_order = {"leader": 0, "coLeader": 1, "elder": 2, "member": 3}
        for m in sorted(members, key=lambda x: (role_order.get(x.get("role", "member"), 9), x["name"])):
            p = next((x for x in participants if x["tag"] == m["tag"]), None)
            decks_today = p.get("decksUsedToday", 0) if p else 0
            decks_total = p.get("decksUsed",      0) if p else 0
            fame        = p.get("fame",           0) if p else 0
            in_race     = "in race" if p else "NOT IN RACE"
            print(
                f"  [v]   {m['name']:<20} ({m['tag']:<12}) [{m.get('role','?'):<9}]"
                f"  fame={fame:<6}  decks={decks_total} today={decks_today}  {in_race}"
            )

    top            = find_top_performers(participants, active_tags) if period_type == "warDay" else {}
    boat_offenders = find_boat_offenders(participants, active_tags) if (period_type == "warDay" and REPORT_BOAT_ATTACKS) else []
    vlog(f"Top performers: {[(fame_val, [p['name'] for p in players]) for fame_val, players in top.get('tiers', [])]}")
    vlog(f"Boat offenders: {[p['name'] for p in boat_offenders]}")

    # Demotions (co-leader, elder) don't reduce headcount — always actioned.
    # Only outright boots (member role) are guarded by MIN_CLAN_SIZE.
    bootable = [c for c in candidates if c["role"] == "member"]
    max_boots = max(0, member_count - MIN_CLAN_SIZE)
    if max_boots == 0:
        # At or below threshold — all members are watch-only; demotions still proceed.
        for c in candidates:
            c["safe"] = c["role"] == "member"
        vlog(
            f"[Boot Guard] At or below MIN_CLAN_SIZE={MIN_CLAN_SIZE} — "
            f"{len(bootable)} member candidate(s) flagged as watch-only."
        )
    elif max_boots < len(bootable):
        # More bootable members than available slots — worst members get booted,
        # the remainder are watch-only; demotions are unaffected.
        worst_first = sorted(bootable, key=lambda c: (c["fame"], c["decks_used"]))
        boot_tags   = {c["tag"] for c in worst_first[:max_boots]}
        for c in candidates:
            c["safe"] = c["role"] == "member" and c["tag"] not in boot_tags
        vlog(
            f"[Boot Guard] {max_boots} actionable boot(s), "
            f"{len(bootable) - max_boots} watch-only (MIN_CLAN_SIZE={MIN_CLAN_SIZE})."
        )
    else:
        for c in candidates:
            c["safe"] = False

    send_discord_report(candidates, top, boat_offenders, clan_name, today, period_type, member_count)


if __name__ == "__main__":
    main()
