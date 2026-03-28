#!/usr/bin/env python3
"""
report.py — Read SQLite and report on members who didn't participate on
the most recently completed war day.

Run this after ingest has observed the next API phase/day so the previous
war day is fully settled before we evaluate it.

Usage:
    python report.py                         # auto-select previous completed war section
    python report.py --section 25:warDay:3  # report a specific API section
    python report.py --dry-run              # print without sending to Discord
"""

import argparse
import os
import sys

import requests
from dotenv import load_dotenv

import db

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
CLAN_TAG     = os.getenv("CLAN_TAG", "#PJ8Q8P")
DB_PATH      = os.getenv("DB_PATH",  db.DB_PATH)
MIN_DECKS         = int(os.getenv("MIN_DECKS_PER_DAY",   "4"))
PROMOTE_FAME      = int(os.getenv("PROMOTE_FAME",        "2000"))
PROMOTE_WAR_COUNT = int(os.getenv("PROMOTE_WAR_COUNT",   "3"))

_env = os.getenv("ENVIRONMENT", "").strip().lower()
if _env == "production":
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_PROD", "")
else:
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_DEV", "")

# Comma-separated player tags that are never flagged (#ABC123,#DEF456).
EXEMPT_TAGS: set[str] = {
    t.strip().upper()
    for t in os.getenv("EXEMPT_MEMBERS", "").split(",")
    if t.strip()
}

# Discord role action labels per clan role
ROLE_ACTIONS = {
    "leader":   "Flag for review",
    "coLeader": "Demote to Elder",
    "elder":    "Demote to Member",
    "member":   "Boot",
}


# ── Evaluation ─────────────────────────────────────────────────────────────────

def find_non_participants(
    conn,
    period_index: int,
    period_type: str,
    section_index: int,
) -> list[dict]:
    """
    Returns members who used fewer than MIN_DECKS on the given war day.

    - Only runs against warDay snapshots.
    - New members (no prior snapshot before this day) get a grace pass.
    - Exempt tags are always skipped.
    """
    snapshots = db.get_snapshot(conn, period_index, period_type, section_index)
    if not snapshots:
        return []

    period_type = snapshots[0]["period_type"]
    if period_type != "warDay":
        return []

    members    = db.get_members(conn, period_index, period_type, section_index)
    known_tags = db.known_tags_before(conn, period_index, period_type, section_index)
    snap_by_tag = {row["player_tag"]: row for row in snapshots}

    flagged = []
    for m in members:
        tag  = m["player_tag"]
        name = m["player_name"]

        if tag.upper() in EXEMPT_TAGS:
            continue

        if tag not in known_tags:
            continue

        snap = snap_by_tag.get(tag)
        decks_section = (snap["decks_used"] - snap["decks_used_initial"]) if snap else 0

        if decks_section < MIN_DECKS:
            action = ROLE_ACTIONS.get(m["role"], "Boot")
            first_seen = conn.execute(
                """
                SELECT MIN(s.sequence) AS seq
                FROM section_snapshots ss
                JOIN sections s
                  ON s.period_index = ss.period_index
                 AND s.period_type  = ss.period_type
                 AND s.section_index = ss.section_index
                WHERE ss.player_tag = ?
                """,
                (tag,),
            ).fetchone()
            flagged.append({
                "name":             name,
                "tag":              tag,
                "role":             m["role"],
                "action":           action,
                "decks_section":    decks_section,
                "fame":             snap["fame"]          if snap else 0,
                "decks_used":       snap["decks_used"]    if snap else 0,
                "trophies":         m["trophies"],
                "first_seen_seq":   first_seen["seq"]     if first_seen else 0,
            })

    # Newest members (highest first-seen sequence) at top, oldest at bottom.
    flagged.sort(key=lambda c: c["first_seen_seq"], reverse=True)
    return flagged


def find_promotion_candidates(
    conn,
    period_index: int,
    period_type: str,
    section_index: int,
) -> list[dict]:
    """
    Returns 'member'-role players who earned >= PROMOTE_FAME fame
    in each of the last PROMOTE_WAR_COUNT completed warDay sections.
    """
    current = conn.execute(
        "SELECT sequence FROM sections WHERE period_index=? AND period_type=? AND section_index=?",
        (period_index, period_type, section_index),
    ).fetchone()
    if current is None:
        return []

    recent = conn.execute(
        """
        SELECT sequence FROM sections
        WHERE period_type = 'warDay' AND sequence <= ?
        ORDER BY sequence DESC
        LIMIT ?
        """,
        (current["sequence"], PROMOTE_WAR_COUNT),
    ).fetchall()

    if len(recent) < PROMOTE_WAR_COUNT:
        return []  # Not enough history yet

    sequences = [r["sequence"] for r in recent]
    placeholders = ",".join("?" * len(sequences))

    qualifying = conn.execute(
        f"""
        SELECT ss.player_tag
        FROM section_snapshots ss
        JOIN sections s
          ON s.period_index  = ss.period_index
         AND s.period_type   = ss.period_type
         AND s.section_index = ss.section_index
        WHERE s.sequence IN ({placeholders})
          AND ss.fame >= ?
        GROUP BY ss.player_tag
        HAVING COUNT(DISTINCT s.sequence) = ?
        """,
        (*sequences, PROMOTE_FAME, PROMOTE_WAR_COUNT),
    ).fetchall()
    qualifying_tags = {r["player_tag"] for r in qualifying}

    members = db.get_members(conn, period_index, period_type, section_index)
    candidates = [
        {"name": m["player_name"], "tag": m["player_tag"]}
        for m in members
        if m["player_tag"] in qualifying_tags and m["role"] == "member"
    ]
    candidates.sort(key=lambda c: c["name"].lower())
    return candidates


def find_top_performers(snapshots: list) -> list[list[dict]]:
    """
    Returns up to three podium tiers (1st, 2nd, 3rd) by fame.
    Each tier is a list of player dicts — ties at the same fame level share a rank.
    Tiers with fame == 0 are omitted.
    """
    if not snapshots:
        return []

    sorted_snaps = sorted(snapshots, key=lambda s: s["fame"], reverse=True)
    tiers: list[list[dict]] = []
    seen_fame: set[int] = set()
    for snap in sorted_snaps:
        fame = snap["fame"]
        if fame <= 0:
            break
        if fame not in seen_fame:
            if len(tiers) >= 3:
                break
            tiers.append([])
            seen_fame.add(fame)
        tiers[-1].append({"name": snap["player_name"], "tag": snap["player_tag"], "fame": fame})

    return tiers


# Action groups in display order
ACTION_ORDER = ["Boot", "Demote to Member", "Demote to Elder", "Flag for review"]


def _group_flagged(flagged: list[dict]) -> list[tuple[str, list[dict]]]:
    """Return (action_label, members) pairs in display order, skipping empty groups."""
    groups: dict[str, list[dict]] = {a: [] for a in ACTION_ORDER}
    for c in flagged:
        groups.setdefault(c["action"], []).append(c)
    return [(action, members) for action, members in groups.items() if members]


# ── Discord ────────────────────────────────────────────────────────────────────

PODIUM_MEDALS = ["🥇", "🥈", "🥉"]


def _build_lines(
    section_label: str,
    flagged: list[dict],
    top_performers: list[list[dict]],
    promotion_candidates: list[dict],
) -> list[str]:
    lines = [f"**Action Required — Section {section_label}**", ""]

    if not flagged:
        lines.append("No action required today. Everyone participated!")
    else:
        for action, group in _group_flagged(flagged):
            lines.append(f"**{action}**")
            for c in group:
                lines.append(
                    f"• **{c['name']}** [{c['trophies']:,}] — "
                    f"{c['decks_section']}/{MIN_DECKS} decks | {c['fame']} fame"
                )
            lines.append("")

    if promotion_candidates:
        lines.append("")
        lines.append(f"**Promote to Elder** ({PROMOTE_WAR_COUNT} wars ≥ {PROMOTE_FAME} fame)")
        for c in promotion_candidates:
            lines.append(f"⬆️ **{c['name']}** ({c['tag']})")

    if top_performers:
        lines.append("")
        lines.append("**Top Performers**")
        for i, tier in enumerate(top_performers):
            medal = PODIUM_MEDALS[i]
            names = ", ".join(f"**{p['name']}**" for p in tier)
            fame  = tier[0]["fame"]
            lines.append(f"{medal} {names} — {fame} fame")

    return lines


def send_discord(
    section_label: str,
    flagged: list[dict],
    top_performers: list[list[dict]],
    promotion_candidates: list[dict],
) -> None:
    if not DISCORD_WEBHOOK:
        print("DISCORD_WEBHOOK not set — skipping Discord send")
        return

    env_label = "PROD" if _env == "production" else "DEV"
    print(f"Sending to Discord ({env_label})...")

    content = "\n".join(_build_lines(section_label, flagged, top_performers, promotion_candidates))

    # Discord messages cap at 2000 chars; split on newlines only.
    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0
    for line in content.split("\n"):
        # +1 for the newline that join will add
        needed = len(line) + (1 if current_lines else 0)
        if current_lines and current_len + needed > 1990:
            chunks.append("\n".join(current_lines))
            current_lines = [line]
            current_len = len(line)
        else:
            current_lines.append(line)
            current_len += needed
    if current_lines:
        chunks.append("\n".join(current_lines))
    for chunk in chunks:
        resp = requests.post(
            DISCORD_WEBHOOK,
            json={"content": chunk},
            timeout=10,
        )
        resp.raise_for_status()

    print(f"Discord message sent ({len(flagged)} flagged)")


# ── Console ────────────────────────────────────────────────────────────────────


def print_report(
    section_label: str,
    flagged: list[dict],
    top_performers: list[list[dict]],
    promotion_candidates: list[dict],
) -> None:
    print("\n".join(_build_lines(section_label, flagged, top_performers, promotion_candidates)))
    print()


def parse_section_arg(value: str) -> tuple[int, str, int]:
    try:
        return db.parse_section_key(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Report boot candidates from SQLite.")
    parser.add_argument(
        "--section",
        type=parse_section_arg,
        help="Section to report as '<period_index>:<period_type>:<section_index>'. Defaults to the latest completed war section.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print report but do not send to Discord.")
    args = parser.parse_args()

    conn = db.get_db(DB_PATH)
    db.init_db(conn)

    # Choose which section to report.
    if args.section:
        period_index, period_type, section_index = args.section
    else:
        latest_section = db.latest_completed_war_section(conn)
        if latest_section is None:
            print("No completed war section found yet. Run ingest.py over at least two API sections first.")
            conn.close()
            sys.exit(1)
        period_index = latest_section["period_index"]
        period_type = latest_section["period_type"]
        section_index = latest_section["section_index"]

    section_label = db.section_key(period_index, period_type, section_index)

    snapshots = db.get_snapshot(conn, period_index, period_type, section_index)
    if not snapshots:
        print(f"No snapshot data found for {section_label}. Run ingest.py first.")
        conn.close()
        sys.exit(1)

    flagged               = find_non_participants(conn, period_index, period_type, section_index)
    top_performers        = find_top_performers(snapshots)
    promotion_candidates  = find_promotion_candidates(conn, period_index, period_type, section_index)

    print_report(section_label, flagged, top_performers, promotion_candidates)

    if not args.dry_run:
        send_discord(section_label, flagged, top_performers, promotion_candidates)

    conn.close()


if __name__ == "__main__":
    main()
