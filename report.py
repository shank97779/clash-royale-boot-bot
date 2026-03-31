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
MIN_DECKS_PER_DAY       = int(os.getenv("MIN_DECKS_PER_DAY",           "4"))
MAX_DECKS_PER_DAY       = 4  # Clash Royale war day maximum — not configurable
WORST_PERFORMERS_DAYS   = int(os.getenv("WORST_PERFORMERS_DAYS",         "16"))
WORST_PERFORMERS_SHOW   = int(os.getenv("WORST_PERFORMERS_SHOW",           "10"))
BEST_PERFORMERS_DAYS    = int(os.getenv("BEST_PERFORMERS_DAYS",          "16"))
BEST_PERFORMERS_SHOW    = int(os.getenv("BEST_PERFORMERS_SHOW",            "10"))

_env = os.getenv("ENVIRONMENT", "").strip().lower()
if _env == "production":
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_PROD", "")
else:
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_DEV", "")

# Comma-separated player tags that are never flagged (#ABC123,#DEF456).
EXEMPT_TAGS: set[str] = {
    t.strip().upper()
    for t in os.getenv("EXEMPT_TAGS", "").split(",")
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
    snapshots: list,
    members: list,
) -> list[dict]:
    """
    Returns members who haven't used enough decks across the war weekend.

    Evaluation is cumulative over the whole weekend, not per-day:
      required = (completed_war_days - days_excused) * MIN_DECKS_PER_DAY
      flag if decks_used < required

    days_excused = number of war day sections where the player wasn't in the
    clan roster yet, including their join day (so new members owe nothing on
    the day they join and catch up at 4/day from the next day).

    Exempt tags are always skipped.
    """
    if not snapshots or snapshots[0]["period_type"] != "warDay":
        return []

    snap_by_tag = {row["player_tag"]: row for row in snapshots}

    weekend_sections  = db.war_weekend_sections(conn, period_index, period_type, section_index)
    weekend_sequences = [s["sequence"] for s in weekend_sections]
    completed_days    = len(weekend_sections)

    flagged = []
    for m in members:
        tag  = m["player_tag"]
        name = m["player_name"]

        if tag.upper() in EXEMPT_TAGS:
            continue

        excused   = db.days_excused(conn, tag, weekend_sequences)
        owed      = completed_days - excused
        required  = owed * MIN_DECKS_PER_DAY
        max_decks = owed * MAX_DECKS_PER_DAY

        if required <= 0:
            continue  # brand-new — owes nothing yet

        snap            = snap_by_tag.get(tag)
        decks_used      = snap["decks_used"]       if snap else 0
        decks_used_today = snap["decks_used_today"] if snap else 0

        if decks_used < required:
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
                "name":           name,
                "tag":            tag,
                "role":           m["role"],
                "action":         action,
                "decks_used":      decks_used,
                "decks_today":     decks_used_today,
                "required":       required,
                "max_decks":      max_decks,
                "owed_days":      owed,
                "excused":        excused,
                "fame":           snap["fame"] if snap else 0,
                "trophies":       m["trophies"],
                "first_seen_seq": first_seen["seq"] if first_seen else 0,
            })

    flagged.sort(key=lambda c: (c["decks_used"], c["fame"]))
    return flagged



def find_member_fame_stats(
    conn,
    period_index: int,
    period_type: str,
    section_index: int,
    num_days: int = WORST_PERFORMERS_DAYS,
    members: list | None = None,
) -> tuple[list[dict], int, float]:
    """
    Compute per-day average fame over the last num_days warDay sections for
    every current non-exempt member who has snapshot data.

    Returns (stats, actual_days, clan_avg_fame) where:
      - stats           : all qualifying members sorted worst-first (asc avg_fame)
      - actual_days     : warDay sections actually found (may be < num_days)
      - clan_avg_fame   : mean fame/day across ALL members (incl. exempt) for context

    Callers slice the list for their purpose:
      worst N  : stats[:n]                  (exclude already-flagged tags first)
      best  N  : reversed(stats)[:n]        (filter to role=='member' and threshold)

    Per-day fame diffs cumulative values within a war weekend; the first day
    of each new weekend uses its value directly (fame resets each war).
    """
    target = conn.execute(
        "SELECT sequence FROM sections WHERE period_index=? AND period_type=? AND section_index=?",
        (period_index, period_type, section_index),
    ).fetchone()
    if target is None:
        return [], 0, 0.0

    day_rows = conn.execute(
        """
        SELECT period_index, period_type, section_index, sequence
        FROM sections
        WHERE period_type = 'warDay' AND sequence <= ?
        ORDER BY sequence DESC
        LIMIT ?
        """,
        (target["sequence"], num_days),
    ).fetchall()
    if not day_rows:
        return [], 0, 0.0

    day_rows    = list(reversed(day_rows))  # oldest-first
    actual_days = len(day_rows)

    if members is None:
        members = db.get_members(conn, period_index, period_type, section_index)

    stats: list[dict] = []
    all_avg_fames: list[float] = []

    for m in members:
        tag      = m["player_tag"]
        name     = m["player_name"]
        daily    = _per_day_fame(conn, day_rows, tag)
        if not daily:
            continue
        avg_fame = sum(daily) / len(daily)
        all_avg_fames.append(avg_fame)
        if tag.upper() in EXEMPT_TAGS:
            continue
        stats.append({
            "name":         name,
            "tag":          tag,
            "role":         m["role"],
            "action":       ROLE_ACTIONS.get(m["role"], "Boot"),
            "days_tracked": len(daily),
            "avg_fame":     round(avg_fame),
            "trophies":     m["trophies"],
        })

    clan_avg_fame = round(sum(all_avg_fames) / len(all_avg_fames), 1) if all_avg_fames else 0.0
    stats.sort(key=lambda p: p["avg_fame"])
    return stats, actual_days, clan_avg_fame


def _per_day_fame(conn, day_rows: list, tag: str) -> list[int]:
    """Return per-day fame values (oldest-first) for `tag` across `day_rows`."""
    daily: list[int] = []
    for r in day_rows:
        snap = conn.execute(
            "SELECT fame_today FROM section_snapshots"
            " WHERE period_index=? AND period_type=? AND section_index=? AND player_tag=?",
            (r["period_index"], r["period_type"], r["section_index"], tag),
        ).fetchone()
        if snap is None or snap["fame_today"] is None:
            continue
        daily.append(snap["fame_today"])
    return daily


def find_top_performers(snapshots: list, trophies_by_tag: dict | None = None) -> list[list[dict]]:
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
        tiers[-1].append({
                "name":    snap["player_name"],
                "tag":     snap["player_tag"],
                "fame":    fame,
                "trophies": (trophies_by_tag or {}).get(snap["player_tag"].upper(), 0),
            })

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
    worst_performers: list[dict],
    worst_actual_days: int = WORST_PERFORMERS_DAYS,
    promo_actual_days: int = WORST_PERFORMERS_DAYS,
    clan_avg_fame: float = 0.0,
) -> list[str]:
    lines = [f"**Action Required — Section {section_label}**", ""]

    if not flagged:
        lines.append("No action required today. Everyone participated!")
    else:
        for action, group in _group_flagged(flagged):
            lines.append(f"**{action}**")
            for c in group:
                new_note = f" *(joined day {c['excused']})*" if c['excused'] else ""
                lines.append(
                    f"• **{c['name']}** [{c['trophies']:,}] — "
                    f"{c['decks_used']}/{c['max_decks']} decks (last: {c['decks_today']}) | {c['fame']} fame{new_note}"
                )
            lines.append("")

    if top_performers:
        lines.append("**Current War MVPs**")
        for i, tier in enumerate(top_performers):
            medal = PODIUM_MEDALS[i]
            names = ", ".join(f"**{p['name']}** [{p['trophies']:,}]" for p in tier)
            fame  = tier[0]["fame"]
            lines.append(f"{medal} {names} — {fame} fame")

    lines.append("")
    lines.append(f"**Best Performers** (last {promo_actual_days} days)")
    if promotion_candidates:
        for c in promotion_candidates:
            action = " → Promote to Elder" if c["role"] == "member" else ""
            lines.append(f"⬆️ **{c['name']}** [{c['trophies']:,}] — {c['avg_fame']:,} fame/day avg ({c['days_tracked']} days){action}")
    else:
        lines.append("No promotion candidates.")

    if worst_performers:
        lines.append("")
        lines.append(f"**Worst Performers (last {worst_actual_days} war days)**")
        if clan_avg_fame > 0:
            lines.append(f"_Clan average: {clan_avg_fame:,.0f} fame/day_")
        for p in worst_performers:
            lines.append(
                f"⚠️ **{p['name']}** [{p['trophies']:,}] — "
                f"{p['avg_fame']:,} fame/day avg ({p['days_tracked']} days) "
                f"→ {p['action']}"
            )

    return lines


def _split_content(content: str, max_len: int = 1990) -> list[str]:
    """Split content into chunks no larger than max_len at line boundaries."""
    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    for line in content.split("\n"):
        needed = len(line) + (1 if current_lines else 0)
        if current_lines and current_len + needed > max_len:
            chunks.append("\n".join(current_lines))
            current_lines = [line]
            current_len = len(line)
        else:
            current_lines.append(line)
            current_len += needed

    if current_lines:
        chunks.append("\n".join(current_lines))
    return chunks


def send_discord(
    section_label: str,
    flagged: list[dict],
    top_performers: list[list[dict]],
    promotion_candidates: list[dict],
    worst_performers: list[dict],
    worst_actual_days: int = WORST_PERFORMERS_DAYS,
    promo_actual_days: int = WORST_PERFORMERS_DAYS,
    clan_avg_fame: float = 0.0,
) -> None:
    if not DISCORD_WEBHOOK:
        print("DISCORD_WEBHOOK not set — skipping Discord send")
        return

    env_label = "PROD" if _env == "production" else "DEV"
    print(f"Sending to Discord ({env_label})...")

    content = "\n".join(_build_lines(section_label, flagged, top_performers, promotion_candidates, worst_performers, worst_actual_days, promo_actual_days, clan_avg_fame))
    for chunk in _split_content(content):
        resp = requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=10)
        resp.raise_for_status()

    print(f"Discord message sent ({len(flagged)} flagged)")


# ── Console ────────────────────────────────────────────────────────────────────


def print_report(
    section_label: str,
    flagged: list[dict],
    top_performers: list[list[dict]],
    promotion_candidates: list[dict],
    worst_performers: list[dict],
    worst_actual_days: int = WORST_PERFORMERS_DAYS,
    promo_actual_days: int = WORST_PERFORMERS_DAYS,
    clan_avg_fame: float = 0.0,
) -> None:
    print("\n".join(_build_lines(section_label, flagged, top_performers, promotion_candidates, worst_performers, worst_actual_days, promo_actual_days, clan_avg_fame)))
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
    parser.add_argument(
        "--worst-days",
        type=int,
        default=WORST_PERFORMERS_DAYS,
        metavar="N",
        help=f"Rolling war day window for worst performers (default: {WORST_PERFORMERS_DAYS}).",
    )
    parser.add_argument(
        "--worst-show",
        type=int,
        default=WORST_PERFORMERS_SHOW,
        metavar="N",
        help=f"Number of worst performers to show (default: {WORST_PERFORMERS_SHOW}).",
    )
    parser.add_argument(
        "--best-days",
        type=int,
        default=BEST_PERFORMERS_DAYS,
        metavar="N",
        help=f"Rolling war day window for promotion candidates (default: {BEST_PERFORMERS_DAYS}).",
    )
    parser.add_argument(
        "--best-show",
        type=int,
        default=BEST_PERFORMERS_SHOW,
        metavar="N",
        help=f"Number of promotion candidates to show (default: {BEST_PERFORMERS_SHOW}).",
    )
    args = parser.parse_args()
    best_days = args.best_days

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

    members               = db.get_members(conn, period_index, period_type, section_index)
    trophies_by_tag       = {m["player_tag"].upper(): m["trophies"] for m in members}
    flagged               = find_non_participants(conn, period_index, period_type, section_index, snapshots, members)
    top_performers        = find_top_performers(snapshots, trophies_by_tag)
    worst_stats, worst_actual_days, clan_avg_fame = find_member_fame_stats(
        conn, period_index, period_type, section_index, num_days=args.worst_days, members=members
    )
    worst_performers = worst_stats[:args.worst_show]

    if best_days == args.worst_days:
        best_stats, promo_actual_days = list(reversed(worst_stats)), worst_actual_days
    else:
        full_best, promo_actual_days, _ = find_member_fame_stats(
            conn, period_index, period_type, section_index, num_days=best_days, members=members
        )
        best_stats = list(reversed(full_best))

    promotion_candidates = best_stats[:args.best_show]

    print_report(section_label, flagged, top_performers, promotion_candidates, worst_performers, worst_actual_days=worst_actual_days, promo_actual_days=promo_actual_days, clan_avg_fame=clan_avg_fame)

    if not args.dry_run:
        send_discord(section_label, flagged, top_performers, promotion_candidates, worst_performers, worst_actual_days=worst_actual_days, promo_actual_days=promo_actual_days, clan_avg_fame=clan_avg_fame)

    conn.close()


if __name__ == "__main__":
    main()
