"""
db.py — SQLite schema and helper functions.

Runtime storage is keyed by the API section identity:
  (season_index, period_index, period_type, section_index)

`season_index` is a monotonically incrementing integer, starting at 1, that is
bumped automatically by ingest when period_index resets (i.e. a new Clash
Royale season begins and the API reuses small period_index values).

Tables:
  sections          — one row per observed API section, ordered by first_seen_at
  section_snapshots — one row per (section, player_tag) with raw pulled stats
  section_members   — clan roster for a section

Decks participation tracking
-----------------------------
`decksUsed` in the API is cumulative across the entire war weekend
(e.g. 4 → 8 → 12 → 16 across four war days).

Participation is evaluated in report.py by comparing a player's cumulative
`decks_used` against `required = owed_days * MIN_DECKS`, where owed_days =
completed war days minus any days the player wasn't yet in the clan.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import NamedTuple

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "boot-bot.db")

# Section types that count as war days (participation is evaluated on these).
# 'colosseum' is the final war weekend of a season — treated identically to 'warDay'.
WAR_DAY_TYPES = ("warDay", "colosseum")


class SectionKey(NamedTuple):
    season_index:  int
    period_index:  int
    period_type:   str
    section_index: int

    def __str__(self) -> str:
        return f"{self.season_index}:{self.period_index}:{self.period_type}:{self.section_index}"

    @classmethod
    def parse(cls, value: str) -> "SectionKey":
        parts = value.split(":", 3)
        if len(parts) != 4:
            raise ValueError("Section key must look like '<season_index>:<period_index>:<period_type>:<section_index>'")
        si, pi, pt, sci = parts
        return cls(int(si), int(pi), pt, int(sci))


def get_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sections (
            season_index        INTEGER NOT NULL DEFAULT 1,
            period_index  INTEGER NOT NULL,
            period_type   TEXT    NOT NULL,
            section_index INTEGER NOT NULL,
            sequence      INTEGER NOT NULL UNIQUE,
            first_seen_at TEXT    NOT NULL,
            PRIMARY KEY (season_index, period_index, period_type, section_index)
        );

        CREATE INDEX IF NOT EXISTS idx_sections_sequence ON sections(sequence);

        CREATE TABLE IF NOT EXISTS section_snapshots (
            season_index            INTEGER NOT NULL DEFAULT 1,
            period_index      INTEGER NOT NULL,
            period_type       TEXT    NOT NULL,
            section_index     INTEGER NOT NULL,
            player_tag        TEXT    NOT NULL,
            player_name       TEXT    NOT NULL,
            fame              INTEGER NOT NULL DEFAULT 0,
            repair_points     INTEGER NOT NULL DEFAULT 0,
            boat_attacks      INTEGER NOT NULL DEFAULT 0,
            decks_used        INTEGER NOT NULL DEFAULT 0,
            decks_used_today  INTEGER NOT NULL DEFAULT 0,
            fame_today        INTEGER DEFAULT NULL,
            pulled_at         TEXT    NOT NULL,
            PRIMARY KEY (season_index, period_index, period_type, section_index, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_section_snapshots_section
            ON section_snapshots(season_index, period_index, period_type, section_index);
        CREATE INDEX IF NOT EXISTS idx_section_snapshots_tag
            ON section_snapshots(player_tag);

        CREATE TABLE IF NOT EXISTS section_members (
            season_index        INTEGER NOT NULL DEFAULT 1,
            period_index  INTEGER NOT NULL,
            period_type   TEXT    NOT NULL,
            section_index INTEGER NOT NULL,
            player_tag    TEXT    NOT NULL,
            player_name   TEXT    NOT NULL,
            role          TEXT    NOT NULL DEFAULT 'member',
            exp_level     INTEGER NOT NULL DEFAULT 0,
            trophies      INTEGER NOT NULL DEFAULT 0,
            donations     INTEGER NOT NULL DEFAULT 0,
            last_seen     TEXT    NOT NULL DEFAULT '',
            pulled_at     TEXT    NOT NULL,
            PRIMARY KEY (season_index, period_index, period_type, section_index, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_section_members_section
            ON section_members(season_index, period_index, period_type, section_index);
        CREATE INDEX IF NOT EXISTS idx_section_members_tag
            ON section_members(player_tag);
        """
    )
    conn.commit()


def _prev_warday_fame(
    conn: sqlite3.Connection,
    current_sequence: int,
    player_tag: str,
) -> int | None:
    """
    Return the player's cumulative fame from the most recent warDay section
    strictly before current_sequence, provided it belongs to the same war
    weekend:
      - no non-warDay section between them (training/colosseum boundary), AND
      - same period_index (fame resets when a new war period begins).
    Returns None if no such contiguous same-period previous warDay exists.
    """
    current_period = conn.execute(
        "SELECT period_index, season_index FROM sections WHERE sequence = ?",
        (current_sequence,),
    ).fetchone()
    if current_period is None:
        return None

    prev = conn.execute(
        """
        SELECT s.sequence, s.period_index, s.season_index, ss.fame
        FROM sections s
        JOIN section_snapshots ss
          ON ss.season_index        = s.season_index
         AND ss.period_index  = s.period_index
         AND ss.period_type   = s.period_type
         AND ss.section_index = s.section_index
         AND ss.player_tag    = ?
        WHERE s.period_type IN ('warDay', 'colosseum') AND s.sequence < ?
        ORDER BY s.sequence DESC
        LIMIT 1
        """,
        (player_tag, current_sequence),
    ).fetchone()
    if prev is None:
        return None
    # Different season_index or war period → fame has reset
    if prev["season_index"] != current_period["season_index"] or prev["period_index"] != current_period["period_index"]:
        return None
    boundary = conn.execute(
        """
        SELECT 1 FROM sections
        WHERE period_type NOT IN ('warDay', 'colosseum')
          AND sequence > ? AND sequence < ?
        LIMIT 1
        """,
        (prev["sequence"], current_sequence),
    ).fetchone()
    return None if boundary else prev["fame"]


def section_key(season_index: int, period_index: int, period_type: str, section_index: int) -> str:
    """Compatibility shim — prefer SectionKey directly."""
    return str(SectionKey(season_index, period_index, period_type, section_index))


def parse_section_key(value: str) -> tuple[int, int, str, int]:
    """Compatibility shim — prefer SectionKey.parse() directly."""
    return SectionKey.parse(value)


def current_season(conn: sqlite3.Connection) -> int:
    """Return the highest season_index number recorded, or 1 if no data yet."""
    row = conn.execute("SELECT COALESCE(MAX(season_index), 1) AS s FROM sections").fetchone()
    return int(row["s"])


def resolve_season(conn: sqlite3.Connection, period_index: int) -> int:
    """
    Determine the correct season_index for an incoming period_index.
    If the new period_index is less than the current maximum (and data exists),
    the API has reset for a new season — bump the season counter.
    """
    cur = current_season(conn)
    row = conn.execute(
        "SELECT COALESCE(MAX(period_index), 0) AS m FROM sections WHERE season_index = ?",
        (cur,),
    ).fetchone()
    max_pi = int(row["m"])
    return cur + 1 if (period_index < max_pi and max_pi > 0) else cur


def ensure_section(
    conn: sqlite3.Connection,
    now_utc: datetime,
    section: SectionKey,
) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM sections
        WHERE season_index = ? AND period_index = ? AND period_type = ? AND section_index = ?
        LIMIT 1
        """,
        section,
    ).fetchone()
    if existing:
        return

    row = conn.execute("SELECT COALESCE(MAX(sequence), 0) AS max_sequence FROM sections").fetchone()
    next_sequence = int(row["max_sequence"] if row else 0) + 1
    conn.execute(
        """
        INSERT INTO sections
            (season_index, period_index, period_type, section_index, sequence, first_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (*section, next_sequence, now_utc.isoformat()),
    )


def latest_section(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT season_index, period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        ORDER BY sequence DESC
        LIMIT 1
        """
    ).fetchone()


def latest_completed_war_section(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """
    Return the section immediately before the current latest ONLY if it is a
    warDay section.  This ensures the daily cron report fires once on the day
    after a war day and is silent on all other days.
    """
    latest = latest_section(conn)
    if latest is None:
        return None

    prev = conn.execute(
        """
        SELECT season_index, period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        WHERE sequence < ?
        ORDER BY sequence DESC
        LIMIT 1
        """,
        (latest["sequence"],),
    ).fetchone()

    if prev is None or prev["period_type"] not in WAR_DAY_TYPES:
        return None
    return prev


def war_weekend_sections(
    conn: sqlite3.Connection,
    section: SectionKey,
) -> list[sqlite3.Row]:
    """
    Return all warDay sections in the same war weekend as the given section,
    in sequence order (oldest first), up to and including the given section.

    A war weekend is bounded by the nearest preceding non-warDay section
    (i.e. a training period). If there is no such boundary we include all
    warDay sections up to the given one.
    """
    target = conn.execute(
        "SELECT sequence FROM sections WHERE season_index=? AND period_index=? AND period_type=? AND section_index=?",
        section,
    ).fetchone()
    if target is None:
        return []

    # Find the sequence of the most recent non-warDay section before the target.
    boundary = conn.execute(
        """
        SELECT COALESCE(MAX(sequence), 0) AS seq
        FROM sections
        WHERE period_type NOT IN ('warDay', 'colosseum')
          AND sequence < ?
        """,
        (target["sequence"],),
    ).fetchone()
    boundary_seq = boundary["seq"] if boundary else 0

    return conn.execute(
        """
        SELECT season_index, period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        WHERE period_type IN ('warDay', 'colosseum')
          AND sequence >  ?
          AND sequence <= ?
        ORDER BY sequence ASC
        """,
        (boundary_seq, target["sequence"]),
    ).fetchall()


def days_excused(
    conn: sqlite3.Connection,
    player_tag: str,
    weekend_sequences: list[int],
) -> int:
    """
    Return the number of war day sections the player is excused from.

    A player is excused for all days up to and including their first day in
    the clan (their join day is a free pass). Days after that count toward
    the required deck total.

    If the player never appears in any of the weekend sections, all days are
    excused (e.g. they left mid-weekend).
    """
    if not weekend_sequences:
        return 0

    placeholders = ",".join("?" * len(weekend_sequences))

    # Find the sequence of the player's first appearance this weekend.
    first = conn.execute(
        f"""
        SELECT MIN(s.sequence) AS first_seq
        FROM sections s
        JOIN section_members sm
          ON sm.period_index  = s.period_index
         AND sm.period_type   = s.period_type
         AND sm.section_index = s.section_index
        WHERE s.sequence IN ({placeholders})
          AND sm.player_tag = ?
        """,
        (*weekend_sequences, player_tag),
    ).fetchone()

    first_seq = first["first_seq"] if first and first["first_seq"] is not None else None

    if first_seq is None:
        # Player never appeared — excuse all days.
        return len(weekend_sequences)

    if first_seq == weekend_sequences[0]:
        # Player was present from the start of the war weekend — not a new joiner.
        return 0

    # New joiner: excuse every day up to and including their first appearance.
    return sum(1 for s in weekend_sequences if s <= first_seq)


def store_race_snapshot(
    conn: sqlite3.Connection,
    pulled_at: datetime,
    section: SectionKey,
    participants: list,
) -> None:
    """Upsert participant stats for a section, computing fame_today at store time."""
    ensure_section(conn, pulled_at, section)

    current_seq = conn.execute(
        "SELECT sequence FROM sections WHERE season_index=? AND period_index=? AND period_type=? AND section_index=?",
        section,
    ).fetchone()["sequence"]

    for participant in participants:
        tag   = participant["tag"]
        fame  = participant.get("fame", 0)

        if section.period_type in WAR_DAY_TYPES:
            prev_fame  = _prev_warday_fame(conn, current_seq, tag)
            fame_today = max(0, fame - prev_fame) if prev_fame is not None else fame
        else:
            fame_today = None

        conn.execute(
            """
            INSERT INTO section_snapshots
                (season_index, period_index, period_type, section_index, player_tag, player_name,
                 fame, repair_points, boat_attacks, decks_used, decks_used_today,
                 fame_today, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(season_index, period_index, period_type, section_index, player_tag) DO UPDATE SET
                player_name      = excluded.player_name,
                fame             = excluded.fame,
                repair_points    = excluded.repair_points,
                boat_attacks     = excluded.boat_attacks,
                decks_used       = excluded.decks_used,
                decks_used_today = excluded.decks_used_today,
                fame_today       = excluded.fame_today,
                pulled_at        = excluded.pulled_at
            """,
            (
                *section,
                tag,
                participant["name"],
                fame,
                participant.get("repairPoints", 0),
                participant.get("boatAttacks", 0),
                participant.get("decksUsed", 0),
                participant.get("decksUsedToday", 0),
                fame_today,
                pulled_at.isoformat(),
            ),
        )
    conn.commit()


def store_member_snapshot(
    conn: sqlite3.Connection,
    pulled_at: datetime,
    section: SectionKey,
    members: list,
) -> None:
    """Replace the roster for the section with the latest raw member payload."""
    ensure_section(conn, pulled_at, section)
    conn.execute(
        """
        DELETE FROM section_members
        WHERE season_index = ? AND period_index = ? AND period_type = ? AND section_index = ?
        """,
        section,
    )
    for member in members:
        conn.execute(
            """
            INSERT INTO section_members
                (season_index, period_index, period_type, section_index, player_tag, player_name,
                 role, exp_level, trophies, donations, last_seen, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                *section,
                member["tag"],
                member["name"],
                member.get("role", "member"),
                member.get("expLevel", 0),
                member.get("trophies", 0),
                member.get("donations", 0),
                member.get("lastSeen", ""),
                pulled_at.isoformat(),
            ),
        )
    conn.commit()


def get_snapshot(conn: sqlite3.Connection, section: SectionKey) -> list:
    return conn.execute(
        """
        SELECT *
        FROM section_snapshots
        WHERE season_index = ? AND period_index = ? AND period_type = ? AND section_index = ?
        ORDER BY player_name, player_tag
        """,
        section,
    ).fetchall()


def get_members(conn: sqlite3.Connection, section: SectionKey) -> list:
    return conn.execute(
        """
        SELECT *
        FROM section_members
        WHERE season_index = ? AND period_index = ? AND period_type = ? AND section_index = ?
        ORDER BY player_name, player_tag
        """,
        section,
    ).fetchall()


