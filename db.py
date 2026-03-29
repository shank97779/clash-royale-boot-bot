"""
db.py — SQLite schema and helper functions.

Runtime storage is keyed by the API section identity:
  (period_index, period_type, section_index)

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

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "boot-bot.db")


def get_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sections (
            period_index  INTEGER NOT NULL,
            period_type   TEXT    NOT NULL,
            section_index INTEGER NOT NULL,
            sequence      INTEGER NOT NULL UNIQUE,
            first_seen_at TEXT    NOT NULL,
            PRIMARY KEY (period_index, period_type, section_index)
        );

        CREATE INDEX IF NOT EXISTS idx_sections_sequence ON sections(sequence);

        CREATE TABLE IF NOT EXISTS section_snapshots (
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
            pulled_at         TEXT    NOT NULL,
            PRIMARY KEY (period_index, period_type, section_index, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_section_snapshots_section
            ON section_snapshots(period_index, period_type, section_index);
        CREATE INDEX IF NOT EXISTS idx_section_snapshots_tag
            ON section_snapshots(player_tag);

        CREATE TABLE IF NOT EXISTS section_members (
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
            PRIMARY KEY (period_index, period_type, section_index, player_tag)
        );

        CREATE INDEX IF NOT EXISTS idx_section_members_section
            ON section_members(period_index, period_type, section_index);
        CREATE INDEX IF NOT EXISTS idx_section_members_tag
            ON section_members(player_tag);
        """
    )
    conn.commit()


def section_key(period_index: int, period_type: str, section_index: int) -> str:
    return f"{period_index}:{period_type}:{section_index}"


def parse_section_key(value: str) -> tuple[int, str, int]:
    parts = value.split(":", 2)
    if len(parts) != 3:
        raise ValueError("Section key must look like '<period_index>:<period_type>:<section_index>'")

    period_index_str, period_type, section_index_str = parts
    return int(period_index_str), period_type, int(section_index_str)


def ensure_section(
    conn: sqlite3.Connection,
    now_utc: datetime,
    period_index: int,
    period_type: str,
    section_index: int,
) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM sections
        WHERE period_index = ? AND period_type = ? AND section_index = ?
        LIMIT 1
        """,
        (period_index, period_type, section_index),
    ).fetchone()
    if existing:
        return

    row = conn.execute("SELECT COALESCE(MAX(sequence), 0) AS max_sequence FROM sections").fetchone()
    next_sequence = int(row["max_sequence"] if row else 0) + 1
    conn.execute(
        """
        INSERT INTO sections
            (period_index, period_type, section_index, sequence, first_seen_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (period_index, period_type, section_index, next_sequence, now_utc.isoformat()),
    )


def latest_section(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        ORDER BY sequence DESC
        LIMIT 1
        """
    ).fetchone()


def latest_completed_war_section(conn: sqlite3.Connection) -> sqlite3.Row | None:
    latest = latest_section(conn)
    if latest is None:
        return None

    return conn.execute(
        """
        SELECT period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        WHERE period_type = 'warDay'
          AND sequence < ?
        ORDER BY sequence DESC
        LIMIT 1
        """,
        (latest["sequence"],),
    ).fetchone()


def war_weekend_sections(
    conn: sqlite3.Connection,
    period_index: int,
    period_type: str,
    section_index: int,
) -> list[sqlite3.Row]:
    """
    Return all warDay sections in the same war weekend as the given section,
    in sequence order (oldest first), up to and including the given section.

    A war weekend is bounded by the nearest preceding non-warDay section
    (i.e. a training period). If there is no such boundary we include all
    warDay sections up to the given one.
    """
    target = conn.execute(
        "SELECT sequence FROM sections WHERE period_index=? AND period_type=? AND section_index=?",
        (period_index, period_type, section_index),
    ).fetchone()
    if target is None:
        return []

    # Find the sequence of the most recent non-warDay section before the target.
    boundary = conn.execute(
        """
        SELECT COALESCE(MAX(sequence), 0) AS seq
        FROM sections
        WHERE period_type != 'warDay'
          AND sequence < ?
        """,
        (target["sequence"],),
    ).fetchone()
    boundary_seq = boundary["seq"] if boundary else 0

    return conn.execute(
        """
        SELECT period_index, period_type, section_index, sequence, first_seen_at
        FROM sections
        WHERE period_type = 'warDay'
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
    period_index: int,
    period_type: str,
    section_index: int,
    participants: list,
) -> None:
    """Upsert participant stats for a section."""
    ensure_section(conn, pulled_at, period_index, period_type, section_index)

    for participant in participants:
        conn.execute(
            """
            INSERT INTO section_snapshots
                (period_index, period_type, section_index, player_tag, player_name,
                 fame, repair_points, boat_attacks, decks_used, decks_used_today, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_index, period_type, section_index, player_tag) DO UPDATE SET
                player_name      = excluded.player_name,
                fame             = excluded.fame,
                repair_points    = excluded.repair_points,
                boat_attacks     = excluded.boat_attacks,
                decks_used       = excluded.decks_used,
                decks_used_today = excluded.decks_used_today,
                pulled_at        = excluded.pulled_at
            """,
            (
                period_index,
                period_type,
                section_index,
                participant["tag"],
                participant["name"],
                participant.get("fame", 0),
                participant.get("repairPoints", 0),
                participant.get("boatAttacks", 0),
                participant.get("decksUsed", 0),
                participant.get("decksUsedToday", 0),
                pulled_at.isoformat(),
            ),
        )
    conn.commit()


def store_member_snapshot(
    conn: sqlite3.Connection,
    pulled_at: datetime,
    period_index: int,
    period_type: str,
    section_index: int,
    members: list,
) -> None:
    """Replace the roster for the section with the latest raw member payload."""
    ensure_section(conn, pulled_at, period_index, period_type, section_index)
    conn.execute(
        """
        DELETE FROM section_members
        WHERE period_index = ? AND period_type = ? AND section_index = ?
        """,
        (period_index, period_type, section_index),
    )
    for member in members:
        conn.execute(
            """
            INSERT INTO section_members
                (period_index, period_type, section_index, player_tag, player_name,
                 role, exp_level, trophies, donations, last_seen, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                period_index,
                period_type,
                section_index,
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


def get_snapshot(
    conn: sqlite3.Connection,
    period_index: int,
    period_type: str,
    section_index: int,
) -> list:
    return conn.execute(
        """
        SELECT *
        FROM section_snapshots
        WHERE period_index = ? AND period_type = ? AND section_index = ?
        ORDER BY player_name, player_tag
        """,
        (period_index, period_type, section_index),
    ).fetchall()


def get_members(
    conn: sqlite3.Connection,
    period_index: int,
    period_type: str,
    section_index: int,
) -> list:
    return conn.execute(
        """
        SELECT *
        FROM section_members
        WHERE period_index = ? AND period_type = ? AND section_index = ?
        ORDER BY player_name, player_tag
        """,
        (period_index, period_type, section_index),
    ).fetchall()


