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
The API field `decksUsedToday` is unreliable — it can be 0 in a snapshot even
when `decksUsed` proves the player has already battled in that section (e.g. if
they played before our first ingest run of a new section, or after whatever
reset the API applies to that field).

Instead we track participation via a delta:

    decks_used - decks_used_initial

`decks_used_initial` is the value of `decksUsed` from the very first snapshot
captured for a player in a given section. The upsert in store_race_snapshot
sets it on first insert and never overwrites it, so the delta accurately
reflects how many decks were played within this section regardless of when
`decksUsedToday` happened to be reset.
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
            -- decks_used_initial: value of decksUsed at section first-seen; used to compute
            -- the within-section delta (decks_used - decks_used_initial) because
            -- decksUsedToday can be 0 even when the player has already battled.
            decks_used_initial INTEGER NOT NULL DEFAULT 0,
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


def store_race_snapshot(
    conn: sqlite3.Connection,
    pulled_at: datetime,
    period_index: int,
    period_type: str,
    section_index: int,
    participants: list,
) -> None:
    """Upsert participant values for a section, preserving decks_used_initial from first insert."""
    ensure_section(conn, pulled_at, period_index, period_type, section_index)

    for participant in participants:
        conn.execute(
            """
            INSERT INTO section_snapshots
                (period_index, period_type, section_index, player_tag, player_name,
                 fame, repair_points, boat_attacks, decks_used, decks_used_initial,
                 decks_used_today, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_index, period_type, section_index, player_tag) DO UPDATE SET
                player_name      = excluded.player_name,
                fame             = excluded.fame,
                repair_points    = excluded.repair_points,
                boat_attacks     = excluded.boat_attacks,
                decks_used       = excluded.decks_used,
                decks_used_today = excluded.decks_used_today,
                pulled_at        = excluded.pulled_at
                -- decks_used_initial is intentionally NOT updated after first insert
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
                participant.get("decksUsed", 0),  # decks_used_initial — only used on first insert
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


def known_tags_before(
    conn: sqlite3.Connection,
    period_index: int,
    period_type: str,
    section_index: int,
) -> set[str]:
    current = conn.execute(
        """
        SELECT sequence
        FROM sections
        WHERE period_index = ? AND period_type = ? AND section_index = ?
        """,
        (period_index, period_type, section_index),
    ).fetchone()
    if current is None:
        return set()

    rows = conn.execute(
        """
        SELECT DISTINCT ss.player_tag
        FROM section_snapshots ss
        JOIN sections s
          ON s.period_index = ss.period_index
         AND s.period_type = ss.period_type
         AND s.section_index = ss.section_index
        WHERE s.sequence < ?
        """,
        (current["sequence"],),
    ).fetchall()
    return {row["player_tag"] for row in rows}


