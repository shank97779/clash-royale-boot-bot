#!/usr/bin/env python3
"""
ingest.py — Fetch river race + member data from the Clash Royale API,
save raw JSON to data/, and store snapshots in SQLite.

Run this on a cron every hour (or more frequently on war days).
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

import db

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
CLAN_TAG   = os.getenv("CLAN_TAG",     "#PJ8Q8P")
API_TOKEN  = os.getenv("CR_API_TOKEN", "")
DB_PATH    = os.getenv("DB_PATH",      db.DB_PATH)
DATA_DIR   = os.getenv("DATA_DIR",     "data")
LOG_DIR    = os.getenv("LOG_DIR",      "logs")

CR_API_BASE = "https://api.clashroyale.com/v1"


# ── File helpers ───────────────────────────────────────────────────────────────

def timestamp_key(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")


def run_log_path(ts: datetime) -> Path:
    return Path(LOG_DIR) / "ingest" / f"{timestamp_key(ts)}_ingest.log"


def log(msg: str) -> None:
    line = msg.rstrip()
    print(line)
    if not hasattr(log, "_path"):
        log._path = run_log_path(datetime.now(timezone.utc))
    log_path = log._path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


# ── API ────────────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"Authorization": f"Bearer {API_TOKEN}"}


def fetch_river_race() -> dict:
    tag = CLAN_TAG.replace("#", "%23")
    resp = requests.get(
        f"{CR_API_BASE}/clans/{tag}/currentriverrace",
        headers=_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_members() -> list:
    tag = CLAN_TAG.replace("#", "%23")
    resp = requests.get(
        f"{CR_API_BASE}/clans/{tag}/members",
        headers=_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("items", [])


def log_member_stats(members: list, participants: list, section_key: str) -> None:
    participants_by_tag = {
        participant["tag"]: participant
        for participant in participants
    }

    for member in sorted(members, key=lambda item: (item.get("name", "").lower(), item.get("tag", ""))):
        tag = member["tag"]
        participant = participants_by_tag.get(tag)

        if participant is None:
            log(
                "[member] "
                f"section={section_key} | "
                f"name={member['name']} | tag={tag} | role={member.get('role', 'member')} | "
                f"exp={member.get('expLevel', 0)} | trophies={member.get('trophies', 0)} | "
                "in_race=no | fame=na | repair=na | boat_attacks=na | decks_used=na | decks_today=na",
            )
            continue

        log(
            "[member] "
            f"section={section_key} | "
            f"name={member['name']} | tag={tag} | role={member.get('role', 'member')} | "
            f"exp={member.get('expLevel', 0)} | trophies={member.get('trophies', 0)} | "
            "in_race=yes | "
            f"fame={participant.get('fame', 0)} | repair={participant.get('repairPoints', 0)} | "
            f"boat_attacks={participant.get('boatAttacks', 0)} | decks_used={participant.get('decksUsed', 0)} | "
            f"decks_today={participant.get('decksUsedToday', 0)}",
        )


# ── Raw JSON archive ───────────────────────────────────────────────────────────

def save_archive(
    ts: datetime,
    period_index: int,
    period_type: str,
    section_index: int,
    race_data: dict,
    members: list,
) -> str:
    out_dir = Path(DATA_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{timestamp_key(ts)}.json"
    out_path = out_dir / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "capturedAt": ts.isoformat(),
                "sectionKey": db.section_key(period_index, period_type, section_index),
                "periodIndex": period_index,
                "periodType": period_type,
                "sectionIndex": section_index,
                "currentRiverRace": race_data,
                "members": members,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
        f.write("\n")
    return str(out_path)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    if not API_TOKEN:
        sys.exit("Error: CR_API_TOKEN is not set. Copy .env.example to .env.")

    now_utc = datetime.now(timezone.utc)
    log._path = run_log_path(now_utc)
    ts_str  = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    log(f"[{ts_str}] ingest | clan={CLAN_TAG}")

    race_data = fetch_river_race()
    members   = fetch_members()

    clan_info     = race_data.get("clan", {})
    participants  = clan_info.get("participants", [])
    period_index  = race_data.get("periodIndex", 0)
    period_type   = race_data.get("periodType", "unknown")
    section_index = race_data.get("sectionIndex", 0)
    clan_name     = clan_info.get("name", CLAN_TAG)

    log(
        f"[ingest] {clan_name} | members={len(members)} | "
        f"participants={len(participants)} | period_index={period_index} "
        f"period={period_type} section={section_index}",
    )

    conn = db.get_db(DB_PATH)
    db.init_db(conn)
    db.ensure_section(conn, now_utc, period_index, period_type, section_index)
    current_section_key = db.section_key(period_index, period_type, section_index)

    archive_path = save_archive(now_utc, period_index, period_type, section_index, race_data, members)
    log(f"[ingest] section={current_section_key}")
    log(f"[ingest] saved {archive_path}")
    log_member_stats(members, participants, current_section_key)

    db.store_race_snapshot(conn, now_utc, period_index, period_type, section_index, participants)
    db.store_member_snapshot(conn, now_utc, period_index, period_type, section_index, members)
    conn.close()

    log(f"[ingest] snapshots stored in {DB_PATH}")


if __name__ == "__main__":
    main()
