"""Unit tests for boot-bot."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import db
import ingest
from report import EXEMPT_TAGS, MIN_DECKS_PER_DAY, find_non_participants, find_top_performers


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_db(conn)
    return conn


def store_section(
    conn,
    *,
    seen_at: datetime,
    season_index: int = 1,
    period_index: int,
    period_type: str,
    section_index: int,
    participants: list | None = None,
    members: list | None = None,
) -> None:
    db.store_race_snapshot(
        conn,
        seen_at,
        season_index,
        period_index,
        period_type,
        section_index,
        participants or [],
    )
    db.store_member_snapshot(
        conn,
        seen_at,
        season_index,
        period_index,
        period_type,
        section_index,
        members or [],
    )


class TestSectionKeys:
    def test_section_key_round_trip(self):
        key = db.section_key(1, 25, "warDay", 3)
        assert key == "1:25:warDay:3"
        assert db.parse_section_key(key) == (1, 25, "warDay", 3)


class TestInitDb:
    def test_creates_section_tables(self):
        conn = make_conn()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "sections" in tables
        assert "section_snapshots" in tables
        assert "section_members" in tables


class TestSections:
    def test_ensure_section_assigns_sequence_once(self):
        conn = make_conn()
        now = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)

        db.ensure_section(conn, now, 1, 25, "warDay", 0)
        db.ensure_section(conn, now, 1, 25, "warDay", 0)
        db.ensure_section(conn, now, 1, 25, "warDay", 1)

        rows = conn.execute(
            "SELECT period_index, period_type, section_index, sequence FROM sections ORDER BY sequence"
        ).fetchall()
        assert [(r["period_index"], r["period_type"], r["section_index"], r["sequence"]) for r in rows] == [
            (25, "warDay", 0, 1),
            (25, "warDay", 1, 2),
        ]

    def test_latest_completed_war_section_excludes_current_section(self):
        conn = make_conn()
        db.ensure_section(conn, datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc), 1, 25, "warDay", 0)
        db.ensure_section(conn, datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc), 1, 25, "warDay", 1)
        db.ensure_section(conn, datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc), 1, 25, "warDay", 2)

        latest = db.latest_completed_war_section(conn)
        assert db.section_key(latest["season_index"], latest["period_index"], latest["period_type"], latest["section_index"]) == "1:25:warDay:1"

    def test_latest_completed_war_section_allows_training_as_current(self):
        conn = make_conn()
        db.ensure_section(conn, datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc), 1, 25, "warDay", 0)
        db.ensure_section(conn, datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc), 1, 25, "warDay", 1)
        db.ensure_section(conn, datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc), 1, 25, "training", 0)

        latest = db.latest_completed_war_section(conn)
        assert db.section_key(latest["season_index"], latest["period_index"], latest["period_type"], latest["section_index"]) == "1:25:warDay:1"


class TestStoreRaceSnapshot:
    def test_inserts_participants(self):
        conn = make_conn()
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[
                {"tag": "#AAA", "name": "Alice", "decksUsed": 4, "decksUsedToday": 4, "fame": 200},
            ],
        )

        rows = conn.execute("SELECT * FROM section_snapshots").fetchall()
        assert len(rows) == 1
        assert rows[0]["player_tag"] == "#AAA"
        assert rows[0]["decks_used_today"] == 4

    def test_replaces_section_with_latest_raw_values(self):
        conn = make_conn()
        first_seen = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)
        second_seen = datetime(2026, 3, 15, 11, 0, tzinfo=timezone.utc)

        db.store_race_snapshot(
            conn,
            first_seen,
            1,
            25,
            "warDay",
            0,
            [{"tag": "#AAA", "name": "Alice", "decksUsed": 4, "decksUsedToday": 4, "fame": 200}],
        )
        db.store_race_snapshot(
            conn,
            second_seen,
            1,
            25,
            "warDay",
            0,
            [{"tag": "#AAA", "name": "Alice", "decksUsed": 2, "decksUsedToday": 0, "fame": 100}],
        )

        row = conn.execute("SELECT * FROM section_snapshots WHERE player_tag = '#AAA'").fetchone()
        assert row["decks_used"] == 2
        assert row["decks_used_today"] == 0
        assert row["fame"] == 100
        assert row["pulled_at"] == second_seen.isoformat()


class TestStoreMemberSnapshot:
    def test_replaces_members_for_section(self):
        conn = make_conn()
        seen_at = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)

        db.store_member_snapshot(
            conn,
            seen_at,
            1,
            25,
            "warDay",
            0,
            [{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )
        db.store_member_snapshot(
            conn,
            seen_at,
            1,
            25,
            "warDay",
            0,
            [{"tag": "#BBB", "name": "Bob", "role": "elder"}],
        )

        rows = conn.execute("SELECT player_tag FROM section_members").fetchall()
        assert {row["player_tag"] for row in rows} == {"#BBB"}


class TestIngestFiles:
    def test_run_log_path_uses_timestamped_filename(self):
        ts = datetime(2026, 3, 27, 10, 5, tzinfo=timezone.utc)

        original_log_dir = ingest.LOG_DIR
        try:
            ingest.LOG_DIR = "logs"
            assert ingest.run_log_path(ts) == Path("logs/ingest/20260327T100500Z_ingest.log")
        finally:
            ingest.LOG_DIR = original_log_dir

    def test_save_archive_writes_one_timestamped_json(self, tmp_path):
        ts = datetime(2026, 3, 27, 10, 5, tzinfo=timezone.utc)

        original_data_dir = ingest.DATA_DIR
        try:
            ingest.DATA_DIR = str(tmp_path)
            archive_path = ingest.save_archive(
                ts,
                1,
                25,
                "warDay",
                3,
                {"periodIndex": 25, "periodType": "warDay", "sectionIndex": 3},
                [{"tag": "#AAA", "name": "Alice"}],
            )
        finally:
            ingest.DATA_DIR = original_data_dir

        assert Path(archive_path) == tmp_path / "20260327T100500Z.json"

        payload = json.loads(Path(archive_path).read_text(encoding="utf-8"))
        assert payload["capturedAt"] == ts.isoformat()
        assert payload["sectionKey"] == "1:25:warDay:3"
        assert payload["seasonIndex"] == 1
        assert payload["periodIndex"] == 25
        assert payload["members"][0]["tag"] == "#AAA"

    def test_log_member_stats_writes_grep_friendly_lines(self, tmp_path):
        log_path = tmp_path / "20260327T151500Z_ingest.log"
        had_path = hasattr(ingest.log, "_path")
        original_path = getattr(ingest.log, "_path", None)
        ingest.log._path = log_path

        try:
            ingest.log_member_stats(
                [
                    {"tag": "#AAA", "name": "Alice", "role": "elder", "expLevel": 14, "trophies": 7000},
                    {"tag": "#BBB", "name": "Bob", "role": "member", "expLevel": 13, "trophies": 6500},
                ],
                [
                    {
                        "tag": "#AAA",
                        "name": "Alice",
                        "fame": 400,
                        "repairPoints": 0,
                        "boatAttacks": 1,
                        "decksUsed": 4,
                        "decksUsedToday": 2,
                    },
                ],
                "25:warDay:3",
            )
        finally:
            if had_path:
                ingest.log._path = original_path
            else:
                del ingest.log._path

        lines = log_path.read_text(encoding="utf-8").splitlines()
        assert lines == [
            "[member] section=25:warDay:3 | name=Alice | tag=#AAA | role=elder | exp=14 | trophies=7000 | in_race=yes | fame=400 | repair=0 | boat_attacks=1 | decks_used=4 | decks_today=2",
            "[member] section=25:warDay:3 | name=Bob | tag=#BBB | role=member | exp=13 | trophies=6500 | in_race=no | fame=na | repair=na | boat_attacks=na | decks_used=na | decks_today=na",
        ]


SECTION = (1, 25, "warDay", 1)
PRIOR_SECTION = (1, 25, "warDay", 0)


class TestFindNonParticipants:
    def test_empty_when_no_snapshot(self):
        conn = make_conn()
        snapshots = db.get_snapshot(conn, *SECTION)
        members = db.get_members(conn, *SECTION)
        assert find_non_participants(conn, *SECTION, snapshots, members) == []

    def test_empty_for_training_section(self):
        conn = make_conn()
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="training",
            section_index=0,
            participants=[{"tag": "#AAA", "name": "Alice", "decksUsedToday": 0}],
            members=[{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )
        snapshots = db.get_snapshot(conn, 1, 25, "training", 0)
        members_rows = db.get_members(conn, 1, 25, "training", 0)
        assert find_non_participants(conn, 1, 25, "training", 0, snapshots, members_rows) == []

    def test_grace_period_skips_new_member(self):
        conn = make_conn()
        # A prior war day exists with a different member — NewGuy wasn't in the clan yet.
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[{"tag": "#OTHER", "name": "Other", "decksUsedToday": 4, "decksUsed": 4}],
            members=[{"tag": "#OTHER", "name": "Other", "role": "member"}],
        )
        # NewGuy joins on day 2 (their first and join day) — both days are excused.
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=1,
            participants=[{"tag": "#NEW", "name": "NewGuy", "decksUsedToday": 0, "decksUsed": 0}],
            members=[
                {"tag": "#OTHER", "name": "Other", "role": "member"},
                {"tag": "#NEW", "name": "NewGuy", "role": "member"},
            ],
        )
        # NewGuy owes 0 days (both days excused: day 1 absent + day 2 is join day).
        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        result = find_non_participants(conn, *SECTION, snapshots, members_rows)
        new_guy = [r for r in result if r["tag"] == "#NEW"]
        assert new_guy == []

    def test_flags_low_participation(self):
        conn = make_conn()
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[{"tag": "#AAA", "name": "Alice", "decksUsedToday": 4, "decksUsed": 4}],
            members=[{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=1,
            participants=[{"tag": "#AAA", "name": "Alice", "decksUsedToday": 2, "decksUsed": 2, "fame": 200}],
            members=[{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )

        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        result = find_non_participants(conn, *SECTION, snapshots, members_rows)
        assert len(result) == 1
        assert result[0]["tag"] == "#AAA"
        assert result[0]["decks_today"] == 2

    def test_missing_from_race_is_flagged(self):
        conn = make_conn()
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[{"tag": "#ABSENT", "name": "Absent", "decksUsedToday": 4, "decksUsed": 4}],
            members=[{"tag": "#ABSENT", "name": "Absent", "role": "member"}],
        )
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=1,
            participants=[{"tag": "#OTHER", "name": "Other", "decksUsedToday": 4, "decksUsed": 4}],
            members=[{"tag": "#ABSENT", "name": "Absent", "role": "member"}],
        )

        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        result = find_non_participants(conn, *SECTION, snapshots, members_rows)
        assert result[0]["tag"] == "#ABSENT"
        assert result[0]["decks_today"] == 0

    def test_exempt_tag_is_skipped(self):
        conn = make_conn()
        original = EXEMPT_TAGS.copy()
        EXEMPT_TAGS.add("#EXEMPT")
        try:
            store_section(
                conn,
                seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
                period_index=25,
                period_type="warDay",
                section_index=0,
                participants=[{"tag": "#EXEMPT", "name": "Owner", "decksUsedToday": 4, "decksUsed": 4}],
                members=[{"tag": "#EXEMPT", "name": "Owner", "role": "member"}],
            )
            store_section(
                conn,
                seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
                period_index=25,
                period_type="warDay",
                section_index=1,
                participants=[{"tag": "#EXEMPT", "name": "Owner", "decksUsedToday": 0, "decksUsed": 0}],
                members=[{"tag": "#EXEMPT", "name": "Owner", "role": "member"}],
            )
            snapshots = db.get_snapshot(conn, *SECTION)
            members_rows = db.get_members(conn, *SECTION)
            assert find_non_participants(conn, *SECTION, snapshots, members_rows) == []
        finally:
            EXEMPT_TAGS.clear()
            EXEMPT_TAGS.update(original)

    def test_removed_member_not_flagged(self):
        conn = make_conn()
        # Day 1: both Alice and Bob are in the clan, both participate.
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[
                {"tag": "#AAA", "name": "Alice", "decksUsedToday": 4, "decksUsed": 4},
                {"tag": "#BOB", "name": "Bob",   "decksUsedToday": 4, "decksUsed": 4},
            ],
            members=[
                {"tag": "#AAA", "name": "Alice", "role": "member"},
                {"tag": "#BOB", "name": "Bob",   "role": "member"},
            ],
        )
        # Day 2: Bob has been removed from the clan. Alice used 0 decks.
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=1,
            participants=[
                {"tag": "#AAA", "name": "Alice", "decksUsedToday": 0, "decksUsed": 4, "fame": 100},
            ],
            members=[
                {"tag": "#AAA", "name": "Alice", "role": "member"},
                # Bob is not in the current roster
            ],
        )
        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        result = find_non_participants(conn, *SECTION, snapshots, members_rows)
        tags = [r["tag"] for r in result]
        assert "#BOB" not in tags  # already removed, not our problem
        assert "#AAA" in tags      # Alice is still in clan and underperformed

    def test_sorted_by_decks_then_fame(self):
        conn = make_conn()
        for seen_at, section_index in [
            (datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc), 0),
            (datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc), 1),
        ]:
            participants = []
            members = []
            for tag, name, decks, fame in [
                ("#A", "Alice", 2, 300),
                ("#B", "Bob", 0, 100),
                ("#C", "Carol", 0, 50),
                ("#D", "Dave", 1, 200),
            ]:
                participants.append({"tag": tag, "name": name, "decksUsedToday": 4 if section_index == 0 else decks, "decksUsed": decks, "fame": fame})
                members.append({"tag": tag, "name": name, "role": "member"})
            store_section(
                conn,
                seen_at=seen_at,
                period_index=25,
                period_type="warDay",
                section_index=section_index,
                participants=participants,
                members=members,
            )

        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        result = find_non_participants(conn, *SECTION, snapshots, members_rows)
        assert [row["tag"] for row in result] == ["#C", "#B", "#D", "#A"]

    def test_threshold_boundary(self):
        conn = make_conn()
        store_section(
            conn,
            seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=0,
            participants=[{"tag": "#AAA", "name": "Alice", "decksUsedToday": 4, "decksUsed": 4}],
            members=[{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )
        store_section(
            conn,
            seen_at=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            period_index=25,
            period_type="warDay",
            section_index=1,
            participants=[{"tag": "#AAA", "name": "Alice", "decksUsedToday": MIN_DECKS_PER_DAY, "decksUsed": 2 * MIN_DECKS_PER_DAY}],
            members=[{"tag": "#AAA", "name": "Alice", "role": "member"}],
        )
        snapshots = db.get_snapshot(conn, *SECTION)
        members_rows = db.get_members(conn, *SECTION)
        assert find_non_participants(conn, *SECTION, snapshots, members_rows) == []


class TestWarWeekendSections:
    def test_returns_all_war_days_in_weekend(self):
        conn = make_conn()
        for i, seen_at in enumerate([
            datetime(2026, 3, 26, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 28, 10, 0, tzinfo=timezone.utc),
        ]):
            store_section(conn, seen_at=seen_at, period_index=25 + i, period_type="warDay", section_index=3)

        rows = db.war_weekend_sections(conn, 1, 27, "warDay", 3)
        assert [(r["period_index"], r["period_type"]) for r in rows] == [
            (25, "warDay"), (26, "warDay"), (27, "warDay")
        ]

    def test_stops_at_training_boundary(self):
        conn = make_conn()
        # previous war weekend
        store_section(conn, seen_at=datetime(2026, 3, 19, 10, 0, tzinfo=timezone.utc),
                      period_index=21, period_type="warDay", section_index=3)
        # training section between weekends
        store_section(conn, seen_at=datetime(2026, 3, 23, 10, 0, tzinfo=timezone.utc),
                      period_index=24, period_type="training", section_index=3)
        # current war weekend
        store_section(conn, seen_at=datetime(2026, 3, 26, 10, 0, tzinfo=timezone.utc),
                      period_index=25, period_type="warDay", section_index=3)
        store_section(conn, seen_at=datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc),
                      period_index=26, period_type="warDay", section_index=3)

        rows = db.war_weekend_sections(conn, 1, 26, "warDay", 3)
        assert len(rows) == 2
        assert rows[0]["period_index"] == 25
        assert rows[1]["period_index"] == 26

    def test_returns_empty_for_unknown_section(self):
        conn = make_conn()
        assert db.war_weekend_sections(conn, 1, 99, "warDay", 3) == []


class TestDaysExcused:
    def _make_weekend(self, conn):
        """Store two war days and return their sequences."""
        store_section(conn, seen_at=datetime(2026, 3, 26, 10, 0, tzinfo=timezone.utc),
                      period_index=25, period_type="warDay", section_index=3,
                      members=[{"tag": "#VET", "name": "Vet", "role": "member"}])
        store_section(conn, seen_at=datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc),
                      period_index=26, period_type="warDay", section_index=3,
                      members=[
                          {"tag": "#VET",  "name": "Vet",    "role": "member"},
                          {"tag": "#NEW",  "name": "NewGuy", "role": "member"},
                      ])
        rows = db.war_weekend_sections(conn, 1, 26, "warDay", 3)
        return [r["sequence"] for r in rows]

    def test_veteran_has_zero_excused(self):
        conn = make_conn()
        seqs = self._make_weekend(conn)
        assert db.days_excused(conn, "#VET", seqs) == 0

    def test_joiner_on_day2_excused_both_days(self):
        conn = make_conn()
        seqs = self._make_weekend(conn)
        # #NEW first appears day 2 → days 1 and 2 both excused
        assert db.days_excused(conn, "#NEW", seqs) == 2

    def test_player_never_in_roster_excused_all(self):
        conn = make_conn()
        seqs = self._make_weekend(conn)
        assert db.days_excused(conn, "#GHOST", seqs) == 2

    def test_empty_sequences_returns_zero(self):
        conn = make_conn()
        assert db.days_excused(conn, "#ANY", []) == 0


class TestFindTopPerformers:
    def test_top_three_tiers(self):
        snaps = [
            {"player_name": "Alice", "player_tag": "#A", "fame": 1800},
            {"player_name": "Bob",   "player_tag": "#B", "fame": 1600},
            {"player_name": "Carol", "player_tag": "#C", "fame": 1400},
            {"player_name": "Dave",  "player_tag": "#D", "fame": 1200},
        ]
        tiers = find_top_performers(snaps)
        assert len(tiers) == 3
        assert tiers[0][0]["name"] == "Alice"
        assert tiers[1][0]["name"] == "Bob"
        assert tiers[2][0]["name"] == "Carol"

    def test_tied_players_share_tier(self):
        snaps = [
            {"player_name": "Alice", "player_tag": "#A", "fame": 1800},
            {"player_name": "Bob",   "player_tag": "#B", "fame": 1600},
            {"player_name": "Carol", "player_tag": "#C", "fame": 1600},
        ]
        tiers = find_top_performers(snaps)
        assert len(tiers) == 2
        assert len(tiers[1]) == 2
        tier2_names = {p["name"] for p in tiers[1]}
        assert tier2_names == {"Bob", "Carol"}

    def test_zero_fame_excluded(self):
        snaps = [
            {"player_name": "Alice", "player_tag": "#A", "fame": 0},
            {"player_name": "Bob",   "player_tag": "#B", "fame": 0},
        ]
        assert find_top_performers(snaps) == []

    def test_empty_snapshots(self):
        assert find_top_performers([]) == []
