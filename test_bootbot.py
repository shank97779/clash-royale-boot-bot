#!/usr/bin/env python3
"""
Test suite for bootbot.py
Run with:  python -m unittest test_bootbot -v
"""

import sqlite3
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import bootbot


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    bootbot.init_db(conn)
    return conn


def ins_snap(conn, snap_date, period_type, section_index, tag, name,
             decks_used, decks_used_today=0, fame=0, boat_attacks=0,
             period_index=0):
    conn.execute(
        """INSERT INTO snapshots
               (snapshot_date, period_type, section_index, period_index,
                player_tag, player_name, fame, repair_points,
                boat_attacks, decks_used, decks_used_today)
           VALUES (?,?,?,?,?,?,?,0,?,?,?)""",
        (snap_date, period_type, section_index, period_index,
         tag, name, fame, boat_attacks, decks_used, decks_used_today),
    )
    conn.commit()


def ins_member(conn, snap_date, tag, name, role="member", last_seen=""):
    conn.execute(
        """INSERT INTO member_snapshots
               (snapshot_date, player_tag, player_name, role,
                exp_level, trophies, donations, last_seen)
           VALUES (?,?,?,?,0,0,0,?)""",
        (snap_date, tag, name, role, last_seen),
    )
    conn.commit()


def participant(tag, name, decks_used=0, fame=0, boat_attacks=0):
    return {
        "tag": tag, "name": name,
        "decksUsed": decks_used, "fame": fame,
        "boatAttacks": boat_attacks, "repairPoints": 0,
    }


def member(tag, name, role="member", last_seen=""):
    return {"tag": tag, "name": name, "role": role, "lastSeen": last_seen}


# ── clash_day_from_utc ────────────────────────────────────────────────────────

class TestClashDayFromUtc(unittest.TestCase):

    def _utc(self, y, mo, d, h, mi=0):
        return datetime(y, mo, d, h, mi, 0, tzinfo=timezone.utc)

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 10)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 0)
    def test_one_minute_before_reset_is_previous_day(self):
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 9, 59)),
            "2026-03-14",
        )

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 10)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 0)
    def test_exactly_at_reset_is_current_day(self):
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 10, 0)),
            "2026-03-15",
        )

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 10)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 0)
    def test_after_reset_is_current_day(self):
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 18, 0)),
            "2026-03-15",
        )

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 8)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 30)
    def test_custom_reset_time_before(self):
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 8, 29)),
            "2026-03-14",
        )

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 8)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 30)
    def test_custom_reset_time_at(self):
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 8, 30)),
            "2026-03-15",
        )

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 10)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 0)
    def test_midnight_utc_is_previous_day(self):
        # midnight UTC is 10 hours before reset → previous clash day
        self.assertEqual(
            bootbot.clash_day_from_utc(self._utc(2026, 3, 15, 0, 0)),
            "2026-03-14",
        )


# ── previous_clash_day ────────────────────────────────────────────────────────

class TestPreviousClashDay(unittest.TestCase):

    def test_normal(self):
        self.assertEqual(bootbot.previous_clash_day("2026-03-15"), "2026-03-14")

    def test_cross_month(self):
        self.assertEqual(bootbot.previous_clash_day("2026-03-01"), "2026-02-28")

    def test_cross_year(self):
        self.assertEqual(bootbot.previous_clash_day("2026-01-01"), "2025-12-31")

    def test_leap_year(self):
        self.assertEqual(bootbot.previous_clash_day("2024-03-01"), "2024-02-29")


# ── reset_datetime_utc ────────────────────────────────────────────────────────

class TestResetDatetimeUtc(unittest.TestCase):

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 10)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 0)
    def test_preserves_date_sets_time(self):
        now = datetime(2026, 3, 15, 14, 30, 55, tzinfo=timezone.utc)
        reset = bootbot.reset_datetime_utc(now)
        self.assertEqual(reset.date(), now.date())
        self.assertEqual(reset.hour, 10)
        self.assertEqual(reset.minute, 0)
        self.assertEqual(reset.second, 0)
        self.assertEqual(reset.microsecond, 0)

    @patch.object(bootbot, 'CLASH_RESET_UTC_HOUR', 8)
    @patch.object(bootbot, 'CLASH_RESET_UTC_MINUTE', 30)
    def test_custom_reset_time(self):
        now = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        reset = bootbot.reset_datetime_utc(now)
        self.assertEqual(reset.hour, 8)
        self.assertEqual(reset.minute, 30)


# ── snapshot_exists / report history ─────────────────────────────────────────

class TestSnapshotExists(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_false_when_empty(self):
        self.assertFalse(bootbot.snapshot_exists(self.conn, "2026-03-15"))

    def test_true_after_insert(self):
        ins_snap(self.conn, "2026-03-15", "warDay", 0, "#AAA", "Alice", 4)
        self.assertTrue(bootbot.snapshot_exists(self.conn, "2026-03-15"))

    def test_false_for_different_date(self):
        ins_snap(self.conn, "2026-03-15", "warDay", 0, "#AAA", "Alice", 4)
        self.assertFalse(bootbot.snapshot_exists(self.conn, "2026-03-16"))


class TestReportHistory(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_not_sent_initially(self):
        self.assertFalse(bootbot.report_was_sent(self.conn, "2026-03-15"))

    def test_mark_and_check(self):
        now = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        bootbot.mark_report_sent(self.conn, "2026-03-15", now)
        self.assertTrue(bootbot.report_was_sent(self.conn, "2026-03-15"))

    def test_different_date_not_marked(self):
        now = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        bootbot.mark_report_sent(self.conn, "2026-03-15", now)
        self.assertFalse(bootbot.report_was_sent(self.conn, "2026-03-16"))

    def test_mark_idempotent(self):
        now = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        bootbot.mark_report_sent(self.conn, "2026-03-15", now)
        bootbot.mark_report_sent(self.conn, "2026-03-15", now)  # should not raise
        self.assertTrue(bootbot.report_was_sent(self.conn, "2026-03-15"))


# ── prior_war_progress ────────────────────────────────────────────────────────

class TestPriorWarProgress(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_empty_db_returns_zeros(self):
        self.assertEqual(
            bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0),
            (0, 0),
        )

    def test_training_day_rows_not_counted(self):
        ins_snap(self.conn, "2026-03-19", "trainingDay", 0, "#AAA", "Alice", 8)
        self.assertEqual(
            bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0),
            (0, 0),
        )

    def test_different_section_not_counted(self):
        ins_snap(self.conn, "2026-03-19", "warDay", 1, "#AAA", "Alice", 8)
        self.assertEqual(
            bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0),
            (0, 0),
        )

    def test_todays_row_not_counted(self):
        ins_snap(self.conn, "2026-03-20", "warDay", 0, "#AAA", "Alice", 8)
        self.assertEqual(
            bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0),
            (0, 0),
        )

    def test_different_player_not_counted(self):
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#BBB", "Bob", 8)
        self.assertEqual(
            bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0),
            (0, 0),
        )

    def test_one_prior_war_day(self):
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#AAA", "Alice", 8)
        days, decks = bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0)
        self.assertEqual(days, 1)
        self.assertEqual(decks, 8)

    def test_two_prior_war_days_returns_max_decks(self):
        ins_snap(self.conn, "2026-03-18", "warDay", 0, "#AAA", "Alice", 4)
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#AAA", "Alice", 8)
        days, decks = bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0)
        self.assertEqual(days, 2)
        self.assertEqual(decks, 8)  # MAX

    def test_mixed_period_types_only_counts_war_days(self):
        ins_snap(self.conn, "2026-03-17", "trainingDay", 0, "#AAA", "Alice", 0)
        ins_snap(self.conn, "2026-03-18", "warDay",      0, "#AAA", "Alice", 4)
        ins_snap(self.conn, "2026-03-19", "warDay",      0, "#AAA", "Alice", 8)
        days, _ = bootbot.prior_war_progress(self.conn, "#AAA", "2026-03-20", 0)
        self.assertEqual(days, 2)


# ── derive_decks_used_today ───────────────────────────────────────────────────

class TestDeriveDeckUsedToday(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_training_day_always_zero(self):
        self.assertEqual(
            bootbot.derive_decks_used_today(self.conn, "2026-03-16", "trainingDay", 0, "#AAA", 4),
            0,
        )

    def test_war_day_no_prior_equals_cumulative(self):
        self.assertEqual(
            bootbot.derive_decks_used_today(self.conn, "2026-03-18", "warDay", 0, "#AAA", 4),
            4,
        )

    def test_war_day_with_prior_returns_delta(self):
        ins_snap(self.conn, "2026-03-18", "warDay", 0, "#AAA", "Alice", 8)
        result = bootbot.derive_decks_used_today(
            self.conn, "2026-03-19", "warDay", 0, "#AAA", 12
        )
        self.assertEqual(result, 4)  # 12 - 8

    def test_api_regression_clamped_to_zero(self):
        ins_snap(self.conn, "2026-03-18", "warDay", 0, "#AAA", "Alice", 8)
        result = bootbot.derive_decks_used_today(
            self.conn, "2026-03-19", "warDay", 0, "#AAA", 5  # lower than prior
        )
        self.assertEqual(result, 0)

    def test_zero_decks_with_zero_prior(self):
        ins_snap(self.conn, "2026-03-18", "warDay", 0, "#AAA", "Alice", 0)
        self.assertEqual(
            bootbot.derive_decks_used_today(self.conn, "2026-03-19", "warDay", 0, "#AAA", 0),
            0,
        )

    def test_section_scoped(self):
        # Prior row is in section 1; query is for section 2 → treated as no prior
        ins_snap(self.conn, "2026-03-18", "warDay", 1, "#AAA", "Alice", 8)
        result = bootbot.derive_decks_used_today(
            self.conn, "2026-03-19", "warDay", 2, "#AAA", 4
        )
        self.assertEqual(result, 4)  # no prior for section 2


# ── store_snapshot ────────────────────────────────────────────────────────────

class TestStoreSnapshot(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def _row(self, snap_date, tag):
        return self.conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_date=? AND player_tag=?",
            (snap_date, tag),
        ).fetchone()

    def test_first_insert_stores_all_fields(self):
        p = participant("#AAA", "Alice", decks_used=4, fame=800, boat_attacks=1)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p])
        row = self._row("2026-03-20", "#AAA")
        self.assertIsNotNone(row)
        self.assertEqual(row["fame"], 800)
        self.assertEqual(row["decks_used"], 4)
        self.assertEqual(row["boat_attacks"], 1)

    def test_war_day_no_prior_derives_full_decks_today(self):
        p = participant("#AAA", "Alice", decks_used=4)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p])
        self.assertEqual(self._row("2026-03-20", "#AAA")["decks_used_today"], 4)

    def test_war_day_with_prior_derives_delta(self):
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#AAA", "Alice", 8)
        p = participant("#AAA", "Alice", decks_used=12)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p])
        self.assertEqual(self._row("2026-03-20", "#AAA")["decks_used_today"], 4)  # 12-8

    def test_training_day_stores_zero_daily_decks(self):
        p = participant("#AAA", "Alice", decks_used=4)
        bootbot.store_snapshot(self.conn, "2026-03-16", "trainingDay", 0, 0, [p])
        self.assertEqual(self._row("2026-03-16", "#AAA")["decks_used_today"], 0)

    def test_hourly_rerun_keeps_max_values(self):
        p = participant("#AAA", "Alice", decks_used=4, fame=800)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p])
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p])
        row = self._row("2026-03-20", "#AAA")
        self.assertEqual(row["decks_used"], 4)
        self.assertEqual(row["fame"], 800)

    def test_api_regression_does_not_overwrite_higher_values(self):
        p_high = participant("#AAA", "Alice", decks_used=8, fame=1600)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p_high])
        p_low = participant("#AAA", "Alice", decks_used=4, fame=800)
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, [p_low])
        row = self._row("2026-03-20", "#AAA")
        self.assertEqual(row["decks_used"], 8)
        self.assertEqual(row["fame"], 1600)

    def test_multiple_participants(self):
        parts = [
            participant("#AAA", "Alice", decks_used=4),
            participant("#BBB", "Bob",   decks_used=8),
        ]
        bootbot.store_snapshot(self.conn, "2026-03-20", "warDay", 0, 0, parts)
        self.assertIsNotNone(self._row("2026-03-20", "#AAA"))
        self.assertIsNotNone(self._row("2026-03-20", "#BBB"))


# ── store_members_snapshot ────────────────────────────────────────────────────

class TestStoreMembersSnapshot(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def _count(self, snap_date):
        return self.conn.execute(
            "SELECT COUNT(*) FROM member_snapshots WHERE snapshot_date=?",
            (snap_date,),
        ).fetchone()[0]

    def _tags(self, snap_date):
        return {
            r["player_tag"] for r in self.conn.execute(
                "SELECT player_tag FROM member_snapshots WHERE snapshot_date=?",
                (snap_date,),
            ).fetchall()
        }

    def test_basic_insert(self):
        bootbot.store_members_snapshot(
            self.conn, "2026-03-20",
            [member("#AAA", "Alice"), member("#BBB", "Bob")],
        )
        self.assertEqual(self._count("2026-03-20"), 2)

    def test_second_run_replaces_not_appends(self):
        bootbot.store_members_snapshot(
            self.conn, "2026-03-20",
            [member("#AAA", "Alice"), member("#BBB", "Bob")],
        )
        # Bob left, Carol joined
        bootbot.store_members_snapshot(
            self.conn, "2026-03-20",
            [member("#AAA", "Alice"), member("#CCC", "Carol")],
        )
        tags = self._tags("2026-03-20")
        self.assertEqual(len(tags), 2)
        self.assertIn("#AAA", tags)
        self.assertIn("#CCC", tags)
        self.assertNotIn("#BBB", tags)

    def test_different_days_are_independent(self):
        bootbot.store_members_snapshot(self.conn, "2026-03-19", [member("#AAA", "Alice")])
        bootbot.store_members_snapshot(self.conn, "2026-03-20", [member("#BBB", "Bob")])
        self.assertEqual(self._count("2026-03-19"), 1)
        self.assertEqual(self._count("2026-03-20"), 1)


# ── get_prior_tags ────────────────────────────────────────────────────────────

class TestGetPriorTags(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_empty_db(self):
        self.assertEqual(bootbot.get_prior_tags(self.conn, "2026-03-20"), set())

    def test_only_today_returns_empty(self):
        ins_snap(self.conn, "2026-03-20", "warDay", 0, "#AAA", "Alice", 4)
        self.assertEqual(bootbot.get_prior_tags(self.conn, "2026-03-20"), set())

    def test_prior_day_included(self):
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#AAA", "Alice", 4)
        ins_snap(self.conn, "2026-03-20", "warDay", 0, "#BBB", "Bob",   4)
        prior = bootbot.get_prior_tags(self.conn, "2026-03-20")
        self.assertIn("#AAA", prior)
        self.assertNotIn("#BBB", prior)

    def test_training_day_snapshots_count(self):
        # get_prior_tags is snapshot-type-agnostic — covers training days too
        ins_snap(self.conn, "2026-03-19", "trainingDay", 0, "#AAA", "Alice", 0)
        self.assertIn("#AAA", bootbot.get_prior_tags(self.conn, "2026-03-20"))

    def test_deduplication(self):
        # Same tag in multiple prior days → appears once
        ins_snap(self.conn, "2026-03-18", "warDay", 0, "#AAA", "Alice", 4)
        ins_snap(self.conn, "2026-03-19", "warDay", 0, "#AAA", "Alice", 8)
        prior = bootbot.get_prior_tags(self.conn, "2026-03-20")
        self.assertEqual(prior.count("#AAA") if isinstance(prior, list) else 1, 1)


# ── _role_action ──────────────────────────────────────────────────────────────

class TestRoleAction(unittest.TestCase):

    def test_leader(self):
        _, action = bootbot._role_action("leader")
        self.assertEqual(action, "Flag for review")

    def test_co_leader(self):
        _, action = bootbot._role_action("coLeader")
        self.assertEqual(action, "Demote to Elder")

    def test_elder(self):
        _, action = bootbot._role_action("elder")
        self.assertEqual(action, "Demote to Member")

    def test_member(self):
        _, action = bootbot._role_action("member")
        self.assertEqual(action, "Boot")

    def test_unknown_defaults_to_boot(self):
        _, action = bootbot._role_action("unknown_role")
        self.assertEqual(action, "Boot")

    def test_each_role_has_emoji(self):
        for role in ("leader", "coLeader", "elder", "member"):
            emoji, _ = bootbot._role_action(role)
            self.assertTrue(emoji.startswith(":"), f"Expected emoji for {role}")


# ── find_boot_candidates ──────────────────────────────────────────────────────

class TestFindBootCandidates(unittest.TestCase):
    """Core flag/skip logic. Each test starts fresh with a simple fixture:
    - Alice (#ALICE) and Bob (#BOB) are established members (prior snapshot on 2026-03-20).
    - TODAY is 2026-03-21, section_index=2.
    """

    TODAY   = "2026-03-21"
    SECTION = 2

    def setUp(self):
        self.conn = make_db()
        self._patches = [
            patch.object(bootbot, 'EXEMPT_MEMBERS',        set()),
            patch.object(bootbot, 'MIN_DECKS_PER_DAY',     4),
            patch.object(bootbot, 'MIN_PARTICIPATION_PCT', 0.5),
        ]
        for p in self._patches:
            p.start()
        ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, "#ALICE", "Alice", 8)
        ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, "#BOB",   "Bob",   8)
        ins_member(self.conn, "2026-03-20", "#ALICE", "Alice")
        ins_member(self.conn, "2026-03-20", "#BOB",   "Bob")

    def tearDown(self):
        self.conn.close()
        for p in self._patches:
            p.stop()

    def _run(self, participants, members, prior_tags=None):
        if prior_tags is None:
            prior_tags = bootbot.get_prior_tags(self.conn, self.TODAY)
        return bootbot.find_boot_candidates(
            self.conn, self.TODAY, "warDay", self.SECTION,
            participants, members, prior_tags,
        )

    # ── Basic skips ────────────────────────────────────────────────────────────

    def test_training_day_returns_empty(self):
        result = bootbot.find_boot_candidates(
            self.conn, self.TODAY, "trainingDay", self.SECTION,
            [participant("#ALICE", "Alice", decks_used=8)],
            [member("#ALICE", "Alice")],
            {"#ALICE"},
        )
        self.assertEqual(result, [])

    def test_new_member_grace_skipped(self):
        # Carol has no prior snapshot
        result = self._run(
            [participant("#CAROL", "Carol", decks_used=4)],
            [member("#CAROL", "Carol")],
            prior_tags=set(),
        )
        self.assertEqual(result, [])

    def test_exempt_member_never_flagged(self):
        with patch.object(bootbot, 'EXEMPT_MEMBERS', {"#ALICE"}):
            result = bootbot.find_boot_candidates(
                self.conn, self.TODAY, "warDay", self.SECTION,
                [participant("#ALICE", "Alice", decks_used=8)],  # daily=0
                [member("#ALICE", "Alice")],
                {"#ALICE"},
            )
        self.assertEqual(result, [])

    # ── Flagging ───────────────────────────────────────────────────────────────

    def test_not_in_participants_flagged(self):
        result = self._run([], [member("#ALICE", "Alice")])
        self.assertEqual(len(result), 1)
        self.assertIn("not participating", result[0]["reasons"][0])

    def test_zero_decks_today_flagged(self):
        # Cumulative stays at 8 (same as yesterday's prior) → daily=0
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=8)],
            [member("#ALICE", "Alice")],
        )
        self.assertEqual(len(result), 1)
        self.assertIn("0 decks", result[0]["reasons"][0])

    def test_good_participation_not_flagged(self):
        # Cumulative 8→12 → daily=4 → fine
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=12, fame=2400)],
            [member("#ALICE", "Alice")],
        )
        self.assertEqual(result, [])

    def test_shashank_false_positive_is_fixed(self):
        """Cumulative 8→12 must NOT be flagged regardless of what the API
        would have returned for decksUsedToday (the original bug)."""
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=12, fame=2400)],
            [member("#ALICE", "Alice")],
        )
        self.assertEqual(result, [])

    def test_below_participation_pct_flagged(self):
        # 3 prior war days (insert two more earlier days with 0 decks)
        ins_snap(self.conn, "2026-03-18", "warDay", self.SECTION, "#ALICE", "Alice", 0)
        ins_snap(self.conn, "2026-03-19", "warDay", self.SECTION, "#ALICE", "Alice", 0)
        self.conn.execute(
            "UPDATE snapshots SET decks_used=0 WHERE player_tag='#ALICE' AND snapshot_date='2026-03-20'"
        )
        self.conn.commit()
        # Today she plays 4 decks (daily>0) but total=4 < (3*4)*0.5=6
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=4, fame=800)],
            [member("#ALICE", "Alice")],
        )
        reasons = " ".join(result[0]["reasons"])
        self.assertIn("expected decks used overall", reasons)

    def test_zero_decks_and_low_pct_gives_two_reasons(self):
        ins_snap(self.conn, "2026-03-18", "warDay", self.SECTION, "#ALICE", "Alice", 0)
        ins_snap(self.conn, "2026-03-19", "warDay", self.SECTION, "#ALICE", "Alice", 0)
        self.conn.execute(
            "UPDATE snapshots SET decks_used=0 WHERE player_tag='#ALICE' AND snapshot_date='2026-03-20'"
        )
        self.conn.commit()
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=0)],
            [member("#ALICE", "Alice")],
        )
        self.assertEqual(len(result[0]["reasons"]), 2)

    def test_first_war_day_no_pct_check(self):
        # Carl has only a prior training-day snapshot (prior_war_days=0)
        ins_snap(self.conn, "2026-03-20", "trainingDay", self.SECTION, "#CARL", "Carl", 0)
        prior = bootbot.get_prior_tags(self.conn, self.TODAY)
        # Carl plays 4 decks today → daily=4 → not flagged; pct check skipped (0 prior war days)
        result = self._run(
            [participant("#CARL", "Carl", decks_used=4, fame=800)],
            [member("#CARL", "Carl")],
            prior_tags=prior,
        )
        self.assertEqual(result, [])

    def test_role_preserved_in_candidate(self):
        result = self._run(
            [participant("#ALICE", "Alice", decks_used=8)],
            [member("#ALICE", "Alice", role="elder")],
        )
        self.assertEqual(result[0]["role"], "elder")

    # ── Sort order ─────────────────────────────────────────────────────────────

    def test_role_order_coLeader_before_elder_before_member(self):
        for tag, role in [("#CO", "coLeader"), ("#EL", "elder"), ("#MB", "member")]:
            ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, tag, tag, 4)
            ins_member(self.conn, "2026-03-20", tag, tag, role=role)
        prior = bootbot.get_prior_tags(self.conn, self.TODAY)
        parts   = [participant(t, t, decks_used=4) for t in ("#CO", "#EL", "#MB")]
        members_list = [member(t, t, role=r) for t, r in (("#CO","coLeader"),("#EL","elder"),("#MB","member"))]
        result = self._run(parts, members_list, prior_tags=prior)
        roles = [c["role"] for c in result]
        self.assertEqual(roles.index("coLeader"), 0)
        self.assertLess(roles.index("coLeader"), roles.index("elder"))
        self.assertLess(roles.index("elder"),    roles.index("member"))

    def test_within_role_newer_joiner_comes_first(self):
        for tag in ("#M1", "#M2"):
            ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, tag, tag, 4)
        # M1 joined later (2026-03-17), M2 joined earlier (2026-03-16)
        ins_member(self.conn, "2026-03-17", "#M1", "M1")
        ins_member(self.conn, "2026-03-16", "#M2", "M2")
        prior = bootbot.get_prior_tags(self.conn, self.TODAY)
        parts = [participant(t, t, decks_used=4) for t in ("#M1", "#M2")]
        members_list = [member(t, t) for t in ("#M1", "#M2")]
        result = self._run(parts, members_list, prior_tags=prior)
        self.assertEqual(result[0]["tag"], "#M1")  # newer first


# ── find_top_performers ───────────────────────────────────────────────────────

class TestFindTopPerformers(unittest.TestCase):

    def setUp(self):
        self._patch = patch.object(bootbot, 'TOP_PERFORMERS_N', 3)
        self._patch.start()

    def tearDown(self):
        self._patch.stop()

    def test_empty_returns_empty_tiers(self):
        self.assertEqual(bootbot.find_top_performers([], set())["tiers"], [])

    def test_non_active_excluded(self):
        parts = [participant("#AAA", "Alice", fame=2000)]
        self.assertEqual(bootbot.find_top_performers(parts, set())["tiers"], [])

    def test_single_player(self):
        result = bootbot.find_top_performers([participant("#AAA", "Alice", fame=2000)], {"#AAA"})
        self.assertEqual(len(result["tiers"]), 1)
        self.assertEqual(result["tiers"][0][0], 2000)

    def test_tied_fame_same_tier(self):
        parts = [
            participant("#AAA", "Alice", fame=2000),
            participant("#BBB", "Bob",   fame=2000),
        ]
        result = bootbot.find_top_performers(parts, {"#AAA", "#BBB"})
        self.assertEqual(len(result["tiers"]), 1)
        self.assertEqual(len(result["tiers"][0][1]), 2)

    def test_tiers_capped_at_top_n(self):
        parts = [participant(f"#{i}", f"P{i}", fame=2000 - i * 100) for i in range(5)]
        active = {f"#{i}" for i in range(5)}
        result = bootbot.find_top_performers(parts, active)
        self.assertEqual(len(result["tiers"]), 3)

    def test_ordered_by_fame_descending(self):
        parts = [
            participant("#A", "A", fame=1000),
            participant("#B", "B", fame=3000),
            participant("#C", "C", fame=2000),
        ]
        result = bootbot.find_top_performers(parts, {"#A", "#B", "#C"})
        fame_values = [tier[0] for tier in result["tiers"]]
        self.assertEqual(fame_values, [3000, 2000, 1000])

    def test_tiebreaker_fewer_decks_listed_first(self):
        parts = [
            participant("#EFF", "Efficient", fame=2000, decks_used=8),
            participant("#SPN", "Spender",   fame=2000, decks_used=12),
        ]
        result = bootbot.find_top_performers(parts, {"#EFF", "#SPN"})
        self.assertEqual(result["tiers"][0][1][0]["tag"], "#EFF")

    def test_zero_fame_players_still_ranked(self):
        parts = [participant("#AAA", "Alice", fame=0)]
        result = bootbot.find_top_performers(parts, {"#AAA"})
        self.assertEqual(len(result["tiers"]), 1)


# ── find_boat_offenders ───────────────────────────────────────────────────────

class TestFindBoatOffenders(unittest.TestCase):

    def test_no_offenders(self):
        parts = [participant("#AAA", "Alice", boat_attacks=0)]
        self.assertEqual(bootbot.find_boat_offenders(parts, {"#AAA"}), [])

    def test_non_active_excluded(self):
        parts = [participant("#AAA", "Alice", boat_attacks=3)]
        self.assertEqual(bootbot.find_boat_offenders(parts, set()), [])

    def test_sorted_descending(self):
        parts = [
            participant("#A", "A", boat_attacks=1),
            participant("#B", "B", boat_attacks=5),
            participant("#C", "C", boat_attacks=2),
        ]
        result = bootbot.find_boat_offenders(parts, {"#A", "#B", "#C"})
        self.assertEqual([r["tag"] for r in result], ["#B", "#C", "#A"])

    def test_zero_attacks_not_included(self):
        parts = [
            participant("#AAA", "Alice", boat_attacks=0),
            participant("#BBB", "Bob",   boat_attacks=3),
        ]
        result = bootbot.find_boat_offenders(parts, {"#AAA", "#BBB"})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tag"], "#BBB")


# ── build_report_from_snapshot (MIN_CLAN_SIZE guard) ─────────────────────────

class TestBuildReportMinClanSize(unittest.TestCase):
    """Verifies the headcount guard that protects members from boot when the
    clan would drop below MIN_CLAN_SIZE, while always allowing demotions."""

    TODAY   = "2026-03-21"
    SECTION = 0

    def setUp(self):
        self.conn = make_db()
        self._patches = [
            patch.object(bootbot, 'EXEMPT_MEMBERS',        set()),
            patch.object(bootbot, 'MIN_DECKS_PER_DAY',     4),
            patch.object(bootbot, 'MIN_PARTICIPATION_PCT', 0.5),
            patch.object(bootbot, 'REPORT_BOAT_ATTACKS',   False),
            patch.object(bootbot, 'TOP_PERFORMERS_N',      3),
        ]
        for p in self._patches:
            p.start()
        # 5 slackers + 3 good players, all established
        for tag in ["#S1","#S2","#S3","#S4","#S5","#G1","#G2","#G3"]:
            ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, tag, tag, 4)
            ins_member(self.conn, "2026-03-20", tag, tag)

    def tearDown(self):
        self.conn.close()
        for p in self._patches:
            p.stop()

    def _snapped(self, fame_map=None):
        """Slackers: decks same as prior (daily=0). Good: decks grew (daily=4)."""
        slackers = ["#S1","#S2","#S3","#S4","#S5"]
        good_players = ["#G1","#G2","#G3"]
        fm = fame_map or {}
        parts = (
            [participant(t, t, decks_used=4, fame=fm.get(t, 800)) for t in slackers] +
            [participant(t, t, decks_used=8, fame=1600) for t in good_players]
        )
        members_list = (
            [member(t, t) for t in slackers] +
            [member(t, t) for t in good_players]
        )
        return {
            "period_type":   "warDay",
            "section_index": self.SECTION,
            "period_index":  0,
            "participants":  parts,
            "members":       members_list,
        }

    def test_clan_at_min_size_all_members_protected(self):
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 8):
            report = bootbot.build_report_from_snapshot(self.conn, self.TODAY, self._snapped())
        member_cands = [c for c in report["candidates"] if c["role"] == "member"]
        self.assertTrue(all(c["safe"] for c in member_cands))

    def test_clan_well_below_min_size_all_members_protected(self):
        # 8 members, MIN=40 → max_boots=0
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 40):
            report = bootbot.build_report_from_snapshot(self.conn, self.TODAY, self._snapped())
        member_cands = [c for c in report["candidates"] if c["role"] == "member"]
        self.assertTrue(all(c["safe"] for c in member_cands))

    def test_partial_boots_worst_first(self):
        # 8 members, MIN=6 → max_boots=2 → only the 2 lowest-fame slackers booted
        fame_map = {"#S1": 100, "#S2": 200, "#S3": 800, "#S4": 800, "#S5": 800}
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 6):
            report = bootbot.build_report_from_snapshot(
                self.conn, self.TODAY, self._snapped(fame_map=fame_map)
            )
        booted = [c for c in report["candidates"] if c["role"] == "member" and not c["safe"]]
        self.assertEqual(len(booted), 2)
        booted_tags = {c["tag"] for c in booted}
        self.assertIn("#S1", booted_tags)
        self.assertIn("#S2", booted_tags)

    def test_enough_headroom_all_booted(self):
        # 8 members, MIN=2 → max_boots=6 ≥ 5 slackers → all 5 are not safe
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 2):
            report = bootbot.build_report_from_snapshot(self.conn, self.TODAY, self._snapped())
        booted = [c for c in report["candidates"] if c["role"] == "member" and not c["safe"]]
        self.assertEqual(len(booted), 5)

    def test_demotions_never_protected(self):
        """Elder/coLeader candidates are always actioned regardless of clan size."""
        ins_snap(self.conn, "2026-03-20", "warDay", self.SECTION, "#ELD", "Elder", 4)
        ins_member(self.conn, "2026-03-20", "#ELD", "Elder", role="elder")
        snapped = self._snapped()
        snapped["participants"].append(participant("#ELD", "Elder", decks_used=4, fame=800))
        snapped["members"].append(member("#ELD", "Elder", role="elder"))
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 999):
            report = bootbot.build_report_from_snapshot(self.conn, self.TODAY, snapped)
        elder_cands = [c for c in report["candidates"] if c["role"] == "elder"]
        self.assertGreater(len(elder_cands), 0)
        self.assertFalse(any(c["safe"] for c in elder_cands))

    def test_training_day_no_candidates(self):
        snapped = self._snapped()
        snapped["period_type"] = "trainingDay"
        with patch.object(bootbot, 'MIN_CLAN_SIZE', 0):
            report = bootbot.build_report_from_snapshot(self.conn, self.TODAY, snapped)
        self.assertEqual(report["candidates"], [])


# ── load_snapshot_from_db ─────────────────────────────────────────────────────

class TestLoadSnapshotFromDb(unittest.TestCase):

    def setUp(self):
        self.conn = make_db()

    def tearDown(self):
        self.conn.close()

    def test_missing_race_snapshot_raises(self):
        ins_member(self.conn, "2026-03-20", "#AAA", "Alice")
        with self.assertRaises(LookupError):
            bootbot.load_snapshot_from_db(self.conn, "2026-03-20")

    def test_missing_member_snapshot_raises(self):
        ins_snap(self.conn, "2026-03-20", "warDay", 0, "#AAA", "Alice", 4)
        with self.assertRaises(LookupError):
            bootbot.load_snapshot_from_db(self.conn, "2026-03-20")

    def test_round_trips_participant_fields(self):
        ins_snap(
            self.conn, "2026-03-20", "warDay", 1, "#AAA", "Alice",
            decks_used=8, decks_used_today=4, fame=1600, boat_attacks=2,
        )
        ins_member(self.conn, "2026-03-20", "#AAA", "Alice", role="elder")
        snapped = bootbot.load_snapshot_from_db(self.conn, "2026-03-20")
        self.assertEqual(snapped["period_type"],   "warDay")
        self.assertEqual(snapped["section_index"], 1)
        p = snapped["participants"][0]
        self.assertEqual(p["tag"],          "#AAA")
        self.assertEqual(p["decksUsed"],    8)
        self.assertEqual(p["decksUsedToday"], 4)
        self.assertEqual(p["fame"],         1600)
        self.assertEqual(p["boatAttacks"],  2)

    def test_round_trips_member_fields(self):
        ins_snap(self.conn, "2026-03-20", "warDay", 0, "#AAA", "Alice", 4)
        ins_member(self.conn, "2026-03-20", "#AAA", "Alice", role="coLeader",
                   last_seen="20260320T120000.000Z")
        snapped = bootbot.load_snapshot_from_db(self.conn, "2026-03-20")
        m = snapped["members"][0]
        self.assertEqual(m["tag"],      "#AAA")
        self.assertEqual(m["role"],     "coLeader")
        self.assertEqual(m["lastSeen"], "20260320T120000.000Z")

    def test_multiple_participants_and_members(self):
        for tag in ("#A", "#B", "#C"):
            ins_snap(self.conn,   "2026-03-20", "warDay", 0, tag, tag, 4)
            ins_member(self.conn, "2026-03-20", tag, tag)
        snapped = bootbot.load_snapshot_from_db(self.conn, "2026-03-20")
        self.assertEqual(len(snapped["participants"]), 3)
        self.assertEqual(len(snapped["members"]),      3)


if __name__ == "__main__":
    unittest.main()
