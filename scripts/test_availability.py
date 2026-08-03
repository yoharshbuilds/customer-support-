"""Tests for agent availability capture and session derivation.

The `agent_availability` webhook is UNDOCUMENTED -- Freshchat's payload article
lists only message_create, conversation_assignment, conversation_resolution and
conversation_reopen. So unlike test_webhook_parse.py, the payloads here are not
copied from a spec: they are the plausible shapes Freshchat uses elsewhere,
written to pin down the behaviour that must hold whichever one turns out to be
real.

That is the whole reason `raw` is stored on every row. When the real shape shows
up at /debug/availability, add it as a case here first, then fix extraction.

Usage: python -m scripts.test_availability
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app import availability

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


# --- shapes that might arrive ---------------------------------------------
SHAPES = [
    ("actor + availability_status", {
        "actor": {"actor_type": "agent", "actor_id": "agent-1"},
        "action": "agent_availability",
        "action_time": "2026-08-03T02:04:00.000Z",
        "data": {"availability": {"availability_status": "AVAILABLE"}},
    }, "agent-1", "available"),
    ("nested agent object", {
        "action": "agent_availability_update",
        "action_time": "2026-08-03T02:04:00.000Z",
        "data": {"agent": {"agent_id": "agent-2", "availability_status": "UNAVAILABLE"}},
    }, "agent-2", "unavailable"),
    ("presence wording", {
        "actor": {"actor_id": "agent-3"},
        "action": "agent_presence_change",
        "occurred_at": "2026-08-03T02:04:00.000Z",
        "presence": "online",
    }, "agent-3", "available"),
    # This wrapped-object shape is what the live /agents API actually returns for
    # agent_status, so it is the likeliest real payload of the lot.
    ("agent_status object, inactive", {
        "agent": {"id": "agent-4"},
        "action": "agent_status_updated",
        "timestamp": "2026-08-03T02:04:00.000Z",
        "agent_status": {"name": "Inactive on Intelli Assign"},
    }, "agent-4", "unavailable"),
    ("agent_status object, active", {
        "agent": {"id": "agent-5"},
        "action": "agent_availability",
        "timestamp": "2026-08-03T02:04:00.000Z",
        "agent_status": {"name": "Active on Intelli Assign"},
    }, "agent-5", "available"),
    ("boolean login_status", {
        "actor": {"actor_id": "agent-6"},
        "action": "agent_availability",
        "action_time": "2026-08-03T02:04:00.000Z",
        "data": {"availability": True},
    }, "agent-6", "available"),
]

NOT_AVAILABILITY = [
    ("conversation_resolution", {"action": "conversation_resolution"}),
    ("message_create", {"action": "message_create"}),
    ("conversation_assignment", {"action": "conversation_assignment"}),
    # A conversation status change is a resolve, not an agent going offline.
    ("conversation status change", {"action": "conversation_status_update", "status": "resolved"}),
]


def main() -> int:
    failures = 0
    total = 0

    def check(label, cond):
        nonlocal failures, total
        total += 1
        failures += not cond
        print(f"{'PASS' if cond else 'FAIL'}  {label}")

    # --- detection ---
    for label, payload, _, _ in SHAPES:
        check(f"detects availability: {label}", availability.is_availability(payload))
    for label, payload in NOT_AVAILABILITY:
        check(f"ignores: {label}", not availability.is_availability(payload))

    # --- extraction ---
    for label, payload, want_id, want_status in SHAPES:
        ev = availability.extract(payload)
        check(f"agent id from {label} -> {ev['agent_id']}", ev["agent_id"] == want_id)
        check(f"status from {label} -> {ev['status']}", ev["status"] == want_status)
        check(f"raw kept for {label}", ev["raw"] is payload)

    # The bug that would silently invert every sign-out: "UNAVAILABLE" contains
    # the substring "avail".
    check("UNAVAILABLE is not read as available",
          availability.normalise_status("UNAVAILABLE") == "unavailable")
    check("AVAILABLE is available", availability.normalise_status("AVAILABLE") == "available")
    check("empty status is unknown", availability.normalise_status("") == "unknown")
    check("garbage status is unknown", availability.normalise_status("On a break") == "unknown")
    # "Inactive..." contains "active", so substring order is load-bearing here too.
    check("'Inactive on Intelli Assign' is unavailable",
          availability.normalise_status("Inactive on Intelli Assign") == "unavailable")
    check("'Active on Intelli Assign' is available",
          availability.normalise_status("Active on Intelli Assign") == "available")

    # --- dedupe ---
    a = availability.extract(SHAPES[0][1])
    b = availability.extract(SHAPES[0][1])
    c = availability.extract(SHAPES[1][1])
    check("same event -> same dedupe key", a["dedupe_key"] == b["dedupe_key"])
    check("different event -> different key", a["dedupe_key"] != c["dedupe_key"])

    # --- sessions ---
    def ev(t, status):
        return {"effective_at": _dt(t), "status": status}

    now = _dt("2026-08-03T12:00:00")

    s = availability.sessions_from_events(
        [ev("2026-08-03T02:04:00", "available"), ev("2026-08-03T08:34:00", "unavailable")], now, IST)
    check("simple shift -> one session", len(s) == 1)
    check("shift length 6.5h", s and abs(s[0]["secs"] - 6.5 * 3600) < 1)
    # 02:04 UTC is 07:34 IST, which belongs to the 06:00-boundary day Aug 3.
    check("session filed under Aug 3 shift day", s and s[0]["shift_day"] == "2026-08-03")

    s = availability.sessions_from_events(
        [ev("2026-08-03T02:00:00", "available"), ev("2026-08-03T03:00:00", "available"),
         ev("2026-08-03T08:00:00", "unavailable")], now, IST)
    check("available twice is not two sessions", len(s) == 1)
    check("duplicate available keeps the earlier start", s and abs(s[0]["secs"] - 6 * 3600) < 1)

    s = availability.sessions_from_events([ev("2026-08-03T08:00:00", "unavailable")], now, IST)
    check("sign-out with no sign-in is dropped", s == [])

    s = availability.sessions_from_events([ev("2026-08-03T02:00:00", "available")], now, IST)
    check("open session is closed at now", len(s) == 1 and s[0]["open"] is True)
    check("open session length is to now", s and abs(s[0]["secs"] - 10 * 3600) < 1)

    s = availability.sessions_from_events(
        [ev("2026-08-03T02:00:00", "available"), ev("2026-08-03T03:00:00", "unknown"),
         ev("2026-08-03T08:00:00", "unavailable")], now, IST)
    check("unknown status does not end a session", len(s) == 1 and not s[0]["open"])

    # A night shift: 22:00 IST Aug 2 -> 05:00 IST Aug 3 is ONE shift, filed under
    # Aug 2, exactly as the activity proxy does it.
    s = availability.sessions_from_events(
        [ev("2026-08-02T16:30:00", "available"), ev("2026-08-02T23:30:00", "unavailable")],
        now, IST)
    check("night shift filed under the day it started",
          s and s[0]["shift_day"] == "2026-08-02")

    # zero-length and reversed pairs must not produce negative hours
    s = availability.sessions_from_events(
        [ev("2026-08-03T02:00:00", "available"), ev("2026-08-03T02:00:00", "unavailable")],
        now, IST)
    check("zero-length session dropped", s == [])

    # --- aggregate ---
    sess = availability.sessions_from_events(
        [ev("2026-08-03T02:00:00", "available"), ev("2026-08-03T04:00:00", "unavailable"),
         ev("2026-08-03T05:00:00", "available"), ev("2026-08-03T08:00:00", "unavailable")],
        now, IST)
    agg = availability.aggregate(sess)
    day = agg.get("2026-08-03", {})
    check("two sessions aggregate to one day", len(agg) == 1 and day.get("sessions") == 2)
    check("aggregate sums to 5h (breaks excluded)", abs(day.get("secs", 0) - 5 * 3600) < 1)
    check("aggregate first is the earliest sign-in",
          day.get("first") == _dt("2026-08-03T02:00:00"))
    check("aggregate last is the latest sign-out",
          day.get("last") == _dt("2026-08-03T08:00:00"))

    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
