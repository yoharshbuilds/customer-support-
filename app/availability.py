"""Capture Freshchat agent availability webhooks -- a real sign-in record.

Freshchat is already sending these: /healthz showed `agent_availability` in the
by_action tally, 30 of them, all landing in the ignore branch where they were
counted, logged, and thrown away. Storing them turns attendance from "resolved a
chat at some point" into "was signed in from 07:34 to 14:02".

The payload shape is NOT documented. The public webhook docs list four events
(message_create, conversation_assignment, conversation_resolution,
conversation_reopen) and this is not one of them, so every field name below is a
best-effort guess across the shapes Freshchat uses elsewhere. That is exactly why
`raw` is stored verbatim on every row: if a guess is wrong, the extracted columns
can be rebuilt from raw with an UPDATE, and nothing is lost in the meantime.

Inspect what actually arrives with /debug/availability?secret=...
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

from . import config, enrich

log = logging.getLogger(__name__)

# Keys that have held an availability value in Freshchat payloads. Order matters:
# the most specific name wins, so a generic "status" cannot shadow the real one.
_STATUS_KEYS = ("availability_status", "agent_availability", "availability",
                "presence_status", "presence", "agent_status", "status", "state")
# Where the agent id might live. `actor_id` is what the resolution payloads use.
_AGENT_KEYS = ("agent_id", "actor_id", "user_id", "id")
# When the change happened, as opposed to when we received it.
_TIME_KEYS = ("action_time", "occurred_at", "changed_at", "updated_time",
              "updated_at", "timestamp", "created_time", "time")


def is_availability(payload: dict) -> bool:
    """True when this webhook is an agent availability/presence change.

    Matched on a substring rather than an exact action name: the event is
    undocumented, so its label may well change, and a rename must not silently
    switch the capture off.
    """
    action = (enrich._walk(payload, "action") or "").lower()
    if not action:
        return False
    if "availability" in action or "presence" in action:
        return True
    # "agent_status" style labels, but not conversation status changes -- those
    # are resolutions and belong to the other path.
    return "agent" in action and "status" in action


def _first(payload: dict, keys) -> str | None:
    for k in keys:
        v = enrich._walk(payload, k)
        if v:
            return v
    return None


def _walk_any(obj, key: str):
    """Like enrich._walk but returns the value whatever its type.

    Needed because the status can arrive as an object: the live /agents API
    returns `"agent_status": {"name": "Active on Intelli Assign"}`, and
    enrich._walk only ever yields strings, so that shape read as no status at all.
    """
    if isinstance(obj, dict):
        if key in obj and obj[key] not in (None, "", [], {}):
            return obj[key]
        for sub in obj.values():
            found = _walk_any(sub, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for sub in obj:
            found = _walk_any(sub, key)
            if found is not None:
                return found
    return None


def _status_value(payload: dict) -> str | None:
    """The availability string, whether it is a bare value or wrapped in an object."""
    for k in _STATUS_KEYS:
        v = _walk_any(payload, k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            # {"name": "Active on Intelli Assign"} and friends
            for inner in ("name", "status", "value", "label"):
                got = v.get(inner)
                if isinstance(got, str) and got.strip():
                    return got.strip()
        if isinstance(v, bool):
            return "true" if v else "false"
    return None


def normalise_status(raw: str | None) -> str:
    """available / unavailable / unknown.

    "UNAVAILABLE" contains "avail", so the negative must be tested first or every
    sign-out reads as a sign-in.
    """
    s = (raw or "").strip().lower()
    if not s:
        return "unknown"
    # Negatives first, and by substring: "UNAVAILABLE" contains "avail", and
    # "Inactive on Intelli Assign" contains "active". Either one tested in the
    # wrong order silently inverts every sign-out into a sign-in.
    if "unavail" in s or "inactive" in s or s in ("offline", "away", "false", "0"):
        return "unavailable"
    if "avail" in s or "active" in s or s in ("online", "true", "1"):
        return "available"
    return "unknown"


def _agent_id(payload: dict) -> str | None:
    """Prefer an id from an actor/agent sub-object over a bare top-level id.

    A top-level "id" on this payload could be the event's own id. Looking inside
    the actor first avoids keying attendance on the wrong thing.
    """
    for container in ("actor", "agent", "data", "user"):
        sub = payload.get(container)
        if isinstance(sub, dict):
            got = _first(sub, _AGENT_KEYS)
            if got:
                return got
    return _first(payload, _AGENT_KEYS)


def extract(payload: dict) -> dict:
    """Best-effort structured view of an availability webhook."""
    raw_status = _status_value(payload)
    changed_at = enrich.parse_ts(_first(payload, _TIME_KEYS))
    action = (enrich._walk(payload, "action") or "unknown")[:60]
    agent_id = _agent_id(payload)
    return {
        "agent_id": agent_id,
        "raw_status": (raw_status or "")[:60] or None,
        "status": normalise_status(raw_status),
        "changed_at": changed_at,
        "action": action,
        "raw": payload,
        # Freshchat retries webhooks, and a retry must not become a second
        # sign-in. Keyed on the content of the event, not on arrival time, so a
        # replay collapses onto the row it already wrote. Falls back to hashing
        # the whole body when the fields we need are missing -- better a
        # duplicate-ish row than a dropped event.
        "dedupe_key": hashlib.sha256(
            "|".join([
                str(agent_id or ""),
                str(raw_status or ""),
                changed_at.isoformat() if changed_at else json.dumps(payload, sort_keys=True)[:400],
            ]).encode()
        ).hexdigest(),
    }


# --- derivation -----------------------------------------------------------

def _shift_day(ts: datetime, tz) -> str:
    """Which shift day a moment belongs to, matching the activity proxy."""
    local = ts.astimezone(tz)
    return (local - timedelta(hours=config.SHIFT_START_HOUR)).date().isoformat()


def sessions_from_events(events: list[dict], now: datetime, tz) -> list[dict]:
    """Pair availability transitions into signed-in sessions.

    `events` must be ordered oldest-first and already scoped to one agent.

    Rules, all of which exist because a webhook stream is not a tidy sequence:
      * available -> available is not two sessions; the first one is still open.
      * unavailable with nothing open is a sign-out we never saw the start of,
        so it is dropped rather than invented.
      * a session still open is closed at `now`, and flagged, so an agent who
        never signs out does not accumulate infinite hours.
      * unknown statuses are ignored entirely -- guessing would be worse than
        having no row.
    """
    out: list[dict] = []
    open_at: datetime | None = None
    for ev in events:
        ts = ev.get("effective_at")
        status = ev.get("status")
        if ts is None or status == "unknown":
            continue
        if status == "available":
            if open_at is None:
                open_at = ts
        elif status == "unavailable":
            if open_at is not None:
                if ts > open_at:
                    out.append({"start": open_at, "end": ts, "open": False})
                open_at = None
    if open_at is not None:
        out.append({"start": open_at, "end": now, "open": True})

    for s in out:
        s["shift_day"] = _shift_day(s["start"], tz)
        s["secs"] = max(0.0, (s["end"] - s["start"]).total_seconds())
    return out


def aggregate(sessions: list[dict]) -> dict:
    """Collapse one agent's sessions into per-shift-day attendance."""
    by_day: dict[str, dict] = {}
    for s in sessions:
        d = by_day.setdefault(s["shift_day"], {
            "first": s["start"], "last": s["end"], "secs": 0.0,
            "sessions": 0, "open": False,
        })
        d["first"] = min(d["first"], s["start"])
        d["last"] = max(d["last"], s["end"])
        d["secs"] += s["secs"]
        d["sessions"] += 1
        d["open"] = d["open"] or s["open"]
    return by_day
