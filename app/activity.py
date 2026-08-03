"""Agent presence from message activity: first text -> last text.

Why this and not resolves: on 1 Aug the resolve-based proxy reported Ansila
Lissie at 0.0 hours and Sumayya A as absent. Message data shows 7.6h/125 messages
and 7.0h/113 messages -- full shifts, both. An agent texts continuously through a
shift; whether they are the one who clicks Resolve is close to arbitrary.

Source is the Extract API's Message-Sent event, which is historical and complete,
so this works backwards over data that already exists rather than only from the
day it is switched on.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

import httpx

from . import config, db, extract

log = logging.getLogger(__name__)

# Deliberate overlap on each catch-up: a message can land in Freshchat's export
# slightly after its timestamp, and re-ingesting is free because dedupe_key
# collides. Cheaper than missing the boundary.
INGEST_OVERLAP_HOURS = 3
INGEST_INTERVAL_SECONDS = int(60 * 60)


def _dedupe_key(agent_id: str, sent_at: str, conversation_id: str) -> str:
    """Message-Sent has no message id, so key on what identifies the message.

    interaction_id is per interaction and shared by many messages, so it cannot
    be used. actor + millisecond timestamp + conversation is unique in practice.
    """
    return hashlib.sha256(f"{agent_id}|{sent_at}|{conversation_id}".encode()).hexdigest()


def rows_from_extract(raw: list[dict]) -> list[dict]:
    """Turn Message-Sent CSV rows into insertable agent-message rows."""
    out = []
    for r in raw:
        # USER and SYSTEM rows exist in the same export; only agents mark attendance.
        if (r.get("actor_type") or "").strip().upper() != "AGENT":
            continue
        agent_id = (r.get("actor_id") or "").strip()
        sent_at = (r.get("created_at") or "").strip()
        if not agent_id or not sent_at:
            continue
        conv = (r.get("conversation_id") or "").strip()
        out.append({
            "agent_id": agent_id,
            "agent_name": (r.get("actor_name") or "").strip() or None,
            "conversation_id": conv or None,
            "sent_at": sent_at,
            "source": "extract",
            "dedupe_key": _dedupe_key(agent_id, sent_at, conv),
        })
    return out


async def ingest(client: httpx.AsyncClient, start: datetime, end: datetime) -> tuple[int, int]:
    """Pull Message-Sent for a window and store it. Returns (fetched, inserted)."""
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    raw = await extract.fetch(client, extract.EVENT_MESSAGE_SENT,
                             start.astimezone(timezone.utc).strftime(fmt),
                             end.astimezone(timezone.utc).strftime(fmt))
    rows = rows_from_extract(raw)
    inserted = db.insert_agent_messages(rows)
    log.info("message ingest %s..%s: %d rows, %d new",
             start.date(), end.date(), len(rows), inserted)
    return len(rows), inserted


async def catch_up(client: httpx.AsyncClient) -> tuple[int, int]:
    """Ingest the recent window. Safe to call repeatedly."""
    now = datetime.now(timezone.utc)
    return await ingest(client, now - timedelta(hours=INGEST_OVERLAP_HOURS), now)


def attendance_rows(days: int) -> dict:
    """{agent_id: {shift_day: {...}}} from message activity."""
    out: dict[str, dict] = {}
    for r in db.message_activity(days, config.TIMEZONE, config.SHIFT_START_HOUR):
        span = (r["last_at"] - r["first_at"]).total_seconds() / 3600.0
        out.setdefault(r["agent_id"], {})[r["shift_day"].isoformat()] = {
            "agent_name": r["agent_name"],
            "first_at": r["first_at"],
            "last_at": r["last_at"],
            "span_hrs": round(span, 1),
            "messages": r["messages"],
            "conversations": r["conversations"],
        }
    return out


def hourly(days: int) -> dict:
    """{agent_id: {shift_day: {hour: messages}}} in local time."""
    out: dict[str, dict] = {}
    for r in db.message_hourly(days, config.TIMEZONE, config.SHIFT_START_HOUR):
        (out.setdefault(r["agent_id"], {})
            .setdefault(r["shift_day"].isoformat(), {}))[str(r["hour"])] = r["messages"]
    return out
