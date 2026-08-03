"""Attendance derived from resolve activity.

IMPORTANT, and the first thing to say to anyone reading the dashboard: this is a
proxy, not a punch clock. Nothing in the stack records when an agent signed in.
What we have is the moment each chat was resolved, so "present" means "resolved
at least one chat during that shift day", and the shift window is the span
between the first and last resolve. An agent who worked a full shift on chats
someone else closed reads as absent, and an idle hour inside a shift is
invisible. It is a floor on attendance, never an exact one.

The one place this would become exact is a poller storing Freshchat's
availability_status over time; the API exposes the current value only.
"""
from __future__ import annotations

from . import config, db

# Everything is bucketed into a shift day that starts at SHIFT_START_HOUR local
# time, so a night shift stays one row instead of being split across midnight.
_SHIFT_DAY = "(local_ts - make_interval(hours => %(shift_hour)s))::date"

_ROWS_SQL = f"""
WITH local AS (
    SELECT COALESCE(resolver_agent_id,   agent_id)   AS agent_id,
           COALESCE(resolver_agent_name, agent_name) AS agent_name,
           resolved_at,
           resolved_at AT TIME ZONE %(tz)s           AS local_ts
    FROM resolve_events
    WHERE COALESCE(resolver_agent_name, agent_name) IS NOT NULL
)
SELECT agent_id,
       agent_name,
       {_SHIFT_DAY}                                   AS shift_day,
       MIN(local_ts)                                  AS first_activity,
       MAX(local_ts)                                  AS last_activity,
       EXTRACT(EPOCH FROM (MAX(resolved_at) - MIN(resolved_at))) AS span_secs,
       COUNT(*)                                       AS resolves
FROM local
WHERE {_SHIFT_DAY} > (
        (now() AT TIME ZONE %(tz)s - make_interval(hours => %(shift_hour)s))::date
        - make_interval(days => %(days)s)
      )
GROUP BY 1, 2, 3
ORDER BY 3 DESC, 7 DESC
"""

# How far the underlying data actually reaches. Without this a day before the
# backfill window looks like a day when the whole team was absent.
_BOUNDS_SQL = f"""
WITH local AS (
    SELECT resolved_at AT TIME ZONE %(tz)s AS local_ts FROM resolve_events
)
SELECT MIN({_SHIFT_DAY}) AS first_day,
       MAX({_SHIFT_DAY}) AS last_day,
       (now() AT TIME ZONE %(tz)s - make_interval(hours => %(shift_hour)s))::date AS today_shift_day,
       (now() AT TIME ZONE %(tz)s) AS local_now
FROM local
"""


def _params(days: int) -> dict:
    return {"tz": config.TIMEZONE, "shift_hour": config.SHIFT_START_HOUR, "days": days}


def bounds() -> dict:
    with db.cursor() as cur:
        cur.execute(_BOUNDS_SQL, _params(0))
        return cur.fetchone() or {}


def rows(days: int) -> list[dict]:
    """One row per agent per shift day they were active."""
    with db.cursor() as cur:
        cur.execute(_ROWS_SQL, _params(days))
        out = []
        for r in cur.fetchall():
            # EXTRACT(EPOCH ...) comes back as Decimal, which will not divide by
            # a float. Cast before the arithmetic, not after.
            span_hrs = round(float(r["span_secs"] or 0) / 3600.0, 1)
            out.append({
                "agent_id": r["agent_id"],
                "agent": r["agent_name"],
                "day": r["shift_day"].isoformat(),
                "first": r["first_activity"].strftime("%H:%M"),
                "last": r["last_activity"].strftime("%H:%M"),
                "first_iso": r["first_activity"].isoformat(),
                "last_iso": r["last_activity"].isoformat(),
                "span_hrs": span_hrs,
                "resolves": r["resolves"],
                "short": span_hrs < config.SHORT_SHIFT_HOURS,
                # A window ending before it starts means the shift crossed
                # midnight -- worth showing, it is the night rota.
                "overnight": r["last_activity"].time() < r["first_activity"].time(),
            })
        return out
