"""Order volume, including cancelled orders, day by day and week by week.

Source is rds_public.order_batches on Metabase's REDSHIFT connection, queried
through the dataset API. Redshift carries the same table as the Postgres instance
and is exactly as fresh -- both reported the same max(order_placed_at) to the
millisecond -- and it is what Plazza Dashboard v4 reads, so these numbers agree
with what the team already sees. Verified identical on eight consecutive days:
totals 488/604/636/520/524/549/557/562 and fully-cancelled 28/36/49/35/29/45/45/47
matched Postgres exactly.

Redshift dialect, which differs from Postgres in three ways that matter here:
FILTER is unsupported (use SUM(CASE ...)), timezone conversion is
CONVERT_TIMEZONE rather than AT TIME ZONE, and intervals are DATEADD.

Why not the sources the existing Plazza Dashboard v4 uses:

  * analytics.orders_comparison on database 7 (Postgres) is STALE -- it stops at
    2025-06-21 with 3,536 rows. The dashboard cards read it from database 8
    (Redshift), where it is current.
  * Redshift orders_comparison EXCLUDES fully cancelled orders. Verified against
    Postgres on every complete day: not_cancelled + partly_cancelled matches its
    figure exactly (2 Aug 551+17=568, 1 Aug 575+12=587, 27 Jul 489+26=515). So
    the dashboard's "Total Order Count" is not "including cancel".
  * rds_public.orders cannot be counted directly: status='cart' is an abandoned
    cart, and there were 15,114 of those in one week against ~2,100 real orders.

An order has many batches, so counting is done per order after grouping, and
cancellation is split three ways -- every batch cancelled, some batches
cancelled, none -- because "cancelled order" is ambiguous when only part of an
order was cancelled.
"""
from __future__ import annotations

import logging
import time

import httpx

from . import config

log = logging.getLogger(__name__)

# Metabase queries take seconds and the page polls, so results are cached
# briefly. 20s keeps 'orders placed' visibly live without every viewer costing a
# separate Redshift scan.
CACHE_SECONDS = 20
_cache: dict[str, tuple[float, dict]] = {}

_PER_ORDER = """
WITH per_order AS (
    SELECT order_id,
           MIN(order_placed_at)                                    AS placed,
           COUNT(*)                                                AS batches,
           SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)    AS cancelled_batches
    FROM rds_public.order_batches
    WHERE order_placed_at >= DATEADD(day, -%(days)s, GETDATE())
    GROUP BY order_id
)
"""

# %%(days)s is doubled because these strings go through Python %% formatting
# before Metabase ever sees them.
SQL_DAILY = _PER_ORDER + """
SELECT TO_CHAR(CONVERT_TIMEZONE('%(tz)s', placed), 'YYYY-MM-DD')          AS bucket,
       COUNT(*)                                                          AS total_orders,
       SUM(CASE WHEN cancelled_batches = batches THEN 1 ELSE 0 END)      AS fully_cancelled,
       SUM(CASE WHEN cancelled_batches > 0
                 AND cancelled_batches < batches THEN 1 ELSE 0 END)      AS partly_cancelled,
       SUM(CASE WHEN cancelled_batches = 0 THEN 1 ELSE 0 END)            AS not_cancelled
FROM per_order
GROUP BY 1
ORDER BY 1 DESC
"""

SQL_WEEKLY = _PER_ORDER + """
SELECT TO_CHAR(DATE_TRUNC('week', CONVERT_TIMEZONE('%(tz)s', placed)),
               'YYYY-MM-DD')                                             AS bucket,
       COUNT(*)                                                          AS total_orders,
       SUM(CASE WHEN cancelled_batches = batches THEN 1 ELSE 0 END)      AS fully_cancelled,
       SUM(CASE WHEN cancelled_batches > 0
                 AND cancelled_batches < batches THEN 1 ELSE 0 END)      AS partly_cancelled,
       SUM(CASE WHEN cancelled_batches = 0 THEN 1 ELSE 0 END)            AS not_cancelled,
       COUNT(DISTINCT TO_CHAR(CONVERT_TIMEZONE('%(tz)s', placed), 'YYYY-MM-DD'))
                                                                         AS days_in_bucket
FROM per_order
GROUP BY 1
ORDER BY 1 DESC
"""


async def _query(client: httpx.AsyncClient, sql: str) -> list[dict]:
    r = await client.post(
        f"{config.metabase_base()}/api/dataset",
        headers={"x-api-key": config.METABASE_API_KEY, "Content-Type": "application/json"},
        json={"database": config.METABASE_REDSHIFT_DB_ID, "type": "native",
              "native": {"query": sql}},
        timeout=120.0,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") == "failed" or data.get("error"):
        raise RuntimeError(f"metabase query failed: {str(data.get('error'))[:300]}")
    cols = [c["name"] for c in data["data"]["cols"]]
    return [dict(zip(cols, row)) for row in data["data"]["rows"]]


def _rate(row: dict) -> dict:
    total = row.get("total_orders") or 0
    fully = row.get("fully_cancelled") or 0
    partly = row.get("partly_cancelled") or 0
    return {
        **row,
        "cancel_rate": round(100.0 * fully / total, 1) if total else 0.0,
        # What the Plazza Dashboard v4 "Total Order Count" card shows, so the two
        # can be reconciled instead of looking like one of them is wrong.
        "excl_fully_cancelled": total - fully,
        "any_cancellation": fully + partly,
    }


async def volume(client: httpx.AsyncClient, days: int) -> dict:
    key = f"volume:{days}"
    hit = _cache.get(key)
    if hit and (time.monotonic() - hit[0]) < CACHE_SECONDS:
        return hit[1]

    params = {"days": days, "tz": config.TIMEZONE}
    daily = [_rate(r) for r in await _query(client, SQL_DAILY % params)]
    weekly = [_rate(r) for r in await _query(client, SQL_WEEKLY % params)]
    out = {
        "timezone": config.TIMEZONE,
        "days": days,
        "daily": daily,
        "weekly": weekly,
        "source": "rds_public.order_batches (Metabase Redshift)",
    }
    _cache[key] = (time.monotonic(), out)
    return out
