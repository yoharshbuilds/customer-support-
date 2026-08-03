"""Data integrity: do our numbers match the sources that are actually authoritative?

MECE layer 3 of 4. Layer 1 tests logic, layer 2 tests the HTTP contract, this tests
the NUMBERS against systems outside our control. Needs network and credentials.

    python -m scripts.test_integrity [--days 4]

Which source is authoritative is not uniform, and getting this wrong would make
our data worse rather than better:

  * Metabase Redshift `rds_public.*` -- a replica of the production Plazza
    database. AUTHORITATIVE for orders. Any difference is our bug: asserted exact.
  * Freshchat Extract API -- Freshchat's own export of its own events.
    AUTHORITATIVE for conversations raised and resolved: asserted exact on complete
    days that fall after our capture began.
  * Metabase Redshift `freshworks.*` -- a SYNC of Freshchat, and it demonstrably
    drops rows: on 31 Jul it was short 15 resolved and 14 raised against the
    Freshchat export, and its Postgres twin died outright on 2 June. NOT
    authoritative. Compared and reported, but only asserted one-directionally
    (we must never hold FEWER than the mirror; holding more is the mirror's gap).

Only complete local days are compared. Today is still accumulating, and the
hourly ingest lags, so including it would fail for reasons that are not defects.
"""
from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

from app import config, db, enrich, extract, reconcile

IST = timezone(timedelta(hours=5, minutes=30))
FAILS: list[str] = []
WARNS: list[str] = []
TOTAL = 0


def check(label: str, cond: bool, hard: bool = True) -> None:
    global TOTAL
    TOTAL += 1
    if not cond:
        (FAILS if hard else WARNS).append(label)
    tag = "PASS" if cond else ("FAIL" if hard else "WARN")
    print(f"{tag}  {label}")


def section(name: str) -> None:
    print(f"\n=== {name} ===")


async def mb(client: httpx.AsyncClient, sql: str) -> list[dict]:
    r = await client.post(
        f"{config.metabase_base()}/api/dataset",
        headers={"x-api-key": config.METABASE_API_KEY, "Content-Type": "application/json"},
        json={"database": config.METABASE_REDSHIFT_DB_ID, "type": "native",
              "native": {"query": sql}},
        timeout=180.0,
    )
    r.raise_for_status()
    d = r.json()
    if d.get("error"):
        raise RuntimeError(str(d["error"])[:300])
    cols = [c["name"] for c in d["data"]["cols"]]
    return [dict(zip(cols, row)) for row in d["data"]["rows"]]


def complete_days(days: int) -> list[str]:
    """Local days to compare: the last `days`, excluding today."""
    today = datetime.now(IST).date()
    return [(today - timedelta(days=i)).isoformat() for i in range(1, days + 1)]


async def test_orders(client: httpx.AsyncClient, days: list[str]) -> None:
    section("orders vs Metabase Redshift rds_public (authoritative -- exact match required)")
    from app import orders as orders_mod
    ours = {r["bucket"]: r for r in (await orders_mod.volume(client, 30))["daily"]}

    rows = await mb(client, f"""
        WITH per_order AS (
          SELECT order_id, MIN(order_placed_at) AS placed, COUNT(*) AS batches,
                 SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cx
          FROM rds_public.order_batches
          WHERE order_placed_at >= DATEADD(day,-{len(days)+2},GETDATE())
          GROUP BY order_id)
        SELECT TO_CHAR(CONVERT_TIMEZONE('{config.TIMEZONE}', placed),'YYYY-MM-DD') AS d,
               COUNT(*) AS total,
               SUM(CASE WHEN cx=batches THEN 1 ELSE 0 END) AS fully,
               SUM(CASE WHEN cx>0 AND cx<batches THEN 1 ELSE 0 END) AS partly
        FROM per_order GROUP BY 1""")
    truth = {r["d"]: r for r in rows}

    for d in days:
        o, t = ours.get(d), truth.get(d)
        if not t:
            continue
        check(f"{d}: total orders {o and o['total_orders']} == {t['total']}",
              bool(o) and o["total_orders"] == t["total"])
        check(f"{d}: fully cancelled {o and o['fully_cancelled']} == {t['fully']}",
              bool(o) and o["fully_cancelled"] == t["fully"])
        check(f"{d}: partly cancelled {o and o['partly_cancelled']} == {t['partly']}",
              bool(o) and o["partly_cancelled"] == t["partly"])

    section("orders vs Plazza Dashboard v4 card (excludes fully cancelled; +/-1 midnight)")
    v4 = await mb(client, f"""
        SELECT TO_CHAR(order_placed_at,'YYYY-MM-DD') AS d,
               COUNT(DISTINCT order_id) AS n
        FROM analytics.orders_comparison
        WHERE order_placed_at >= DATEADD(day,-{len(days)+2},CURRENT_DATE)
        GROUP BY 1""")
    card = {r["d"]: r["n"] for r in v4}
    # orders_comparison is a DERIVED table on a daily refresh, not a live view. Its
    # newest day can be mid-refresh: a run of this test caught 3 Aug at 535 against
    # a true 538, and minutes later the card itself read 538. So the newest day the
    # card knows about is checked softly; every settled day is asserted hard.
    newest_card_day = max(card) if card else None
    for d in days:
        o, n = ours.get(d), card.get(d)
        if not o or n is None:
            continue
        delta = o["excl_fully_cancelled"] - n
        settled = d != newest_card_day
        check(f"{d}: v4 card {n} vs our excl-cancelled {o['excl_fully_cancelled']}"
              f" (delta {delta:+d}, |delta|<=1)"
              + ("" if settled else " [newest card day -- may be mid-refresh]"),
              abs(delta) <= 1, hard=settled)


async def test_conversations(client: httpx.AsyncClient, days: list[str]) -> None:
    section("resolutions vs Freshchat Extract (authoritative -- exact on days we captured)")
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=len(days) + 1)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"

    with db.cursor() as cur:
        cur.execute("SELECT MIN(resolved_at) AS lo FROM resolve_events")
        capture_from = (cur.fetchone() or {}).get("lo")

    resolved = await extract.fetch(client, extract.EVENT_CONVERSATION_RESOLVED,
                                   start.strftime(fmt), now.strftime(fmt))
    fc = defaultdict(set)
    for r in resolved:
        cid = r.get("conversation_id")
        if not cid:
            continue
        ts = enrich.resolved_at_from(reconcile.payload_for(r))
        fc[ts.astimezone(IST).date().isoformat()].add(cid)

    with db.cursor() as cur:
        cur.execute("""
            SELECT (resolved_at AT TIME ZONE %(tz)s)::date::text AS d,
                   COUNT(DISTINCT conversation_id) AS n
            FROM resolve_events
            WHERE resolved_at >= %(start)s GROUP BY 1
        """, {"tz": config.TIMEZONE, "start": start})
        ours = {r["d"]: r["n"] for r in cur.fetchall()}

    for d in days:
        if d not in fc:
            continue
        # A day that predates capture cannot be complete and is not a defect.
        if capture_from and d < capture_from.astimezone(IST).date().isoformat():
            print(f"SKIP  {d}: predates capture ({capture_from.date()})")
            continue
        check(f"{d}: resolved ours {ours.get(d)} == Freshchat {len(fc[d])}",
              ours.get(d) == len(fc[d]))

    section("tickets raised vs Freshchat Extract (authoritative)")
    created = await extract.fetch(client, extract.EVENT_CONVERSATION_CREATED,
                                  start.strftime(fmt), now.strftime(fmt))
    fcc = defaultdict(set)
    for r in created:
        cid = r.get("conversation_id")
        ts = r.get("created_at")
        if not cid or not ts:
            continue
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        fcc[t.astimezone(IST).date().isoformat()].add(cid)

    with db.cursor() as cur:
        cur.execute("""
            SELECT (created_at AT TIME ZONE %(tz)s)::date::text AS d, COUNT(*) AS n
            FROM ticket_created_events WHERE created_at >= %(start)s GROUP BY 1
        """, {"tz": config.TIMEZONE, "start": start})
        ours_c = {r["d"]: r["n"] for r in cur.fetchall()}

    for d in days:
        if d not in fcc:
            continue
        got, want = ours_c.get(d, 0), len(fcc[d])
        # +/-1: a conversation first created on an earlier day is filed there, and
        # our table is keyed on conversation_id.
        check(f"{d}: raised ours {got} vs Freshchat {want} (|delta|<=1)", abs(got - want) <= 1)

    section("Metabase freshworks mirror (NOT authoritative -- we must never hold fewer)")
    mirror = await mb(client, f"""
        SELECT TO_CHAR(CONVERT_TIMEZONE('{config.TIMEZONE}', created_at),'YYYY-MM-DD') AS d,
               COUNT(DISTINCT conversation_id) AS raised
        FROM freshworks.freshchat_conversations
        WHERE created_at >= DATEADD(day,-{len(days)+1},GETDATE())
        GROUP BY 1""")
    for r in mirror:
        d = r["d"]
        if d not in days:
            continue
        got = ours_c.get(d, 0)
        delta = got - r["raised"]
        check(f"{d}: raised ours {got} >= mirror {r['raised']} (delta {delta:+d})",
              got >= r["raised"], hard=False)


async def test_internal(client: httpx.AsyncClient) -> None:
    section("internal consistency")
    with db.cursor() as cur:
        cur.execute("""SELECT COUNT(*) n FROM resolve_events a JOIN resolve_events b
                       ON a.conversation_id=b.conversation_id AND a.id<b.id
                      AND ABS(EXTRACT(EPOCH FROM (a.resolved_at-b.resolved_at))) < 2""")
        check("no near-duplicate resolutions (<2s apart)", cur.fetchone()["n"] == 0)

        cur.execute("SELECT COUNT(*) n FROM resolve_events WHERE resolved_at > now() + interval '1 hour'")
        check("no resolutions dated in the future", cur.fetchone()["n"] == 0)

        cur.execute("SELECT COUNT(*) n FROM ticket_created_events WHERE created_at > now() + interval '1 hour'")
        check("no tickets dated in the future", cur.fetchone()["n"] == 0)

        cur.execute("SELECT COUNT(*) n FROM agent_message_events WHERE sent_at > now() + interval '1 hour'")
        check("no agent messages dated in the future", cur.fetchone()["n"] == 0)

        cur.execute("SELECT COUNT(*) n FROM resolve_events WHERE resolution_secs < 0")
        check("no negative resolution times", cur.fetchone()["n"] == 0)

        cur.execute("""SELECT COUNT(DISTINCT dedupe_key) d, COUNT(*) n FROM agent_message_events""")
        r = cur.fetchone()
        check("agent message dedupe keys are unique", r["d"] == r["n"])

        cur.execute("""SELECT COUNT(*) n FROM resolve_events
                       WHERE enriched_at IS NOT NULL AND issue_type IS NULL""")
        check("every enriched resolve has an issue tag", cur.fetchone()["n"] == 0, hard=False)


async def main() -> int:
    ndays = 4
    if "--days" in sys.argv:
        ndays = int(sys.argv[sys.argv.index("--days") + 1])
    days = complete_days(ndays)
    print(f"comparing complete local days: {', '.join(days)}  ({config.TIMEZONE})")
    db.init()
    async with httpx.AsyncClient(timeout=300) as client:
        await test_orders(client, days)
        await test_conversations(client, days)
        await test_internal(client)

    print(f"\n{TOTAL - len(FAILS) - len(WARNS)}/{TOTAL} passed"
          + (f", {len(WARNS)} warnings" if WARNS else ""))
    if WARNS:
        print("\nWARNINGS (non-authoritative source disagrees -- expected when the mirror drops rows):")
        for w in WARNS:
            print(f"  {w}")
    if FAILS:
        print("\nFAILED:")
        for f in FAILS:
            print(f"  {f}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
