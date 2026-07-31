"""Run the real enrichment path over many conversations and report coverage.

Usage: python -m scripts.probe_batch <n>   (ids sampled from the CSV export)
"""
from __future__ import annotations
import asyncio
import csv
import statistics
import sys
from collections import Counter

import httpx

from app import db, enrich, freshchat

csv.field_size_limit(10**9)
CSV = "../support_dashboard/raw_export.csv"

_cache: dict = {}
db.get_order_dim = lambda oid: _cache.get(oid)  # type: ignore[assignment]
db.put_order_dim = lambda oid, s, f: _cache.__setitem__(oid, {"store": s, "fulfillment": f})  # type: ignore[assignment]


def sample_ids(n: int) -> list[str]:
    seen: dict[str, int] = {}
    with open(CSV) as fh:
        for row in csv.DictReader(fh):
            if row["status"] == "resolved":
                seen[row["conversation_id"]] = seen.get(row["conversation_id"], 0) + 1
    # Bias toward long threads: those are the ones the pagination fix affects.
    ordered = sorted(seen, key=lambda c: -seen[c])
    return ordered[: n // 2] + ordered[len(ordered) // 2 : len(ordered) // 2 + (n - n // 2)]


async def one(client, sem, cid):
    async with sem:
        try:
            conv = await freshchat.fetch_conversation(client, cid)
            ts = enrich.parse_ts(conv.get("updated_time"))
            if ts is None:
                return None
            row = await enrich.enrich(client, cid, ts)
            row["conversation_id"] = cid
            return row
        except Exception as e:  # noqa: BLE001
            print(f"  !! {cid}: {type(e).__name__}: {str(e)[:90]}")
            return None


async def main(n: int):
    ids = sample_ids(n)
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient() as client:
        rows = [r for r in await asyncio.gather(*(one(client, sem, c) for c in ids)) if r]

    ok = len(rows)
    print(f"\nenriched {ok}/{len(ids)} conversations\n")
    if not ok:
        return

    def pct(k):
        return 100 * sum(1 for r in rows if r.get(k)) / ok

    print(f"  order_id resolved : {pct('order_id'):5.1f}%")
    print(f"  store resolved    : {pct('store'):5.1f}%")
    print(f"  fulfillment       : {pct('fulfillment'):5.1f}%")
    print(f"  issue_type        : {pct('issue_type'):5.1f}%")
    print(f"  agent_name        : {pct('agent_name'):5.1f}%")

    print("\n  store        :", dict(Counter(r["store"] for r in rows)))
    print("  fulfillment  :", dict(Counter(r["fulfillment"] for r in rows)))
    secs = [r["resolution_secs"] for r in rows if r["resolution_secs"] is not None]
    if secs:
        print(f"\n  median resolution: {statistics.median(secs)/60:.1f} min  (n={len(secs)})")
    print("\n  top issues:")
    for issue, c in Counter(r["issue_type"] for r in rows).most_common(6):
        print(f"    {c:3d}  {issue}")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 30))
