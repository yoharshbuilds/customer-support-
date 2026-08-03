"""Backfill agent message activity from the Extract API.

The hourly ingest only covers recent hours, so this loads history. Idempotent:
re-running over the same range inserts nothing new.

Usage:
  python -m scripts.backfill_agent_messages 2026-07-27 2026-08-04
  python -m scripts.backfill_agent_messages 2026-07-27 2026-08-04 --chunk-days 2

Extract jobs are rate limited to one POST per minute per event, so multi-chunk
runs wait between chunks.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone

import httpx

from app import activity, db


def _day(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


async def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) < 2:
        print(__doc__)
        return 2
    start, end = _day(args[0]), _day(args[1])
    chunk = 2
    if "--chunk-days" in sys.argv:
        chunk = int(sys.argv[sys.argv.index("--chunk-days") + 1])

    db.init()
    total_rows = total_new = 0
    async with httpx.AsyncClient() as client:
        cursor = start
        first = True
        while cursor < end:
            stop = min(cursor + timedelta(days=chunk), end)
            if not first:
                print("  (waiting 65s for the 1 POST/min rate limit)")
                await asyncio.sleep(65)
            first = False
            try:
                fetched, new = await activity.ingest(client, cursor, stop)
                total_rows += fetched
                total_new += new
                print(f"{cursor:%Y-%m-%d} .. {stop:%Y-%m-%d}: {fetched} agent messages, {new} new")
            except Exception as e:  # noqa: BLE001 -- one bad chunk must not lose the rest
                print(f"{cursor:%Y-%m-%d} .. {stop:%Y-%m-%d}: FAILED {e}")
            cursor = stop

    print(f"\ntotal: {total_rows} agent messages seen, {total_new} newly stored")
    s = db.message_stats()
    print(f"table now: {s.get('messages')} messages, {s.get('agents')} agents, "
          f"{s.get('oldest')} .. {s.get('newest')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
