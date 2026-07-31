"""End-to-end probe: run the real enrichment path against live Freshchat + Metabase.

No DB required -- the order_dim cache is swapped for an in-memory dict so the
exact production code path (freshchat.py -> metabase.py -> enrich.py) runs.

Usage:
    python -m scripts.probe_one <conversation_id> [<conversation_id> ...]
"""
from __future__ import annotations
import asyncio
import json
import sys

import httpx

from app import db, enrich, freshchat

_cache: dict[str, dict] = {}
db.get_order_dim = lambda oid: _cache.get(oid)  # type: ignore[assignment]
db.put_order_dim = lambda oid, store, ful: _cache.__setitem__(  # type: ignore[assignment]
    oid, {"store": store, "fulfillment": ful}
)


def fmt(secs):
    if secs is None:
        return "n/a"
    h, m = divmod(int(secs) // 60, 60)
    return f"{h}h {m}m" if h else f"{m}m"


async def main(conv_ids: list[str]) -> None:
    async with httpx.AsyncClient() as client:
        for cid in conv_ids:
            print("=" * 74)
            try:
                conv = await freshchat.fetch_conversation(client, cid)
            except httpx.HTTPError as e:
                print(f"{cid}  FETCH FAILED: {e}")
                continue

            resolved_at = enrich.parse_ts(conv.get("updated_time"))
            if resolved_at is None:
                print(f"{cid}  no updated_time, skipping")
                continue

            row = await enrich.enrich(client, cid, resolved_at)
            row["conversation_id"] = cid
            row["resolved_at"] = resolved_at
            row["_status"] = conv.get("status")
            print(json.dumps(row, indent=2, default=str))
            print(
                f"--> issue={row['issue_type']!r} order={row['order_id']!r} "
                f"store={row['store']!r} fulfillment={row['fulfillment']!r} "
                f"resolution={fmt(row['resolution_secs'])}"
            )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    asyncio.run(main(sys.argv[1:]))
