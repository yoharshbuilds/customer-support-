"""Resolve CART order ids to (store, fulfillment) via the Metabase dataset API.

Verified against 832 hand-labelled orders:
  * ``order_batches.is_instant = false``  <->  "deferred"  (86/86 exact, no false positives)
  * ``order_batches.store_id -> erp_public.stores``  covers ~94% of orders
The legacy "clubbed" bucket is NOT reproducible from the live schema
(``milk_run_id`` is entirely NULL), so it is deliberately not emitted.
"""
from __future__ import annotations
import logging
import re

import httpx

from . import config

log = logging.getLogger(__name__)

ORDER_ID_RE = re.compile(r"^CART-\d{3,}$")

# One row per order: an order can fan out into several batches, so aggregate
# rather than joining straight through (that would double-count).
SQL = """
SELECT o.order_id,
       (array_agg(s.store_name ORDER BY b.is_primary_store DESC NULLS LAST, b.created_at)
          FILTER (WHERE s.store_name IS NOT NULL))[1] AS store_name,
       bool_or(b.is_instant)            AS any_instant,
       count(b.id)                      AS n_batches,
       count(DISTINCT b.store_id)       AS n_stores
  FROM rds_public.orders o
  JOIN rds_public.order_batches b ON b.order_id = o.id
  LEFT JOIN erp_public.stores s  ON s.store_id = b.store_id
 WHERE o.order_id IN (%s)
 GROUP BY o.order_id
"""


def _norm_store(name: str | None) -> str | None:
    """'Yemalur001' -> 'Yemalur', 'Bellandur002' -> 'Bellandur'."""
    if not name:
        return None
    return re.sub(r"\d+$", "", name).strip() or None


async def lookup_orders(client: httpx.AsyncClient, order_ids: list[str]) -> dict[str, dict]:
    """Return {order_id: {"store": ..., "fulfillment": ...}} for the ids found."""
    ids = [o for o in dict.fromkeys(order_ids) if ORDER_ID_RE.match(o or "")]
    if not ids or not config.METABASE_URL or not config.METABASE_API_KEY:
        return {}

    in_list = ", ".join(f"'{o}'" for o in ids)  # safe: every id matched ORDER_ID_RE
    payload = {
        "database": config.METABASE_DB_ID,
        "type": "native",
        "native": {"query": SQL % in_list},
    }
    try:
        r = await client.post(
            f"{config.metabase_base()}/api/dataset",
            headers={"x-api-key": config.METABASE_API_KEY, "Content-Type": "application/json"},
            json=payload,
            timeout=45.0,
        )
        r.raise_for_status()
        data = r.json()
    except httpx.HTTPError as e:
        log.warning("metabase lookup failed: %s", e)
        return {}

    if data.get("status") == "failed" or data.get("error"):
        log.warning("metabase query error: %s", str(data.get("error"))[:300])
        return {}

    cols = [c["name"] for c in data["data"]["cols"]]
    out: dict[str, dict] = {}
    for row in data["data"]["rows"]:
        rec = dict(zip(cols, row))
        any_instant = rec.get("any_instant")
        n_stores = rec.get("n_stores") or 0
        out[rec["order_id"]] = {
            "store": _norm_store(rec.get("store_name")),
            "fulfillment": None if any_instant is None else ("instant" if any_instant else "deferred"),
            "n_batches": rec.get("n_batches"),
            # ~19.5% of orders are picked across both dark stores. This is the
            # nearest live equivalent of the legacy "clubbed" bucket, which is
            # itself not reproducible (milk_run_id is entirely NULL now).
            "is_cross_store": n_stores > 1 if n_stores else None,
        }
    return out
