"""Who is on the support team, from Freshchat.

The roster is what turns activity data into attendance: without it you only see
who worked, never who was expected and did not appear. Freshchat is the only
live source -- Metabase's `freshworks.freshchat_agents` mirror stopped syncing
on 2026-06-02 and lists 8 agents where the API returns 17.
"""
from __future__ import annotations
import logging
import time

import httpx

from . import config

log = logging.getLogger(__name__)

_cache: dict | None = None
_cache_at: float = 0.0
CACHE_SECONDS = 300


async def fetch(client: httpx.AsyncClient) -> list[dict]:
    """Active agents, newest roster wins. Cached briefly: the roster changes
    when someone is hired, not when a chat is resolved."""
    global _cache, _cache_at
    if _cache is not None and (time.monotonic() - _cache_at) < CACHE_SECONDS:
        return _cache["agents"]

    r = await client.get(
        f"{config.freshchat_base()}/agents",
        headers={"Authorization": f"Bearer {config.FRESHCHAT_API_TOKEN}"},
        params={"page": 1, "items_per_page": 100},
        timeout=30.0,
    )
    r.raise_for_status()
    agents = []
    for a in r.json().get("agents") or []:
        # Deactivated and deleted agents are ex-staff. They stay out of the
        # denominator, or every past hire counts as absent forever.
        if a.get("is_deactivated") or a.get("is_deleted"):
            continue
        name = f"{a.get('first_name', '')} {a.get('last_name', '')}".strip()
        agents.append({
            "id": a.get("id"),
            "name": name or a.get("email") or a.get("id"),
            "email": a.get("email"),
            "role": a.get("role_name"),
            "is_support": a.get("role_name") == config.SUPPORT_ROLE,
            # Current state, for context only. This is a snapshot -- Freshchat
            # exposes no availability history, so it cannot be used to
            # reconstruct attendance for any day but right now.
            "availability_status": a.get("availability_status"),
            "login_status": a.get("login_status"),
        })
    _cache = {"agents": agents}
    _cache_at = time.monotonic()
    return agents
