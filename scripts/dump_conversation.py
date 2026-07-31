"""Dump a FULL conversation from Freshchat: metadata + every message, oldest first.

Usage: python -m scripts.dump_conversation <conversation_id> [--json]
"""
from __future__ import annotations
import asyncio
import json
import sys

import httpx

from app import config, freshchat

PAGE_SIZE = 50
MAX_PAGES = 40  # 2000 messages


async def fetch_all_messages(client: httpx.AsyncClient, cid: str) -> list[dict]:
    """Freshchat pages newest-first; collect every page, return oldest-first."""
    out: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        r = await client.get(
            f"{config.freshchat_base()}/conversations/{cid}/messages",
            headers={"Authorization": f"Bearer {config.FRESHCHAT_API_TOKEN}"},
            params={"items_per_page": PAGE_SIZE, "page": page},
            timeout=30.0,
        )
        r.raise_for_status()
        batch = r.json().get("messages") or []
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
    return list(reversed(out))


def text_of(msg: dict) -> str:
    parts = []
    for p in msg.get("message_parts") or []:
        if "text" in p:
            parts.append((p["text"] or {}).get("content") or "")
        elif "image" in p:
            parts.append(f"[image: {(p['image'] or {}).get('url','')[:60]}]")
        elif "file" in p:
            parts.append(f"[file: {(p['file'] or {}).get('name','')}]")
        elif "collection" in p:
            parts.append("[rich/collection part]")
    return " ".join(x for x in parts if x)


async def main(cid: str, as_json: bool) -> None:
    async with httpx.AsyncClient() as client:
        conv = await freshchat.fetch_conversation(client, cid)
        msgs = await fetch_all_messages(client, cid)

    if as_json:
        print(json.dumps({"conversation": conv, "messages": msgs}, indent=2))
        return

    print(f"conversation : {cid}")
    print(f"status       : {conv.get('status')}")
    print(f"properties   : {conv.get('properties')}")
    print(f"created      : {conv.get('created_time')}")
    print(f"updated      : {conv.get('updated_time')}")
    print(f"agent        : {conv.get('assigned_agent_id')}")
    print(f"messages     : {len(msgs)}")
    print("-" * 78)
    for m in msgs:
        who = m.get("actor_type", "?")
        kind = m.get("message_type", "")
        ts = (m.get("created_time") or "")[:19]
        tag = f"{who}/{kind}" if kind == "system" else who
        body = text_of(m).replace("\n", " ")
        print(f"[{ts}] {tag:>14} | {body[:150]}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    asyncio.run(main(sys.argv[1], "--json" in sys.argv))
