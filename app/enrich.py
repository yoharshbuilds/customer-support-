"""Turn a raw Freshchat resolve webhook into a fully attributed row.

Design note: the webhook payload shape is treated as untrusted/varying. We pull
``conversation_id`` out of it defensively, then re-fetch the conversation from
the Freshchat API for authoritative field values. Nothing downstream depends on
the payload's nesting.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone

import httpx
from anthropic import AsyncAnthropic

from . import analyze, db, freshchat, metabase

log = logging.getLogger(__name__)


def _walk(obj, key: str):
    """Depth-first search for the first non-empty value of `key`."""
    if isinstance(obj, dict):
        v = obj.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
        for sub in obj.values():
            found = _walk(sub, key)
            if found:
                return found
    elif isinstance(obj, list):
        for sub in obj:
            found = _walk(sub, key)
            if found:
                return found
    return None


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def extract_conversation_id(payload: dict) -> str | None:
    return _walk(payload, "conversation_id")


def is_resolution(payload: dict) -> bool:
    """True only when the payload is a conversation being marked resolved.

    Freshchat sends four actions: message_create, conversation_assignment,
    conversation_resolution, conversation_reopen. When `action` is present it is
    the ONLY signal we trust -- a `message_create` on an already-resolved
    conversation carries status="resolved" too, and treating that as a resolve
    would count every follow-up message as a new ticket closure.
    """
    action = (_walk(payload, "action") or "").lower()
    if action:
        return "resolution" in action or "resolved" in action

    # No `action` field at all (custom automation / unknown shape): fall back to
    # an explicit resolve container, then to a bare resolved status.
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("resolve"), dict):
        return True
    return (_walk(payload, "status") or "").lower() == "resolved"


def resolved_at_from(payload: dict) -> datetime:
    for key in ("action_time", "actioned_time", "updated_time", "timestamp"):
        ts = parse_ts(_walk(payload, key))
        if ts:
            return ts
    return datetime.now(timezone.utc)


async def _order_dim(client: httpx.AsyncClient, order_id: str) -> dict:
    cached = db.get_order_dim(order_id)
    if cached:
        return dict(cached)
    found = await metabase.lookup_orders(client, [order_id])
    dim = found.get(order_id) or {}
    if found:
        db.put_order_dim(order_id, dim)
    return dim


async def enrich(
    client: httpx.AsyncClient,
    conversation_id: str,
    resolved_at: datetime,
    llm: AsyncAnthropic | None = None,
    payload_actor: dict | None = None,
) -> dict:
    """Fetch conversation, order attribution, and (optionally) the LLM review.

    ``resolved_at`` is fixed by the caller at insert time and never revised here,
    so it stays stable as the row's identity alongside ``conversation_id``.
    """
    conv = await freshchat.fetch_conversation(client, conversation_id)
    props = conv.get("properties") or {}

    created = parse_ts(conv.get("created_time"))
    # A reopened-then-resolved conversation can report timestamps out of order;
    # never let that produce a negative duration.
    secs = (
        int((resolved_at - created).total_seconds())
        if created and resolved_at >= created
        else None
    )

    agent_id = conv.get("assigned_agent_id")
    agent_name = await freshchat.fetch_agent_name(client, agent_id)
    resolver_id = _walk(payload_actor, "actor_id") if payload_actor else None
    resolver_name = await freshchat.fetch_agent_name(client, resolver_id) if resolver_id else None
    cust_ref = await freshchat.fetch_customer_ref(client, conv.get("user_id"))

    # One fetch, two consumers: the order-id regex and the LLM transcript.
    messages = await freshchat.fetch_messages(client, conversation_id)
    order_id = freshchat.find_order_id(messages)

    metrics = freshchat.conversation_metrics(messages, created)
    review = await analyze.analyze(llm, analyze.render_transcript(messages)) if llm else None

    # The model reads the whole thread, so it catches order ids phrased in ways
    # the deep-link regex never sees. Only trust it as a fallback, and only for
    # ids that match the canonical format.
    if not order_id and review:
        for candidate in review.get("order_ids") or []:
            if metabase.ORDER_ID_RE.match((candidate or "").strip().upper()):
                order_id = candidate.strip().upper()
                break

    dim = await _order_dim(client, order_id) if order_id else {}

    return {
        "conv_created_at": created,
        "resolution_secs": secs,
        "agent_id": agent_id,
        "agent_name": agent_name,
        "issue_type": props.get("cf_type"),
        "priority": props.get("priority"),
        "order_id": order_id,
        "store": dim.get("store"),
        "fulfillment": dim.get("fulfillment"),
        "n_batches": dim.get("n_batches"),
        "is_cross_store": dim.get("is_cross_store"),
        "channel_id": conv.get("channel_id"),
        "group_id": conv.get("assigned_group_id"),
        # Deep link into Freshchat -- Option A: we link to the chat instead of
        # copying the transcript (and its health data) into our database.
        "conversation_url": conv.get("url"),
        "customer_ref": cust_ref,
        "resolver_agent_id": resolver_id,
        "resolver_agent_name": resolver_name,
        **metrics,
        # LLM review — all None when no key is configured or the call failed,
        # so the dashboard degrades to the structured fields rather than breaking.
        "llm_issue": (review or {}).get("issue"),
        "llm_resolution": (review or {}).get("resolution"),
        "llm_sentiment": (review or {}).get("sentiment"),
        "llm_escalated": (review or {}).get("escalated"),
        "llm_root_cause": (review or {}).get("root_cause"),
        "llm_summary": (review or {}).get("summary"),
    }
