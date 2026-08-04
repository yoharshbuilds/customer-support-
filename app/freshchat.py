"""Thin Freshchat v2 client — only the calls the enrichment path needs."""
from __future__ import annotations
import hashlib
import logging
import re
from datetime import datetime

import httpx

from . import config

log = logging.getLogger(__name__)

# The app deep-links customers in with a prefilled first message:
#   "Hi, I need help with my Plazza CART-161538"
# That prefill is the only place an order id appears, so we regex it out.
ORDER_RE = re.compile(r"\bCART-(\d{3,})\b", re.IGNORECASE)

_agent_cache: dict[str, str] = {}
_user_cache: dict[str, str | None] = {}


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {config.FRESHCHAT_API_TOKEN}",
        "Accept": "application/json",
    }


async def _get(client: httpx.AsyncClient, path: str, **params):
    r = await client.get(
        f"{config.freshchat_base()}{path}", headers=_headers(), params=params or None, timeout=20.0
    )
    r.raise_for_status()
    return r.json()


async def post(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    """POST to Freshchat. The only write this service makes."""
    r = await client.post(
        f"{config.freshchat_base()}{path}", headers=_headers(), json=body, timeout=20.0
    )
    r.raise_for_status()
    return r.json() if r.content else {}


async def fetch_conversation(client: httpx.AsyncClient, conversation_id: str) -> dict:
    return await _get(client, f"/conversations/{conversation_id}")


PAGE_SIZE = 50
MAX_PAGES = 10  # 500 messages; longer threads are vanishingly rare


async def fetch_messages(client: httpx.AsyncClient, conversation_id: str) -> list[dict]:
    """Return every message in a conversation, OLDEST first.

    Freshchat returns messages NEWEST-first and paginates from there, so the
    deep-link prefill we care about is the very last message of the very last
    page. We must walk to the end rather than reading page 1 -- reading only
    page 1 silently loses the order id on any thread longer than PAGE_SIZE.
    """
    collected: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        try:
            data = await _get(
                client,
                f"/conversations/{conversation_id}/messages",
                items_per_page=PAGE_SIZE,
                page=page,
            )
        except httpx.HTTPError as e:
            log.warning("messages fetch failed for %s page %s: %s", conversation_id, page, e)
            break
        batch = data.get("messages") or []
        collected.extend(batch)
        if len(batch) < PAGE_SIZE:
            break  # short page == last page
    else:
        log.warning("hit MAX_PAGES on %s; older messages not read", conversation_id)

    return list(reversed(collected))


def find_order_id(messages: list[dict]) -> str | None:
    """First CART-<digits> in an oldest-first message list, or None."""
    for msg in messages:
        for part in msg.get("message_parts") or []:
            text = ((part.get("text") or {}).get("content")) or ""
            m = ORDER_RE.search(text)
            if m:
                return f"CART-{m.group(1)}"
    return None


SLA_RESPONSE = "response sla breached"
SLA_RESOLUTION = "resolution sla breached"


def _text_of(msg: dict) -> str:
    return " ".join(
        ((p.get("text") or {}).get("content") or "")
        for p in (msg.get("message_parts") or [])
    )


def is_system(m: dict) -> bool:
    return m.get("message_type") == "system"


def is_private_note(m: dict) -> bool:
    """Internal note. Visible to staff in Freshchat, never sent to the customer.

    These arrive as actor_type='agent' with message_type='private'. Treating
    them as replies inflates the agent's message count and, in 12% of chats we
    sampled, made a note the "first response" -- so first_response_secs was
    measuring how fast the agent typed a note to themselves.
    """
    return m.get("actor_type") == "agent" and m.get("message_type") == "private"


def is_public_agent(m: dict) -> bool:
    """An agent message the customer actually received."""
    return m.get("actor_type") == "agent" and m.get("message_type") == "normal"


def _ts(m: dict) -> datetime | None:
    raw = m.get("created_time")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


# Scripted closers. An agent sending these is performing the closing ritual,
# not answering anything, so they don't count as a substantive reply when
# deciding whether the customer ever agreed the chat was done.
_CLOSERS = (
    "is there anything else i can assist",
    "for further assistance, please feel free",
    "thank you for contacting plazza support",
    "are you still connected",
    "as we have not received a response",
)


def _is_closer(body: str) -> bool:
    low = body.lower()
    return any(c in low for c in _CLOSERS)


def conversation_metrics(messages: list[dict], created: datetime | None,
                         customer_name: str | None = None) -> dict:
    """Derive per-conversation metrics from the transcript we already fetched.

    Costs no extra API calls, and deliberately covers everything that is a
    matter of fact rather than judgement -- timings, handoffs, repetition,
    whether the customer ever agreed the chat was finished. The LLM is left to
    judge only what cannot be counted.
    """
    first_agent_at: datetime | None = None
    n_user = n_agent = n_bot = n_private = 0
    sla_response = sla_resolution = False
    handoffs = 0
    agent_inactive = False

    last_user_at: datetime | None = None
    last_user_overall: datetime | None = None
    last_substantive_at: datetime | None = None
    max_dead_air = 0
    public_bodies: list[str] = []

    for m in messages:
        body = _text_of(m)
        low = body.lower()
        at = _ts(m)

        if is_system(m):
            if SLA_RESPONSE in low:
                sla_response = True
            elif SLA_RESOLUTION in low:
                sla_resolution = True
            if "assigned to" in low and "group" not in low:
                # Counts every agent assignment, including the opening
                # IntelliAssign. The first one is routing, not a handoff --
                # subtracted below so an untransferred chat reads 0.
                handoffs += 1
            if "inactive" in low and "unassigned" in low:
                agent_inactive = True
            continue

        if is_private_note(m):
            n_private += 1
            # A note can also record a handoff ("Hand overed to Sthuthi").
            if "unassigned" in low and "inactive" in low:
                agent_inactive = True
            continue

        actor = m.get("actor_type")
        if actor == "user":
            n_user += 1
            if at:
                last_user_at = at
                last_user_overall = at
        elif actor == "bot":
            n_bot += 1
        elif is_public_agent(m):
            n_agent += 1
            public_bodies.append(body.strip())
            if first_agent_at is None and at:
                first_agent_at = at
            # Dead air: how long the customer waited for a real reply.
            if at and last_user_at and at >= last_user_at:
                max_dead_air = max(max_dead_air, int((at - last_user_at).total_seconds()))
                last_user_at = None
            if at and not _is_closer(body):
                last_substantive_at = at

    first_response_secs = None
    if created and first_agent_at and first_agent_at >= created:
        first_response_secs = int((first_agent_at - created).total_seconds())

    # Same canned paragraph sent more than once in one chat -- the agent pasted
    # instead of adapting. Short acknowledgements are excluded.
    counts: dict[str, int] = {}
    for b in public_bodies:
        if len(b) >= 60:
            counts[b] = counts.get(b, 0) + 1
    repeated_canned = max(counts.values()) if counts else 1

    # Did the customer ever respond after the agent's last real answer? If not,
    # the chat was closed on the agent's schedule, not the customer's.
    closed_without_confirmation = bool(
        last_substantive_at and (last_user_overall is None or last_user_overall < last_substantive_at)
    )

    # Name is used transiently and never stored -- only this boolean survives.
    personalised = False
    if customer_name:
        first = customer_name.strip().split()[0] if customer_name.strip() else ""
        if len(first) >= 3:
            personalised = any(first.lower() in b.lower() for b in public_bodies)

    return {
        "first_response_secs": first_response_secs,
        "msg_total": len(messages),
        "msg_user": n_user,
        "msg_agent": n_agent,
        "msg_bot": n_bot,
        "msg_private": n_private,
        "sla_response_breached": sla_response,
        "sla_resolution_breached": sla_resolution,
        "max_dead_air_secs": max_dead_air or None,
        "handoffs": max(0, handoffs - 1),
        "agent_inactive": agent_inactive,
        "repeated_canned": repeated_canned,
        "closed_without_confirmation": closed_without_confirmation,
        "personalised": personalised,
    }


def customer_ref(phone: str | None, user_id: str | None) -> str | None:
    """Salted digest identifying a customer WITHOUT storing who they are.

    Phone is preferred over user_id because the same person can end up with
    more than one Freshchat user record across channels. The raw value never
    leaves this function -- only the digest is persisted, so the database
    holds no contact details while still supporting "unique customers".
    """
    basis = (phone or "").strip() or (user_id or "").strip()
    if not basis:
        return None
    salt = config.CUSTOMER_HASH_SALT
    if not salt:
        log.warning("CUSTOMER_HASH_SALT unset — customer dedupe disabled")
        return None
    return hashlib.sha256(f"{salt}:{basis}".encode()).hexdigest()[:32]


async def fetch_customer(client: httpx.AsyncClient, user_id: str | None) -> tuple[str | None, str | None]:
    """Return (salted digest, first name).

    The name is returned for transient use only -- it is needed to test whether
    the agent addressed the customer by name, and the caller keeps just that
    boolean. Nothing here is ever written to the database.
    """
    if not user_id:
        return None, None
    phone = name = None
    try:
        u = await _get(client, f"/users/{user_id}")
        phone = u.get("phone")
        name = (u.get("first_name") or "").strip() or None
    except httpx.HTTPError as e:
        log.warning("user fetch failed for %s: %s", user_id, e)
    if user_id in _user_cache:
        return _user_cache[user_id], name
    ref = customer_ref(phone, user_id)
    _user_cache[user_id] = ref
    return ref, name


async def fetch_customer_ref(client: httpx.AsyncClient, user_id: str | None) -> str | None:
    """Look the customer up and immediately reduce them to a digest."""
    if user_id and user_id in _user_cache:
        return _user_cache[user_id]
    ref, _ = await fetch_customer(client, user_id)
    return ref


async def fetch_agent_name(client: httpx.AsyncClient, agent_id: str | None) -> str | None:
    if not agent_id:
        return None
    if agent_id in _agent_cache:
        return _agent_cache[agent_id]
    try:
        a = await _get(client, f"/agents/{agent_id}")
    except httpx.HTTPError as e:
        log.warning("agent fetch failed for %s: %s", agent_id, e)
        return None
    name = " ".join(x for x in [a.get("first_name"), a.get("last_name")] if x).strip() or a.get("email")
    if name:
        _agent_cache[agent_id] = name
    return name
