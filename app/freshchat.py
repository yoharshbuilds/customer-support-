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


def conversation_metrics(messages: list[dict], created: datetime | None) -> dict:
    """Derive per-conversation metrics from the transcript we already fetched.

    Costs no extra API calls. `first_response_secs` measures from conversation
    creation to the first *human* agent reply -- bot greetings and the system
    lines ("assigned to X by IntelliAssign") are excluded, since counting those
    would make every first response look instant.
    """
    first_agent_at: datetime | None = None
    n_user = n_agent = n_bot = 0
    sla_response = sla_resolution = False

    for m in messages:
        actor = m.get("actor_type")
        is_system = m.get("message_type") == "system"
        body = _text_of(m).lower()

        if is_system:
            if SLA_RESPONSE in body:
                sla_response = True
            elif SLA_RESOLUTION in body:
                sla_resolution = True
            continue

        if actor == "user":
            n_user += 1
        elif actor == "bot":
            n_bot += 1
        elif actor == "agent":
            n_agent += 1
            if first_agent_at is None:
                ts = m.get("created_time")
                if ts:
                    try:
                        first_agent_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except ValueError:
                        pass

    first_response_secs = None
    if created and first_agent_at and first_agent_at >= created:
        first_response_secs = int((first_agent_at - created).total_seconds())

    return {
        "first_response_secs": first_response_secs,
        "msg_total": len(messages),
        "msg_user": n_user,
        "msg_agent": n_agent,
        "msg_bot": n_bot,
        "sla_response_breached": sla_response,
        "sla_resolution_breached": sla_resolution,
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


async def fetch_customer_ref(client: httpx.AsyncClient, user_id: str | None) -> str | None:
    """Look the customer up and immediately reduce them to a digest."""
    if not user_id:
        return None
    if user_id in _user_cache:
        return _user_cache[user_id]
    phone = None
    try:
        u = await _get(client, f"/users/{user_id}")
        phone = u.get("phone")
    except httpx.HTTPError as e:
        log.warning("user fetch failed for %s: %s", user_id, e)
    ref = customer_ref(phone, user_id)
    _user_cache[user_id] = ref
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
