"""LLM pass over a resolved chat transcript.

Extracts what the structured Freshchat fields cannot give us:
  * a trustworthy issue category (agent-picked ``cf_type`` is noisy — the single
    biggest bucket in the historical export was "General - Others")
  * whether the ticket was actually resolved or just closed
  * customer sentiment at the end of the chat
  * order ids the deep-link regex missed, which lifts store/fulfillment coverage

Runs after the cheap enrichment, in the same background task, so a slow or
failed model call never blocks the webhook response.
"""
from __future__ import annotations

import json
import logging

from anthropic import AsyncAnthropic

from . import config

log = logging.getLogger(__name__)

MODEL = config.ANALYZE_MODEL
MAX_MESSAGES = 120  # long threads get truncated from the middle, keeping both ends

SYSTEM = """You review resolved customer-support chats for Plazza, an Indian \
online pharmacy that delivers medicines from two Bangalore dark stores \
(Yemalur and Bellandur).

You will be given the full transcript of one conversation that a support agent \
has just marked resolved. Read it and report what actually happened.

Guidance:
- `issue` is what the customer's problem really was, not what the agent tagged \
it as. Agents pick tags in a hurry and frequently default to a generic one.
- `resolution` distinguishes a real fix from a close. "deflected" means the \
agent closed without solving (told the customer to wait, to reorder, or handed \
off with no outcome). "no_customer_response" means the customer stopped \
replying before anything was resolved.
- `order_ids` must be copied verbatim from the transcript. Include every \
CART-style id you see, in any message. Empty list if there are none. Never \
invent or complete a partial id.
- `sentiment` is the customer's state at the END of the chat.
- `escalated` is true if the agent handed off to another team, store, or \
delivery partner rather than resolving it themselves.
- `root_cause` and `summary` are one short sentence each, plain factual English.

Report only what the transcript supports. If something is genuinely unclear, \
prefer the "unclear"/"other" option over guessing."""

SCHEMA = {
    "type": "object",
    "properties": {
        "issue": {
            "type": "string",
            "enum": [
                "order_delay",
                "delivery_issue",
                "order_cancellation",
                "order_modification",
                "return_or_replacement",
                "missing_or_wrong_item",
                "item_unavailable",
                "prescription",
                "refund",
                "payment",
                "app_or_technical",
                "discount_or_referral",
                "product_or_medicine_query",
                "other",
            ],
        },
        "resolution": {
            "type": "string",
            "enum": ["resolved", "partially_resolved", "deflected", "no_customer_response"],
        },
        "sentiment": {"type": "string", "enum": ["positive", "neutral", "negative"]},
        "escalated": {"type": "boolean"},
        "order_ids": {"type": "array", "items": {"type": "string"}},
        "root_cause": {"type": "string"},
        "summary": {"type": "string"},
    },
    "required": [
        "issue",
        "resolution",
        "sentiment",
        "escalated",
        "order_ids",
        "root_cause",
        "summary",
    ],
    "additionalProperties": False,
}


def render_transcript(messages: list[dict]) -> str:
    """Flatten Freshchat messages (oldest first) into a readable transcript."""
    lines: list[str] = []
    for m in messages:
        who = m.get("actor_type", "?")
        if m.get("message_type") == "system":
            who = "system"
        parts = []
        for p in m.get("message_parts") or []:
            if "text" in p:
                parts.append(((p["text"] or {}).get("content") or "").strip())
            elif "image" in p:
                parts.append("[image attached]")
            elif "file" in p:
                parts.append("[file attached]")
        body = " ".join(x for x in parts if x)
        if body:
            lines.append(f"[{(m.get('created_time') or '')[:19]}] {who}: {body}")

    if len(lines) > MAX_MESSAGES:
        head = MAX_MESSAGES // 2
        tail = MAX_MESSAGES - head
        lines = lines[:head] + [f"... [{len(lines) - MAX_MESSAGES} messages omitted] ..."] + lines[-tail:]
    return "\n".join(lines)


async def analyze(client: AsyncAnthropic, transcript: str) -> dict | None:
    """Return the structured review, or None if the model call fails."""
    if not transcript.strip():
        return None
    try:
        resp = await client.messages.create(
            model=MODEL,
            max_tokens=2000,
            # Classification, not prose. At the default temperature the same
            # transcript came back `order_cancellation` on one run and
            # `order_delay` on the next -- which would make the issue breakdown
            # shift under you on a re-run rather than reflecting real change.
            temperature=0,
            system=[{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}],
            output_config={"format": {"type": "json_schema", "schema": SCHEMA}},
            messages=[{"role": "user", "content": f"<transcript>\n{transcript}\n</transcript>"}],
        )
    except Exception as e:  # noqa: BLE001 — caller keeps the row and retries later
        log.warning("LLM analysis failed: %s", e)
        return None

    if resp.stop_reason == "refusal":
        log.warning("LLM refused to analyse transcript")
        return None

    text = next((b.text for b in resp.content if b.type == "text"), None)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        log.warning("LLM returned unparseable JSON")
        return None
