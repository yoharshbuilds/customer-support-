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

from . import config, freshchat, issues

log = logging.getLogger(__name__)

MODEL = config.ANALYZE_MODEL
MAX_MESSAGES = 120  # long threads get truncated from the middle, keeping both ends

SYSTEM = """You review resolved customer-support chats for Plazza, an Indian \
online pharmacy that delivers medicines from two Bangalore dark stores \
(Yemalur and Bellandur).

You are given one conversation that an agent has just marked resolved. Judge \
it from the conversation alone. You are deliberately given no category tag, no \
SLA verdict, and no system events -- form your own view from what was said.

Lines marked "internal note (staff only, customer never saw this)" are notes \
between staff. They are evidence of what the agent did behind the scenes, but \
they were NEVER shown to the customer. Never treat a note as something the \
customer was told.

Definitions:

- `issue`: what the customer's problem actually was. Agents tag in a hurry and \
default to generic categories; ignore how it might have been labelled.

- `issue_category`: the same judgement, expressed in the dashboard's own \
taxonomy. Pick exactly one, by the customer's PRIMARY reason for making \
contact, not by how it was resolved. A random-sample audit of this taxonomy \
(reading full transcripts, not keyword-matching) found ~33% of tickets \
misclassified, concentrated in five specific confusions. The category \
definitions below have been sharpened against those exact failures -- read \
the parenthetical warnings, they are not decoration:

  * late_delivery - AN ORDER ALREADY EXISTS and the customer is chasing its \
progress: "where is my order", no rider assigned, stuck packing, out for \
delivery but not arriving, wants a firm ETA. THIS IS THE DEFAULT for any \
placed-order status check, even when the reveal is stock, Rx, or a \
cancellation, and even when the customer reacts with anger or a cancel demand \
once told -- unless the rest of the conversation decisively moves onto a \
different concrete ask (a real refund dispute with an ARN/RRN being chased, an \
address/phone/slot change, etc). (Audited failure: tickets opening as a plain \
status check were wrongly filed under cancel_request, prescription, \
availability, or unclassified just because the ANSWER mentioned Rx, stock, or \
a cancellation -- the customer's OPENING ask, not the agent's reveal, decides \
this.)
  * courtesy - no substantive issue: a thank-you, "ok", an emoji, a bare rating \
digit, a reply to a closing script, or a chat with NO customer message stating \
any concern at all -- including a chat where the customer only greeted, named \
a cart, or went silent and the agent closed it for non-response. (Audited \
failure: chats with zero real customer content were wrongly filed as \
unclassified; if there is no stated concern, it is courtesy, never \
unclassified.)
  * prescription - the Rx step itself: the doctor has not called, validation is \
slow, the customer missed the call, upload failed, they ask how approval works, \
OR Plazza already cancelled the order for a prescription reason (rejected, \
expired, missing doctor details, restricted-item rules) AND the customer does \
not dispute the refund -- they are asking about, or working through, the Rx \
problem itself (revalidation call, fresh prescription for a reorder, why it \
was rejected). The cancellation is the event; the Rx problem is the topic. If \
the customer instead pivots to chasing the refund amount or timing, use \
return_refund instead (see there).
  * cancel_request - ONLY when the CUSTOMER is the one asking Plazza to cancel \
an order that has not yet been cancelled -- "please cancel my order", "I want \
to cancel", stated BEFORE any cancellation happened -- plus the refund-timing \
follow-up on THEIR OWN cancellation. Do not use this just because the word \
"cancel" appears: a ticket where PLAZZA already cancelled the order (for \
stock, Rx, or any reason) and the customer is asking why, or disputing it, is \
NOT cancel_request -- classify by what they are actually asking about instead \
(late_delivery if they're really asking about the wait/status, prescription \
if about the Rx reason, return_refund if about the money). (Audited failure: \
this was the single most misclassified category -- most of its false positives \
were Plazza-initiated cancellations that a keyword match mistook for a \
customer request.)
  * missing_items - an item is GENUINELY ABSENT at the time of contact: shown \
as delivered but never handed over, a short quantity (ordered 3, got 2), a \
missing accessory, with no ETA still pending for it. Also use this when Plazza \
cancelled a specific item for unavailability and the customer experienced it \
as something missing. Do NOT use this if (a) the item is still out for \
delivery or awaiting redispatch -- that is late_delivery even if the customer \
calls it "missing" -- or (b) the customer received something, but it is the \
WRONG medicine, brand, or strength, or arrived damaged/expired -- that is \
wrong_damaged, regardless of whether they describe the mismatch using the \
word "missing".
  * order_change - amend a placed order rather than cancel it: change address \
or phone, add items, merge shipments, change the slot. If the order does not \
exist yet for the cart in question, this does not apply -- use unclassified.
  * payment_app - platform-side trouble: cannot log in or place an order, \
errors, money debited with no order created, coupon or referral failures, COD \
requests, invoice requests or corrections.
  * return_refund - any refund the customer is disputing or chasing: a split \
payment, a partial amount, timing, an ARN/reference number, a \
consumer-complaint threat -- OR a post-delivery return/exchange of goods that \
arrived correctly and undamaged. Use this whenever the conversation's \
substance is the money, even if an order Plazza cancelled (for any reason, \
including prescription or unavailability) is what triggered the refund -- the \
cancellation is backstory the customer already accepted; what they are \
actually pushing on is the refund itself. Do not use this for a delivered item \
that arrived damaged or wrong with no refund dispute in play -- that is \
wrong_damaged.
  * wrong_damaged - the customer RECEIVED something, but it is not what was \
ordered (wrong medicine, brand, or strength) or it arrived damaged, leaking, \
or expired. The item is present, just not right -- contrast with \
missing_items, where nothing was handed over at all.
  * availability - a PRE-ORDER question with NO existing order behind it: is \
this medicine stocked or permitted, what substitutes exist, do you deliver to \
my pincode. If the customer already has a placed order and is asking about \
ITS delivery or stock status, that is late_delivery, not availability, even \
though stock is literally what the agent says caused the delay. (Audited \
failure: this was the second most common confusion -- an existing order being \
chased was repeatedly mistaken for a pre-order stock question because stock \
was mentioned.)
  * unclassified - a LAST RESORT. Before choosing it, check it does not \
actually fit courtesy (no concern stated), late_delivery (a status check), or \
return_refund (a refund chase) -- those three absorbed most of what was \
previously miscategorised here. Reserve unclassified for genuine novelty: a \
vendor/business/job inquiry, a customer-care-number request with no order \
context, a question about medicine identity or authenticity, something that \
is a real ask but plainly outside the other ten. If in doubt between \
unclassified and a real category, pick the real category.

There is deliberately no separate category for "Plazza cancelled my order" on \
its own. A cancellation is an event, not a topic -- decide by what the \
customer is actually pushing on: the Rx problem (prescription), the money \
(return_refund), a specific absent item (missing_items), or something else \
entirely. Do not invent a bucket for the cancellation itself.

- `outcome`: did the customer leave with their problem solved?
  * resolved - the problem was actually fixed or fully answered.
  * partially_resolved - part of it was handled, part left open.
  * deflected - closed without solving: told to wait, to reorder, to consult \
someone, or handed off with no outcome reached in this chat.
  * no_customer_response - the customer stopped replying before anything was \
resolved.

- `answered_the_ask`: did the agent address what the customer actually asked, \
as opposed to something adjacent? A customer asking "when will it arrive" who \
is told "it is in the picking stage" was NOT given a time.

- `commitment`: the most specific promise the agent made about what happens \
next. "before 11:20 PM" or "by end of day" is specific_time. "will be updated \
in the app", "shortly", "on priority" is vague. No forward-looking statement at \
all is none.

- `empathy`:
  * specific_to_stake - acknowledged the particular thing this customer said \
was at risk (needing medicine urgently, waiting up at night, a missed dose).
  * scripted - polite apology language only, interchangeable between chats.
  * absent - no acknowledgement of the customer's position.

- `proactive`: things the agent did beyond answering the immediate question. \
Include only what the transcript evidences. Empty list if none.
  * chased_internally - contacted another team/person to get an answer \
(internal notes are valid evidence of this).
  * verified_outcome - checked back and confirmed the thing actually happened.
  * offered_alternative - proposed another route when the first failed.
  * followed_up_unprompted - returned with an update the customer did not ask for.

- `sentiment_start` / `sentiment_end`: the customer's mood in their opening \
messages, and in their final ones. If the customer never wrote enough to tell, \
use neutral.

- `escalation_risk`: true if the customer expressed anger, threatened to \
complain publicly or to leave, or repeated the same demand with rising urgency.
- `escalation_handled`: true only if such a moment occurred AND the agent \
defused it. False when there was risk and it was ignored. False when there was \
no risk.

- `escalated`: the agent handed the case to another team, store, or delivery \
partner rather than concluding it themselves.

- `order_ids`: copy CART-style ids verbatim from the transcript, any message. \
Empty list if none. Never invent or complete a partial id.

- `root_cause`: why this contact happened at all, in one factual sentence. \
Describe the underlying operational cause, not the customer's phrasing.
- `summary`: one sentence on what happened and how it ended.

Report only what the transcript supports. Where genuinely unclear, choose the \
weakest option ("other", "none", "absent") rather than guessing generously."""

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
        # The dashboard's taxonomy. Kept alongside `issue` rather than replacing
        # it: `issue` still backs the existing filters and the historical rows,
        # and the two are cross-checkable.
        "issue_category": {"type": "string", "enum": list(issues.MODEL_CATEGORIES)},
        "outcome": {
            "type": "string",
            "enum": ["resolved", "partially_resolved", "deflected", "no_customer_response"],
        },
        "answered_the_ask": {"type": "string", "enum": ["fully", "partly", "no"]},
        "commitment": {"type": "string", "enum": ["specific_time", "vague", "none"]},
        "empathy": {"type": "string", "enum": ["specific_to_stake", "scripted", "absent"]},
        "proactive": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["chased_internally", "verified_outcome",
                         "offered_alternative", "followed_up_unprompted"],
            },
        },
        "sentiment_start": {"type": "string", "enum": ["positive", "neutral", "negative"]},
        "sentiment_end": {"type": "string", "enum": ["positive", "neutral", "negative"]},
        "escalation_risk": {"type": "boolean"},
        "escalation_handled": {"type": "boolean"},
        "escalated": {"type": "boolean"},
        "order_ids": {"type": "array", "items": {"type": "string"}},
        "root_cause": {"type": "string"},
        "summary": {"type": "string"},
    },
    "required": [
        "issue",
        "issue_category",
        "outcome",
        "answered_the_ask",
        "commitment",
        "empathy",
        "proactive",
        "sentiment_start",
        "sentiment_end",
        "escalation_risk",
        "escalation_handled",
        "escalated",
        "order_ids",
        "root_cause",
        "summary",
    ],
    "additionalProperties": False,
}


def render_transcript(messages: list[dict]) -> str:
    """Flatten Freshchat messages (oldest first) into a readable transcript.

    Two deliberate exclusions, so the model judges the conversation and nothing
    else:

    * **System lines are dropped entirely.** They carry Freshchat's own
      verdicts -- ``"SLA Policy" Response SLA breached``, ``marked resolved
      by`` -- and leaving them in invites the model to restate Freshchat's
      opinion instead of forming its own. Everything factual in them
      (handoffs, SLA flags, timings) is computed in ``conversation_metrics``.
    * **Private notes are labelled, not disguised.** They arrive as
      ``actor_type='agent'`` and used to render as ordinary replies, so the
      model read staff-only notes as things the customer was told.

    The agent's own ``cf_type`` tag is never passed in at all.
    """
    lines: list[str] = []
    for m in messages:
        if freshchat.is_system(m):
            continue
        parts = []
        for p in m.get("message_parts") or []:
            if "text" in p:
                parts.append(((p["text"] or {}).get("content") or "").strip())
            elif "image" in p:
                parts.append("[image attached]")
            elif "file" in p:
                parts.append("[file attached]")
        body = " ".join(x for x in parts if x)
        if not body:
            continue

        if freshchat.is_private_note(m):
            who = "internal note (staff only, customer never saw this)"
        elif m.get("actor_type") == "user":
            who = "customer"
        elif m.get("actor_type") == "bot":
            who = "automated bot"
        else:
            who = "agent"
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
            # NOTE: do not add `temperature` here. Sonnet 5 rejects it with a
            # 400 ("`temperature` is deprecated for this model"), and because
            # analyze() swallows errors that fails silently -- every llm_*
            # column comes back NULL while enrichment still reports success.
            # Run-to-run stability comes from the enum schema and from the
            # explicit definitions in SYSTEM instead.
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
