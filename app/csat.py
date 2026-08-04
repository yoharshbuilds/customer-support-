"""Our own post-resolution survey, sent into the conversation itself.

Freshchat's native CSAT is a two-step flow that loses people: of 21 customers
who answered "was your issue resolved?", only 9 went on to give a rating. Every
extra step costs responses, so this asks one question with one tap.

The three options map onto ``llm_resolution`` (resolved / partially_resolved /
deflected) on purpose. Each answer is therefore ground truth for the model's
judgement of the same chat -- the calibration we otherwise have no way to do.

Unlike Freshchat's survey this is an ordinary message in the thread, so the
customer's reply comes back as a ``message_create`` webhook we already receive.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

from . import config, db, freshchat

log = logging.getLogger(__name__)

STATS = {"sent": 0, "failed": 0, "skipped": 0, "responses": 0,
         "last_sent_at": None, "last_error": None}

PROMPT = "Thanks for contacting Plazza. How did we do?"

# WhatsApp allows at most three reply buttons, so a 1-5 scale cannot be
# buttons. Three options is the most this channel supports natively.
OPTIONS = (
    ("resolved", "😊 Solved it"),
    ("partially_resolved", "😐 Partly"),
    ("deflected", "😞 Not solved"),
)

FOLLOW_UP = "Sorry about that — could you tell us what went wrong?"

# Answer text -> our code. Matched against the customer's reply, so both the
# label and the bare wording count: WhatsApp echoes the button label, but a
# customer may also just type it.
_ANSWER_INDEX = {label.lower(): code for code, label in OPTIONS}
_ANSWER_INDEX.update({label.split(" ", 1)[1].lower(): code for code, label in OPTIONS})


def should_send(fields: dict) -> tuple[bool, str]:
    """Whether this resolve deserves a survey. Returns (send?, why not)."""
    outcome = fields.get("llm_resolution")
    if outcome is None:
        return False, "not analysed yet"
    # A customer who stopped replying has already left; surveying them produces
    # nothing and messages someone who disengaged. 46 of 126 resolves in a
    # sample day were this.
    if outcome == "no_customer_response":
        return False, "customer had already stopped replying"
    if (fields.get("msg_user") or 0) == 0:
        return False, "customer never wrote anything"
    return True, ""


def build_survey() -> dict:
    """Freshchat send-message body for the survey."""
    return {
        "actor_type": "bot",
        "message_type": "normal",
        "message_parts": [{"text": {"content": PROMPT}}],
        "reply_parts": [
            {
                "collection": {
                    "sub_parts": [
                        {"quick_reply_button": {"label": label, "custom_reply_text": label}}
                        for _, label in OPTIONS
                    ]
                }
            }
        ],
    }


def classify_reply(text: str) -> str | None:
    """Map a customer's reply back to a survey answer, or None if unrelated."""
    t = (text or "").strip().lower()
    if not t:
        return None
    if t in _ANSWER_INDEX:
        return _ANSWER_INDEX[t]
    # Emoji alone, or the label with punctuation.
    for code, label in OPTIONS:
        emoji, words = label.split(" ", 1)
        if t == emoji or t.startswith(words.lower()):
            return code
    return None


async def maybe_send(client: httpx.AsyncClient, conversation_id: str, fields: dict) -> None:
    """Send the survey into the conversation, if enabled and appropriate."""
    if not config.CSAT_ENABLED:
        return
    ok, why = should_send(fields)
    if not ok:
        STATS["skipped"] += 1
        log.info("csat skipped for %s: %s", conversation_id, why)
        return
    try:
        await freshchat.post(client, f"/conversations/{conversation_id}/messages", build_survey())
        db.mark_csat_sent(conversation_id)
        STATS["sent"] += 1
        STATS["last_sent_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log.info("csat sent for %s", conversation_id)
    except Exception as e:  # noqa: BLE001 — a failed survey must never lose the row
        STATS["failed"] += 1
        STATS["last_error"] = str(e)[:200]
        log.warning("csat send failed for %s: %s", conversation_id, e)


def record_reply(conversation_id: str, text: str) -> str | None:
    """Store a survey answer arriving on a message webhook. Returns the code."""
    code = classify_reply(text)
    if code is None:
        return None
    if db.record_csat_response(conversation_id, code, text[:500]):
        STATS["responses"] += 1
        log.info("csat response for %s: %s", conversation_id, code)
        return code
    return None
