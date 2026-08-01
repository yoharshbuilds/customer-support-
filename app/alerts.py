"""Slack alert for a resolve that the review says wasn't really a resolve.

Fires from the same background task that enriches the row, so it lands roughly
ten seconds after the agent clicks Resolve -- while the customer is often still
reachable. That is the point: this is a recovery window, not a QA report.

Only the webhook path alerts. The backfill script calls ``enrich`` directly and
never reaches here, so loading a historical day cannot fire hundreds of pings.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

from . import config

log = logging.getLogger(__name__)

# Delivery tally. Whether a Slack message actually left the building is
# otherwise only visible in the host's logs, and "the rules would have fired"
# is not the same claim as "the customer-facing alert was delivered".
STATS = {"sent": 0, "failed": 0, "suppressed_below_bar": 0,
         "last_sent_at": None, "last_error": None}

# Each rule: (id, human explanation, predicate). The explanation is what lands
# in Slack under "what went wrong", so it is written for a person skimming on a
# phone, not for a dashboard.
RULES = (
    (
        "no_reply",
        "agent resolved without sending the customer a single message",
        # Requires that the customer actually wrote something. A chat where
        # nobody said anything is junk, not a failure of service.
        lambda f: (f.get("msg_agent") or 0) == 0 and (f.get("msg_user") or 0) > 0,
    ),
    (
        "escalation_ignored",
        "customer escalated (anger, threat to complain) and it was not defused",
        lambda f: bool(f.get("llm_escalation_risk"))
        and not f.get("llm_escalation_handled")
        and f.get("llm_resolution") != "resolved",
    ),
    (
        "deflected_unanswered",
        "closed without solving it, and the customer's actual question went unanswered",
        lambda f: f.get("llm_resolution") == "deflected" and f.get("llm_answered_ask") == "no",
    ),
    (
        "mood_deteriorated",
        "customer arrived calm and left unhappy",
        # Deterioration, not just a negative ending. Someone who arrives angry
        # about a genuine delay and leaves angry is an operations problem; a
        # customer the chat itself made unhappy is the one worth interrupting for.
        lambda f: f.get("llm_sentiment") == "negative" and f.get("llm_sentiment_start") != "negative",
    ),
)

# One failing signal describes ~30% of a normal day -- real, but far too much to
# interrupt anyone with. Two independent signals is ~10%, and those chats are
# unambiguously bad. `no_reply` pings on its own because there is no reading of
# it that is acceptable. Tunable from the environment without a redeploy.
ALWAYS_ALERT = {"no_reply"}


def signals(fields: dict) -> list[tuple[str, str]]:
    """Every rule this resolve trips, as (id, human explanation)."""
    if not fields.get("llm_issue"):
        return []  # no review means no judgement -- stay quiet rather than alert on nulls
    return [(rid, why) for rid, why, hit in RULES if hit(fields)]


def evaluate(fields: dict) -> list[str]:
    """Reasons worth interrupting someone for. Empty = don't ping."""
    hits = signals(fields)
    if not hits:
        return []
    severe = {rid for rid, _ in hits} & ALWAYS_ALERT
    if len(hits) >= config.ALERT_MIN_SIGNALS or severe:
        return [why for _, why in hits]
    return []


def _fmt_duration(secs) -> str:
    if not secs:
        return "—"
    secs = int(secs)
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


def build_message(fields: dict, reasons: list[str]) -> dict:
    order = fields.get("order_id") or "no CART id"
    where = " · ".join(x for x in (fields.get("store"), fields.get("fulfillment")) if x)
    agent = fields.get("resolver_agent_name") or fields.get("agent_name") or "unknown agent"
    issue = (fields.get("llm_issue") or "").replace("_", " ")
    outcome = (fields.get("llm_resolution") or "").replace("_", " ")

    lines = [f"• {r}" for r in reasons]
    detail = [
        f"*{order}*" + (f"  ·  {where}" if where else ""),
        f"Resolved by *{agent}* after {_fmt_duration(fields.get('resolution_secs'))}"
        f"  ·  {issue or 'unclassified'} → *{outcome or 'unknown'}*",
    ]
    if fields.get("max_dead_air_secs"):
        detail.append(f"Longest the customer waited mid-chat: {_fmt_duration(fields['max_dead_air_secs'])}")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "Poorly resolved ticket"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(detail)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*What went wrong*\n" + "\n".join(lines)}},
    ]
    if fields.get("llm_summary"):
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": f"*What happened*\n{fields['llm_summary']}"}})
    if fields.get("llm_root_cause"):
        blocks.append({"type": "context",
                       "elements": [{"type": "mrkdwn", "text": f"Root cause: {fields['llm_root_cause']}"}]})
    if fields.get("conversation_url"):
        blocks.append({"type": "actions", "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": "Open chat in Freshchat"},
            "url": fields["conversation_url"],
        }]})

    return {
        # Fallback text is what shows in the phone notification and sidebar.
        "text": f"Poorly resolved: {order} by {agent} — {reasons[0]}",
        "blocks": blocks,
    }


async def maybe_alert(client: httpx.AsyncClient, conversation_id: str, fields: dict) -> None:
    """Post to Slack when the review says this resolve was below the bar."""
    if not config.SLACK_WEBHOOK_URL:
        return
    reasons = evaluate(fields)
    if not reasons:
        if signals(fields):
            STATS["suppressed_below_bar"] += 1
        return
    try:
        r = await client.post(config.SLACK_WEBHOOK_URL, json=build_message(fields, reasons), timeout=15)
        r.raise_for_status()
        STATS["sent"] += 1
        STATS["last_sent_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log.info("alerted on %s: %s", conversation_id, "; ".join(reasons))
    except Exception as e:  # noqa: BLE001 — a failed alert must never lose the row
        STATS["failed"] += 1
        STATS["last_error"] = str(e)[:200]
        log.warning("slack alert failed for %s: %s", conversation_id, e)
