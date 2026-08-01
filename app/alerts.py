"""Slack alert for a resolve that the review says wasn't really a resolve.

Fires from the same background task that enriches the row, so it lands roughly
ten seconds after the agent clicks Resolve -- while the customer is often still
reachable. That is the point: this is a recovery window, not a QA report.

Only the webhook path alerts. The backfill script calls ``enrich`` directly and
never reaches here, so loading a historical day cannot fire hundreds of pings.
"""
from __future__ import annotations

import logging

import httpx

from . import config

log = logging.getLogger(__name__)

# Each rule: (id, human explanation, predicate). The explanation is what lands
# in Slack under "what went wrong", so it is written for a person skimming on a
# phone, not for a dashboard.
RULES = (
    (
        "no_reply",
        "agent resolved without sending the customer a single message",
        lambda f: (f.get("msg_agent") or 0) == 0,
    ),
    (
        "escalation_ignored",
        "customer escalated (anger, threat to complain) and it was not defused",
        lambda f: bool(f.get("llm_escalation_risk")) and not f.get("llm_escalation_handled"),
    ),
    (
        "deflected_unanswered",
        "closed without solving it, and the customer's actual question went unanswered",
        lambda f: f.get("llm_resolution") == "deflected" and f.get("llm_answered_ask") == "no",
    ),
    (
        "left_unhappy",
        "customer ended the chat unhappy and the problem was not resolved",
        lambda f: f.get("llm_sentiment") == "negative" and f.get("llm_resolution") != "resolved",
    ),
)


def evaluate(fields: dict) -> list[str]:
    """Return human-readable reasons this resolve is below the bar. Empty = fine."""
    # No review means no judgement -- stay quiet rather than alert on nulls.
    if not fields.get("llm_issue"):
        return []
    return [why for _, why, hit in RULES if hit(fields)]


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
        return
    try:
        r = await client.post(config.SLACK_WEBHOOK_URL, json=build_message(fields, reasons), timeout=15)
        r.raise_for_status()
        log.info("alerted on %s: %s", conversation_id, "; ".join(reasons))
    except Exception as e:  # noqa: BLE001 — a failed alert must never lose the row
        log.warning("slack alert failed for %s: %s", conversation_id, e)
