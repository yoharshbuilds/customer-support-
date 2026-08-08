"""Issue taxonomy read from the chat transcript, never from the agent's tag.

The previous version cascaded ``issue_type`` (the agent's tag) first and only
deferred to the model when the tag was one of the vague "... - Others"
catch-alls. That inherited the tag's problems: agents tag in a hurry, and
"General - Others" alone was the second largest tag in the window.

So the tag is now unused for classification. Every ticket is categorised from
what was actually said in the chat:

  1. ``llm_issue_category`` — analyze.py asks the model to pick exactly one of
     the categories below, straight from the transcript. This is the real source.
  2. a bridge from the older ``llm_issue`` enum, for rows written before that
     field existed. Reported as ``legacy`` evidence so the audit shows how much
     of the window is bridged rather than freshly classified.

The bridge is lossy in one known place and it is better to say so than to hide
it: the old enum's ``missing_or_wrong_item`` cannot distinguish a missing item
from a wrong or damaged one, so bridged rows all land in "Missing items in
delivery". Rows classified natively split the two correctly.

Buckets are mutually exclusive (one per ticket) and collectively exhaustive
(the cascade always returns a key). Derived from a 4,403-ticket read of the
30-day window; the ordering below is descending by observed volume, which is
also the display and stacking order.

Retired category: ``cancelled_by_us`` ("Order cancelled by Plazza") existed
briefly and was removed after actually reading all 18 of its tickets rather
than pattern-matching them. "The order was cancelled by Plazza" is an EVENT,
not something a customer contacts about as a topic in its own right -- every
one of those 18 conversations was really about something else that happened
to follow that event:
  * 10 were substantively about the prescription problem that caused it
    (doctor validation, re-upload, revalidation calls) -> prescription
  * 7 were substantively a refund dispute (a split payment, timing, an ARN,
    a consumer-complaint threat) where the cancellation was one line of
    backstory and the whole rest of the conversation was about the money
    -> return_refund
  * 1 was about a specific absent item, described by the customer as missing
    -> missing_items
No keyword or regex produced that split; it came from reading each transcript
and judging what the conversation was actually about. That is the standing
method for auditing this taxonomy, not an exception for this one category.

Every category gets its own hue -- nothing folds into an "Other" band. The
first eight are the validated categorical palette in its CVD-checked order;
the remaining three were picked and checked the same way (see
scripts/validate_palette.js in the dataviz skill) but sit past the
eight-colour ceiling that guarantees full separation, so two things follow:
  * they were chosen to clear CVD separation against their immediate NEIGHBOURS
    in this list specifically, because that is what touches in the stacked
    chart -- not against every other slot, which the palette guidance says is
    not achievable past eight and does not try to promise here.
  * `unclassified` is deliberately a low-chroma grey and would fail the
    palette's own chroma-floor check on its own terms. That is intentional: it
    is the genuine "could not classify" catch-all, not a real topic, and
    giving it a confident, vivid colour would overstate what it is. Every
    category is still named in the legend, table and tooltip, so identity
    never depends on the hue alone regardless of how many slots are in play.
"""
from __future__ import annotations

# Display and stack order: descending by observed volume.
CATEGORIES: list[dict] = [
    {"key": "late_delivery",  "label": "Late delivery & ETA chase",   "color": "#2a78d6"},
    {"key": "courtesy",       "label": "Courtesy & no-content",       "color": "#eb6834"},
    {"key": "prescription",   "label": "Prescription validation",     "color": "#1baf7a"},
    {"key": "cancel_request", "label": "Cancellation request",        "color": "#eda100"},
    {"key": "missing_items",  "label": "Missing items in delivery",   "color": "#e87ba4"},
    {"key": "order_change",   "label": "Order change request",        "color": "#008300"},
    {"key": "payment_app",    "label": "Payment, app & invoice",      "color": "#4a3aa7"},
    {"key": "return_refund",  "label": "Return & refund request",     "color": "#e34948"},
    {"key": "wrong_damaged",  "label": "Wrong or damaged item",       "color": "#0891b2"},
    {"key": "availability",   "label": "Availability & serviceability", "color": "#65a30d"},
    {"key": "unclassified",   "label": "Unclassified",                "color": "#78716c"},
]
CATEGORY_KEYS = {c["key"] for c in CATEGORIES}
COLORS = {c["key"]: c["color"] for c in CATEGORIES}
LABELS = {c["key"]: c["label"] for c in CATEGORIES}
# Historically the 9th+ slots folded into a chart "Other" band; that grouping
# is gone, so every category is "charted" and nothing is "folded". Both names
# are kept (rather than removed) because the frontend keys off them to decide
# whether to render the Other band at all -- an empty FOLDED is what makes it
# disappear without any template change.
CHARTED = [c["key"] for c in CATEGORIES]
FOLDED: list[str] = []

# Back-compat aliases: the module used to export these names.
BUCKETS = CATEGORIES
BUCKET_KEYS = CATEGORY_KEYS

# What analyze.py is allowed to return, and how it maps in. Keeping the model's
# vocabulary identical to the keys means a typo shows up as unclassified instead
# of being silently coerced.
MODEL_CATEGORIES = [c["key"] for c in CATEGORIES]

# Bridge for rows enriched before llm_issue_category existed.
LEGACY_MAP: dict[str, str] = {
    "order_delay": "late_delivery",
    "delivery_issue": "late_delivery",
    "order_cancellation": "cancel_request",
    "order_modification": "order_change",
    "return_or_replacement": "return_refund",
    "refund": "return_refund",
    # Lossy: the old enum merged missing with wrong/damaged. See module docstring.
    "missing_or_wrong_item": "missing_items",
    "item_unavailable": "availability",
    "product_or_medicine_query": "availability",
    "prescription": "prescription",
    "payment": "payment_app",
    "app_or_technical": "payment_app",
    "discount_or_referral": "payment_app",
    # "other" is deliberately absent — it is not evidence. It falls through to
    # the root-cause read below.
}

# Root causes describing contact with no support issue in it: a stray reply to a
# closed chat, a greeting, a thank-you that reopened the thread.
NO_ISSUE_HINTS = (
    "no actual issue", "never described", "without stating", "no discernible issue",
    "stray reply", "closing script", "closing message", "thank-you", "thank you",
    "only a greeting", "no substantive", "no customer-facing", "accidentally",
    "tail end of a prior", "previously resolved", "already-resolved", "reopened a previous",
    "no accompanying message", "did not engage",
)

# Topical phrases, checked only when nothing better is available.
ROOT_CAUSE_HINTS: list[tuple[tuple[str, ...], str]] = [
    (("serviceab", "operates in", "deliver to", "delivers to", "non-service"), "availability"),
    (("prescription", " rx "), "prescription"),
    (("refund", "payment", "coupon", "discount", "invoice"), "payment_app"),
    # No "cancelled by us" hint here on purpose -- see the retirement note in the
    # module docstring. A root cause mentioning ONLY the cancellation, with none
    # of the keywords above or below, is rare enough in practice (every one of
    # the 18 tickets that surfaced this gap also mentioned prescription or a
    # refund) that it is left to fall through to the residual "courtesy" read
    # rather than engineered for.
    (("damaged", "wrong item", "expired"), "wrong_damaged"),
    (("missing", "not handed", "short quantity"), "missing_items"),
    (("delay", "eta", "late"), "late_delivery"),
]


def classify(issue_category: str | None,
             llm_issue: str | None = None,
             llm_root_cause: str | None = None) -> tuple[str, str]:
    """Return (category_key, which evidence decided it).

    Deterministic and total: always returns a key in CATEGORY_KEYS.

    NOTE the first argument is the model's own category, NOT the agent tag. The
    tag is not consulted at all.
    """
    cat = (issue_category or "").strip().lower()
    if cat in CATEGORY_KEYS:
        return cat, "transcript"

    legacy = (llm_issue or "").strip().lower()
    if legacy in LEGACY_MAP:
        return LEGACY_MAP[legacy], "legacy"

    rc = (llm_root_cause or "").strip().lower()
    if rc:
        if any(h in rc for h in NO_ISSUE_HINTS):
            return "courtesy", "root_cause_no_issue"
        for needles, key in ROOT_CAUSE_HINTS:
            if any(n in rc for n in needles):
                return key, "root_cause"
        return "courtesy", "root_cause_residual"

    return "unclassified", "none"


def audit(rows: list[dict]) -> dict:
    """Distribution plus the data-quality residual for a set of rows.

    Rows need only carry llm_issue_category, llm_issue and llm_root_cause.
    """
    counts = {c["key"]: 0 for c in CATEGORIES}
    by_evidence: dict[str, int] = {}
    for r in rows:
        key, ev = classify(r.get("llm_issue_category"), r.get("llm_issue"),
                           r.get("llm_root_cause"))
        counts[key] += 1
        by_evidence[ev] = by_evidence.get(ev, 0) + 1
    total = sum(counts.values()) or 1
    return {
        "total": sum(counts.values()),
        "counts": counts,
        "share": {k: round(100.0 * v / total, 1) for k, v in counts.items()},
        "by_evidence": by_evidence,
        # How much of the window is freshly classified from the transcript
        # versus bridged from the old enum. Should trend to 100% transcript.
        "transcript_pct": round(100.0 * by_evidence.get("transcript", 0) / total, 1),
        "buckets": CATEGORIES,
        "charted": CHARTED,
        "folded": FOLDED,
        # Kept so the existing UI's audit line does not break; the tag is no
        # longer used for classification, so there is nothing to be unmapped.
        "unmapped_tags": {},
    }
