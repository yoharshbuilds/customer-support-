"""MECE issue taxonomy: every ticket lands in exactly one real bucket.

Neither existing field works alone:

  * `issue_type` (the agent's tag) has 47 values with overlapping "... - Others"
    catch-alls. "General - Others" alone is the second largest tag, so using tags
    directly leaves a huge vague bucket.
  * `llm_issue` is cleaner (14 values) but has NULLs where the model call failed
    and an "other" bucket of its own.

So classification is a cascade, most specific evidence first:

  1. the agent's tag, when the tag names a real problem
  2. the LLM's classification, when the tag is one of the vague catch-alls
  3. the tag's FAMILY prefix ("Delivery Related - ...", "Medicine Related - ...")
     which still carries real information even when the suffix is "Others"

Every one of the 47 observed tags is mapped explicitly rather than by keyword
guessing, so a new tag shows up as UNMAPPED in the audit instead of being
silently absorbed. Buckets are mutually exclusive by construction (one bucket per
tag) and collectively exhaustive via the cascade.
"""
from __future__ import annotations

# Ordered: this is also the display order. Colours are chosen so the semantics
# read at a glance -- time problems amber, money green, destructive red, and the
# data-quality bucket deliberately drab so it never looks like a real category.
BUCKETS: list[dict] = [
    {"key": "delay",         "label": "Fulfilment delay",     "color": "#c2810a"},
    {"key": "prescription",  "label": "Prescription & Rx",    "color": "#8b5cd6"},
    {"key": "cancel_return", "label": "Cancellation & return", "color": "#c8452f"},
    {"key": "order_change",  "label": "Order changes",        "color": "#2d6cdf"},
    {"key": "item",          "label": "Item problems",        "color": "#0d9aa8"},
    {"key": "money",         "label": "Refunds & payments",   "color": "#1a9a5f"},
    {"key": "serviceability", "label": "Serviceability & partner", "color": "#d1568f"},
    {"key": "technical",     "label": "App & technical",      "color": "#5b6ee1"},
    {"key": "no_contact",    "label": "No customer contact",  "color": "#8b98a5"},
    {"key": "no_issue",      "label": "No issue raised",      "color": "#6b7280"},
    {"key": "unclassified",  "label": "Unclassified",         "color": "#3f4550"},
]
BUCKET_KEYS = {b["key"] for b in BUCKETS}
COLORS = {b["key"]: b["color"] for b in BUCKETS}
LABELS = {b["key"]: b["label"] for b in BUCKETS}

# Sentinel: the tag exists but names nothing specific, so defer to the LLM.
VAGUE = "__vague__"

TAG_MAP: dict[str, str] = {
    # --- fulfilment delay ---
    "Order Delay": "delay",
    "Delivery Delay": "delay",
    "ETA Query": "delay",
    "Order rescheduled": "delay",
    "Early Delivery Request": "delay",
    "Delivery Related - Early Delivery Request , Delivery instruction to delivery partner , Rapido (Third Party)": "delay",
    # --- prescription ---
    "Prescription Validation Query": "prescription",
    "Order cancelled due to prescription Invalid": "prescription",
    "Prescription Upload Issue": "prescription",
    "Prescription Rx - Others": "prescription",
    "Order Cancelled due to Restricted Item": "prescription",
    # --- cancellation & return ---
    "Order Cancellation Request": "cancel_return",
    "Order Return Request": "cancel_return",
    "Order Replacement Request": "cancel_return",
    # --- order changes ---
    "Address Change Request": "order_change",
    "Add Additional Item on Existing Order": "order_change",
    "Add alternative number at the time of delivery": "order_change",
    "Delivery Instruction": "order_change",
    "Wrong Address / Location Issue": "order_change",
    "COD Option": "order_change",
    # --- item problems ---
    "Item Unavailable": "item",
    "Missing Item in Order": "item",
    "Wrong Item Delivered": "item",
    "Partial Order Available": "item",
    "Item nearly Expiry": "item",
    "Marked as Delivered but Not Received by Customer": "item",
    "Medicine Related - Item Unavailable, Partial Available , Alternative Suggested , Medicines Suggested": "item",
    # --- money ---
    "Refund Status Check": "money",
    "Refund Not Received": "money",
    "Refund Clarification": "money",
    "Refund Related - Refund Follow-up , Refund status , Refund Clarification": "money",
    "Invoice Query": "money",
    "Discount / Coupon Query": "money",
    "Referral Query": "money",
    # --- serviceability & delivery partner ---
    "Non-Serviceable Area": "serviceability",
    "Delivery Partner Issue (Rapido)": "serviceability",
    # --- technical ---
    "Cart Not Updating": "technical",
    # --- no customer contact ---
    "No response from Customer": "no_contact",

    # --- vague: real tag, no real topic. The LLM decides these. ---
    "General - Others": VAGUE,
    "General - Call back request , Non serviceable area , Address Change , Others": VAGUE,
    "Order Related - Order on WhatsApp , Delay , Cancellation , Replacement , Return, ETA , Order Assistance": VAGUE,
    "Payment Related - Others": VAGUE,
    "Refund Related - Others": VAGUE,
    "Discount & Referral - Others": VAGUE,
    "App & Technical - Others": VAGUE,
    "Medicine Related - Others": VAGUE,
    "Delivery Related - Others": VAGUE,
}

LLM_MAP: dict[str, str] = {
    "order_delay": "delay",
    "delivery_issue": "delay",
    "prescription": "prescription",
    "order_cancellation": "cancel_return",
    "return_or_replacement": "cancel_return",
    "order_modification": "order_change",
    "missing_or_wrong_item": "item",
    "item_unavailable": "item",
    "refund": "money",
    "payment": "money",
    "discount_or_referral": "money",
    "app_or_technical": "technical",
    "product_or_medicine_query": "item",
    # "other" is deliberately absent: it is not evidence, so it falls through to
    # the family hint below.
}

# Last resort before Unclassified. A tag whose suffix is "Others" still names a
# family, and that family is real information.
FAMILY_MAP: list[tuple[str, str]] = [
    ("Prescription", "prescription"),
    ("Refund Related", "money"),
    ("Payment Related", "money"),
    ("Discount & Referral", "money"),
    ("App & Technical", "technical"),
    ("Medicine Related", "item"),
    ("Delivery Related", "delay"),
]


# Phrases in the LLM's root cause that name a real topic even when it classified
# the issue as "other". Checked before falling through to "no issue raised", so a
# genuine serviceability question is not filed as a non-issue.
ROOT_CAUSE_HINTS: list[tuple[tuple[str, ...], str]] = [
    (("serviceab", "operates in", "deliver to", "delivers to", "non-service"), "serviceability"),
    (("product image", "medicine", "product", "dosage", "expiry"), "item"),
    (("refund", "payment", "coupon", "discount", "invoice"), "money"),
    (("prescription", " rx "), "prescription"),
    (("delay", "eta", "late"), "delay"),
]

# Root causes that describe contact WITHOUT a support issue: a stray reply to a
# closed chat, a greeting, a thank-you that reopened a thread. These are the bulk
# of what was previously "Unclassified", and they are a real operational category
# -- roughly a tenth of tickets are threads nobody actually raised an issue in.
NO_ISSUE_HINTS = (
    "no actual issue", "never described", "without stating", "no discernible issue",
    "stray reply", "closing script", "closing message", "thank-you", "thank you",
    "only a greeting", "no substantive", "no customer-facing", "accidentally",
    "tail end of a prior", "previously resolved", "already-resolved", "reopened a previous",
    "no accompanying message", "did not engage",
)


def classify(issue_type: str | None, llm_issue: str | None,
             llm_root_cause: str | None = None) -> tuple[str, str]:
    """Return (bucket_key, which evidence decided it).

    Deterministic and total: always returns a bucket in BUCKET_KEYS.
    """
    tag = (issue_type or "").strip()
    llm = (llm_issue or "").strip().lower()

    mapped = TAG_MAP.get(tag)
    if mapped and mapped != VAGUE:
        return mapped, "agent_tag"

    if llm in LLM_MAP:
        return LLM_MAP[llm], "llm" if mapped == VAGUE else "llm_unmapped_tag"

    for prefix, bucket in FAMILY_MAP:
        if tag.startswith(prefix):
            return bucket, "tag_family"

    rc = (llm_root_cause or "").strip().lower()
    if rc:
        if any(h in rc for h in NO_ISSUE_HINTS):
            return "no_issue", "root_cause_no_issue"
        for needles, bucket in ROOT_CAUSE_HINTS:
            if any(n in rc for n in needles):
                return bucket, "root_cause"
        # The model read the transcript and found no category. With no tag and no
        # topical phrase either, "no issue raised" is the honest label -- not a
        # shrug.
        return "no_issue", "root_cause_residual"

    return "unclassified", "none"


def audit(rows: list[dict]) -> dict:
    """Distribution plus the data-quality residual, for a given set of rows.

    `rows` need only carry issue_type and llm_issue.
    """
    counts = {b["key"]: 0 for b in BUCKETS}
    by_evidence: dict[str, int] = {}
    unmapped_tags: dict[str, int] = {}
    for r in rows:
        key, ev = classify(r.get("issue_type"), r.get("llm_issue"),
                            r.get("llm_root_cause"))
        counts[key] += 1
        by_evidence[ev] = by_evidence.get(ev, 0) + 1
        tag = (r.get("issue_type") or "").strip()
        if tag and tag not in TAG_MAP:
            unmapped_tags[tag] = unmapped_tags.get(tag, 0) + 1
    total = sum(counts.values()) or 1
    return {
        "total": sum(counts.values()),
        "counts": counts,
        "share": {k: round(100.0 * v / total, 1) for k, v in counts.items()},
        "by_evidence": by_evidence,
        # A tag we have never mapped. Should be empty; if not, add it to TAG_MAP
        # rather than letting the cascade guess.
        "unmapped_tags": unmapped_tags,
        "buckets": BUCKETS,
    }
