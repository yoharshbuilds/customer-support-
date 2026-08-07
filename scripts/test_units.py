"""Pure-logic tests. No database, no network.

MECE layer 1 of 4. This file covers only functions that transform data in memory:
issue classification, order rate maths, extract-row parsing, dedupe keys, and
reconcile payload rebuilding. Session/webhook parsing live in their own files
(test_availability.py, test_webhook_parse.py); API contracts and data integrity
are layers 2 and 3.

Bias is toward inputs that actually break things: empty strings, None, whitespace,
wrong case, zero denominators, unmapped values, and unicode.

    python -m scripts.test_units
"""
from __future__ import annotations

from app import activity, issues, orders, reconcile

FAILS: list[str] = []
TOTAL = 0


def check(label: str, cond: bool) -> None:
    global TOTAL
    TOTAL += 1
    if not cond:
        FAILS.append(label)
    print(f"{'PASS' if cond else 'FAIL'}  {label}")


def section(name: str) -> None:
    print(f"\n--- {name} ---")


# ---------------------------------------------------------------- issues
def test_issues() -> None:
    section("issues.classify: the transcript category wins outright")
    for c in issues.CATEGORIES:
        got, ev = issues.classify(c["key"])
        check(f"category -> {c['key']}", got == c["key"] and ev == "transcript")
    check("case and whitespace tolerated",
          issues.classify("  LATE_DELIVERY ") == ("late_delivery", "transcript"))

    section("issues.classify: the agent tag is never consulted")
    # The old cascade read the tag first. Passing a tag-shaped string as the
    # category must NOT resolve -- it should fall through to the legacy bridge.
    check("a tag string is not a category",
          issues.classify("Order Delay", "refund")[0] == "return_refund")
    check("tag alone resolves to nothing",
          issues.classify("Order Delay", None, None)[0] == "unclassified")

    section("issues.classify: legacy rows bridge from the old enum")
    for legacy, want in issues.LEGACY_MAP.items():
        got, ev = issues.classify(None, legacy)
        check(f"legacy {legacy} -> {want}", got == want and ev == "legacy")
    check("legacy is case-insensitive",
          issues.classify(None, "ORDER_DELAY")[0] == "late_delivery")
    check("LEGACY_MAP deliberately excludes 'other'", "other" not in issues.LEGACY_MAP)

    section("issues.classify: unknown/degenerate inputs stay total")
    for bad in (None, "", "   ", "brand_new_category_2027", "\n\t"):
        got, _ = issues.classify(bad, None)
        check(f"unknown category {bad!r} -> valid key", got in issues.CATEGORY_KEYS)
    check("all None -> unclassified", issues.classify(None, None)[0] == "unclassified")

    section("issues.classify: root-cause fallback")
    check("no-issue phrase -> courtesy",
          issues.classify(None, "other",
                          "Customer sent only a thank you after a previously resolved chat")[0]
          == "courtesy")
    check("serviceability phrase read",
          issues.classify(None, "other",
                          "Customer asked whether Plazza delivers to Patna")[0] == "availability")
    check("unrecognised root cause still lands somewhere real",
          issues.classify(None, "other", "something entirely novel")[0]
          in issues.CATEGORY_KEYS)

    section("issues: MECE invariants")
    check("every key unique", len(issues.CATEGORY_KEYS) == len(issues.CATEGORIES))
    check("every category has a colour", all(c.get("color") for c in issues.CATEGORIES))
    check("colours unique", len({c["color"] for c in issues.CATEGORIES}) == len(issues.CATEGORIES))
    check("model vocabulary matches the keys",
          set(issues.MODEL_CATEGORIES) == issues.CATEGORY_KEYS)
    check("every LEGACY_MAP target is a real key",
          all(v in issues.CATEGORY_KEYS for v in issues.LEGACY_MAP.values()))
    check("every ROOT_CAUSE_HINTS target is a real key",
          all(v in issues.CATEGORY_KEYS for _, v in issues.ROOT_CAUSE_HINTS))
    check("charted + folded covers every category",
          set(issues.CHARTED) | set(issues.FOLDED) == issues.CATEGORY_KEYS)
    check("charted and folded do not overlap",
          not (set(issues.CHARTED) & set(issues.FOLDED)))
    check("nothing folds into an Other band any more",
          issues.FOLDED == [] and issues.CHARTED == [c["key"] for c in issues.CATEGORIES])

    section("issues.audit: exhaustive and mutually exclusive")
    rows = [
        {"llm_issue_category": "late_delivery"},
        {"llm_issue_category": None, "llm_issue": "refund"},
        {"llm_issue_category": None, "llm_issue": None},
        {"llm_issue_category": None, "llm_issue": "other",
         "llm_root_cause": "customer sent only a thank you"},
    ]
    a = issues.audit(rows)
    check("audit total equals input", a["total"] == len(rows))
    check("counts sum to total", sum(a["counts"].values()) == len(rows))
    check("shares sum to ~100", abs(sum(a["share"].values()) - 100.0) < 1.5)
    check("transcript_pct reported", a["transcript_pct"] == 25.0)
    check("audit on empty input does not divide by zero", issues.audit([])["total"] == 0)


# ---------------------------------------------------------------- orders
def test_orders() -> None:
    section("orders._rate: cancellation maths")
    r = orders._rate({"total_orders": 100, "fully_cancelled": 10, "partly_cancelled": 5,
                      "not_cancelled": 85})
    check("cancel_rate is fully/total", r["cancel_rate"] == 10.0)
    check("excl_fully_cancelled subtracts only fully", r["excl_fully_cancelled"] == 90)
    check("any_cancellation adds fully+partly", r["any_cancellation"] == 15)

    check("zero total does not divide by zero",
          orders._rate({"total_orders": 0, "fully_cancelled": 0, "partly_cancelled": 0})["cancel_rate"] == 0.0)
    check("missing keys treated as zero",
          orders._rate({})["cancel_rate"] == 0.0)
    check("all cancelled -> 100%",
          orders._rate({"total_orders": 7, "fully_cancelled": 7, "partly_cancelled": 0})["cancel_rate"] == 100.0)
    check("none cancelled -> 0% and excl == total",
          orders._rate({"total_orders": 7, "fully_cancelled": 0, "partly_cancelled": 0})
          == {"total_orders": 7, "fully_cancelled": 0, "partly_cancelled": 0,
              "cancel_rate": 0.0, "excl_fully_cancelled": 7, "any_cancellation": 0})
    check("None values coerced, not crashed",
          orders._rate({"total_orders": None, "fully_cancelled": None,
                        "partly_cancelled": None})["cancel_rate"] == 0.0)

    section("orders SQL: Redshift dialect only")
    for sql, name in ((orders.SQL_DAILY, "daily"), (orders.SQL_WEEKLY, "weekly")):
        low = sql.lower()
        check(f"{name}: no FILTER clause (unsupported on Redshift)", "filter (where" not in low)
        check(f"{name}: no AT TIME ZONE", "at time zone" not in low)
        check(f"{name}: no make_interval", "make_interval" not in low)
        check(f"{name}: uses CONVERT_TIMEZONE", "convert_timezone" in low)
        check(f"{name}: uses DATEADD", "dateadd" in low)
    check("weekly reports a day count so a partial week is visible",
          "days_in_bucket" in orders.SQL_WEEKLY)


# ---------------------------------------------------------------- activity
def test_activity() -> None:
    section("activity.rows_from_extract: only agent messages count as presence")
    raw = [
        {"actor_type": "AGENT", "actor_id": "a1", "actor_name": "A",
         "created_at": "2026-08-01T10:00:00.000Z", "conversation_id": "c1"},
        {"actor_type": "agent", "actor_id": "a2", "actor_name": "B",
         "created_at": "2026-08-01T10:00:01.000Z", "conversation_id": "c1"},
        {"actor_type": "USER", "actor_id": "u1", "created_at": "x", "conversation_id": "c1"},
        {"actor_type": "SYSTEM", "actor_id": "s1", "created_at": "x", "conversation_id": "c1"},
        {"actor_type": "AGENT", "actor_id": "", "created_at": "t", "conversation_id": "c1"},
        {"actor_type": "AGENT", "actor_id": "a3", "created_at": "", "conversation_id": "c1"},
        {},
    ]
    rows = activity.rows_from_extract(raw)
    check("keeps only agent rows with id and timestamp", len(rows) == 2)
    check("actor_type match is case-insensitive",
          {r["agent_id"] for r in rows} == {"a1", "a2"})
    check("blank agent name becomes None",
          activity.rows_from_extract([{"actor_type": "AGENT", "actor_id": "a", "actor_name": "  ",
                                       "created_at": "t", "conversation_id": "c"}])[0]["agent_name"] is None)
    check("source is tagged", all(r["source"] == "extract" for r in rows))
    check("empty input is empty output, not an error", activity.rows_from_extract([]) == [])

    section("activity dedupe key: stable, and sensitive to each field")
    k = activity._dedupe_key
    check("identical inputs -> identical key",
          k("a", "2026-08-01T10:00:00.000Z", "c") == k("a", "2026-08-01T10:00:00.000Z", "c"))
    check("different agent -> different key",
          k("a", "t", "c") != k("b", "t", "c"))
    check("millisecond difference -> different key",
          k("a", "2026-08-01T10:00:00.000Z", "c") != k("a", "2026-08-01T10:00:00.001Z", "c"))
    check("different conversation -> different key", k("a", "t", "c1") != k("a", "t", "c2"))
    check("unicode agent id does not crash", isinstance(k("ünïcødé", "t", "c"), str))

    section("activity.tickets_from_extract")
    t = activity.tickets_from_extract([
        {"conversation_id": "c1", "created_at": "2026-08-01T00:00:00.000Z",
         "user_id": "u1", "channel_name": "WhatsApp", "reopened": "false"},
        {"conversation_id": "c2", "created_at": "2026-08-01T00:00:00.000Z", "reopened": "true"},
        {"conversation_id": "", "created_at": "t"},
        {"conversation_id": "c3", "created_at": ""},
    ])
    check("drops rows missing id or timestamp", len(t) == 2)
    check("reopened parsed as bool", t[0]["reopened"] is False and t[1]["reopened"] is True)
    check("blank optional fields become None", t[1]["user_id"] is None)


# ---------------------------------------------------------------- reconcile
def test_reconcile() -> None:
    section("reconcile.payload_for: rebuilds a webhook body faithfully")
    p = reconcile.payload_for({"conversation_id": "c1", "created_at": "2026-08-01T10:00:00.000Z",
                               "actor_id": "a1", "actor_type": "AGENT"})
    check("action is a resolution", p["action"] == "conversation_resolution")
    check("conversation id carried",
          p["data"]["resolve"]["conversation"]["conversation_id"] == "c1")
    check("status is resolved", p["data"]["resolve"]["conversation"]["status"] == "resolved")
    check("actor type lowercased", p["actor"]["actor_type"] == "agent")
    check("provenance marked", p["_source"] == "reconcile")
    check("action_time is the resolve time", p["action_time"] == "2026-08-01T10:00:00.000Z")

    check("missing actor_id tolerated",
          reconcile.payload_for({"conversation_id": "c", "created_at": "t"})["actor"]["actor_id"] is None)
    check("agent_id used when actor_id absent",
          reconcile.payload_for({"conversation_id": "c", "created_at": "t",
                                 "agent_id": "g1"})["actor"]["actor_id"] == "g1")
    check("missing actor_type defaults to agent",
          reconcile.payload_for({"conversation_id": "c", "created_at": "t"})["actor"]["actor_type"] == "agent")

    section("reconcile: rebuilt payload survives the real parser")
    from app import enrich
    ts = enrich.resolved_at_from(p)
    check("resolved_at parses from the rebuilt payload",
          ts is not None and ts.isoformat().startswith("2026-08-01T10:00:00"))
    check("is_resolution accepts the rebuilt payload", enrich.is_resolution(p) is True)
    check("extract_conversation_id finds the id",
          enrich.extract_conversation_id(p) == "c1")


def main() -> int:
    test_issues()
    test_orders()
    test_activity()
    test_reconcile()
    print(f"\n{TOTAL - len(FAILS)}/{TOTAL} passed")
    if FAILS:
        print("\nFAILED:")
        for f in FAILS:
            print(f"  {f}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
