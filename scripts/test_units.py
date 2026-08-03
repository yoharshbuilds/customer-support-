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
    section("issues.classify: every tag maps, nothing silently absorbed")
    for tag, want in list(issues.TAG_MAP.items()):
        if want == issues.VAGUE:
            continue
        got, ev = issues.classify(tag, None)
        check(f"tag -> {want}: {tag[:44]}", got == want and ev == "agent_tag")

    section("issues.classify: vague tags defer to the LLM, not to a shrug")
    check("vague + specific llm uses llm",
          issues.classify("General - Others", "order_delay") == ("delay", "llm"))
    check("vague + llm 'other' + no root cause is not a real bucket",
          issues.classify("General - Others", "other")[0] in ("no_issue", "unclassified"))

    section("issues.classify: unknown/degenerate inputs stay total")
    for bad in (None, "", "   ", "Some Brand New Tag 2027", "\n\t"):
        got, _ = issues.classify(bad, None)
        check(f"unknown tag {bad!r} -> valid bucket", got in issues.BUCKET_KEYS)
    check("both None -> unclassified", issues.classify(None, None)[0] == "unclassified")
    check("llm value unknown falls through",
          issues.classify(None, "totally_new_label")[0] in issues.BUCKET_KEYS)
    check("llm case-insensitive", issues.classify(None, "ORDER_DELAY")[0] == "delay")
    check("llm whitespace tolerated", issues.classify(None, "  refund  ")[0] == "money")

    section("issues.classify: root-cause cascade")
    check("serviceability phrase wins over no-issue",
          issues.classify("General - Others", "other",
                          "Customer asked whether Plazza delivers to Patna")[0] == "serviceability")
    check("no-issue phrase detected",
          issues.classify("General - Others", "other",
                          "Customer sent only a thank you after a previously resolved chat")[0] == "no_issue")
    check("unrecognised root cause still lands somewhere real",
          issues.classify("General - Others", "other", "something entirely novel")[0]
          in issues.BUCKET_KEYS)

    section("issues: MECE invariants")
    check("every bucket has a unique key", len(issues.BUCKET_KEYS) == len(issues.BUCKETS))
    check("every bucket has a colour", all(b.get("color") for b in issues.BUCKETS))
    check("colours are unique", len({b["color"] for b in issues.BUCKETS}) == len(issues.BUCKETS))
    check("every TAG_MAP target is a real bucket or VAGUE",
          all(v == issues.VAGUE or v in issues.BUCKET_KEYS for v in issues.TAG_MAP.values()))
    check("every LLM_MAP target is a real bucket",
          all(v in issues.BUCKET_KEYS for v in issues.LLM_MAP.values()))
    check("LLM_MAP deliberately excludes 'other'", "other" not in issues.LLM_MAP)
    check("FAMILY_MAP targets are real buckets",
          all(v in issues.BUCKET_KEYS for _, v in issues.FAMILY_MAP))

    section("issues.audit: exhaustive and mutually exclusive")
    rows = [
        {"issue_type": "Order Delay", "llm_issue": None},
        {"issue_type": "General - Others", "llm_issue": "refund"},
        {"issue_type": None, "llm_issue": None},
        {"issue_type": "Brand New Tag", "llm_issue": "prescription"},
    ]
    a = issues.audit(rows)
    check("audit total equals input", a["total"] == len(rows))
    check("counts sum to total", sum(a["counts"].values()) == len(rows))
    check("shares sum to ~100", abs(sum(a["share"].values()) - 100.0) < 1.5)
    check("unmapped tag surfaced, not swallowed", "Brand New Tag" in a["unmapped_tags"])
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
