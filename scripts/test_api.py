"""API contract tests: status codes, validation bounds, response shape.

MECE layer 2 of 4. Covers only the HTTP surface -- does an endpoint exist, does it
reject bad input, is the response shaped as the page expects. It deliberately does
NOT check whether the numbers are right; that is layer 3 (test_integrity.py).

Needs the database (TestClient runs the real lifespan) but no assertions depend on
how much data happens to be in it, so it passes on an empty table too.

    python -m scripts.test_api
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

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


def main() -> int:
    with TestClient(app) as c:
        section("endpoints answer at all")
        for path in ("/", "/healthz", "/api/filters", "/api/stats", "/api/median",
                     "/api/tickets?limit=1", "/api/attendance", "/api/orders",
                     "/api/issues", "/api/tickets_flow"):
            r = c.get(path)
            check(f"GET {path} -> 200", r.status_code == 200)

        section("HEAD works where the host health check uses it")
        check("HEAD /healthz -> 200", c.head("/healthz").status_code == 200)
        check("HEAD / -> 200", c.head("/").status_code == 200)

        section("validation: out-of-range input is rejected, not clamped silently")
        cases = [
            ("/api/attendance?days=0", 422), ("/api/attendance?days=121", 422),
            ("/api/attendance?days=1", 200), ("/api/attendance?days=120", 200),
            ("/api/orders?days=0", 422), ("/api/orders?days=181", 422),
            ("/api/orders?days=1", 200),
            ("/api/issues?days=0", 422), ("/api/issues?days=366", 422),
            ("/api/tickets_flow?days=0", 422), ("/api/tickets_flow?days=121", 422),
            ("/api/attendance?days=abc", 422), ("/api/orders?days=-5", 422),
            ("/api/orders?days=1.5", 422),
        ]
        for path, want in cases:
            got = c.get(path).status_code
            check(f"GET {path} -> {want}", got == want)

        section("secret-gated debug endpoints stay shut without the secret")
        for path in ("/debug/rejects", "/debug/availability"):
            check(f"GET {path} (no secret) -> 404", c.get(path).status_code == 404)
            check(f"GET {path}?secret=wrong -> 404",
                  c.get(f"{path}?secret=definitely-not-it").status_code == 404)

        section("page is served with cache revalidation")
        r = c.get("/")
        check("Cache-Control: no-cache present",
              "no-cache" in (r.headers.get("cache-control") or ""))
        check("content-type is html", "text/html" in (r.headers.get("content-type") or ""))
        for marker in ('data-p="4"', 'data-p="5"', 'data-p="6"', 'id="hrgrid"',
                       'id="o_chart"', 'id="i_legend"', 'id="t_chart"', 'id="fresh"'):
            check(f"page contains {marker}", marker in r.text)

        section("/healthz reports every subsystem")
        h = c.get("/healthz").json()
        for key in ("ok", "events", "webhook_hits", "alerts", "availability",
                    "agent_messages", "ticket_inflow", "resolution_reconcile"):
            check(f"healthz has {key}", key in h)
        for key in ("tickets_seen_live", "availability_stored", "by_action"):
            check(f"webhook_hits has {key}", key in h["webhook_hits"])

        section("/api/orders response shape")
        o = c.get("/api/orders?days=14").json()
        check("has daily and weekly", isinstance(o.get("daily"), list) and isinstance(o.get("weekly"), list))
        check("source names Redshift", "Redshift" in (o.get("source") or ""))
        if o["daily"]:
            row = o["daily"][0]
            for k in ("bucket", "total_orders", "fully_cancelled", "partly_cancelled",
                      "not_cancelled", "cancel_rate", "excl_fully_cancelled"):
                check(f"daily row has {k}", k in row)
            check("total = fully + partly + not_cancelled",
                  row["total_orders"] == row["fully_cancelled"] + row["partly_cancelled"] + row["not_cancelled"])
            check("excl_fully = total - fully",
                  row["excl_fully_cancelled"] == row["total_orders"] - row["fully_cancelled"])
            check("buckets are unique",
                  len({r["bucket"] for r in o["daily"]}) == len(o["daily"]))
            check("daily is newest first",
                  [r["bucket"] for r in o["daily"]] == sorted((r["bucket"] for r in o["daily"]), reverse=True))
        if o["weekly"]:
            check("weekly rows carry a day count", "days_in_bucket" in o["weekly"][0])

        section("/api/issues response shape and MECE invariants")
        i = c.get("/api/issues?days=30").json()
        check("counts sum to total", sum(i["counts"].values()) == i["total"])
        check("every bucket key has a count",
              all(b["key"] in i["counts"] for b in i["buckets"]))
        check("every bucket carries a colour", all(b.get("color") for b in i["buckets"]))
        check("per_day present", isinstance(i.get("per_day"), dict))
        check("per_day totals never exceed the overall total",
              sum(sum(v.values()) for v in i["per_day"].values()) == i["total"])
        check("unmapped_tags is a dict (empty is the healthy case)",
              isinstance(i.get("unmapped_tags"), dict))

        section("/api/tickets_flow response shape")
        f = c.get("/api/tickets_flow?days=14").json()
        check("daily is a list", isinstance(f.get("daily"), list))
        check("declares both source freshnesses",
              bool(f.get("inflow_source")) and bool(f.get("resolved_source")))
        for row in f["daily"]:
            check(f"{row['bucket']}: solved never exceeds raised",
                  row["raised_and_solved"] is None or row["raised_and_solved"] <= row["raised"])
            check(f"{row['bucket']}: rate is None exactly when solved is None",
                  (row["solve_rate"] is None) == (row["raised_and_solved"] is None))

        section("/api/attendance response shape")
        a = c.get("/api/attendance?days=14").json()
        for k in ("days", "agents", "cells", "hourly", "shift_start_hour", "timezone"):
            check(f"attendance has {k}", k in a)
        check("today is inside the day list", a["today"] in a["days"] if a["days"] else True)
        for ag in a["agents"]:
            check(f"{ag['name'][:16]}: rate has a denominator only when expected",
                  (ag["days_expected"] is not None) == bool(ag["expected"]))
            check(f"{ag['name'][:16]}: present never exceeds expected",
                  ag["days_expected"] is None or ag["days_present"] <= ag["days_expected"])
        for name, per in a["cells"].items():
            for day, cell in per.items():
                check(f"{name[:12]} {day}: cell day matches its key", cell["day"] == day)
                check(f"{name[:12]} {day}: span is not negative", cell["span_hrs"] >= 0)
                check(f"{name[:12]} {day}: source is known",
                      cell["source"] in ("activity", "messages", "availability"))

        section("unknown routes 404 rather than 500")
        for path in ("/api/nope", "/nope", "/api/orders/extra"):
            check(f"GET {path} -> 404", c.get(path).status_code == 404)

    print(f"\n{TOTAL - len(FAILS)}/{TOTAL} passed")
    if FAILS:
        print("\nFAILED:")
        for f in FAILS:
            print(f"  {f}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
