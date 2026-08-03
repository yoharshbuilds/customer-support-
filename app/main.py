from __future__ import annotations
import asyncio
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from anthropic import AsyncAnthropic
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from . import alerts, attendance, availability, config, db, enrich, roster, signature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("resolve-dashboard")

STATIC = Path(__file__).parent / "static"
_subscribers: set[asyncio.Queue] = set()
_client: httpx.AsyncClient | None = None
_llm: AsyncAnthropic | None = None
# Strong references to in-flight enrichment tasks. asyncio only keeps a weak
# reference to a bare create_task(), so a fire-and-forget task can be garbage
# collected mid-await and vanish without running or raising. That is exactly
# what happened on the first Render deploy: the row was stored, the immediate
# enrichment silently never ran, and only the 120s sweeper filled it in.
_inflight: set[asyncio.Task] = set()

# In-memory tally of what the webhook endpoint has seen since boot. Exposed on
# /healthz so "is Freshchat calling us at all?" can be answered without reading
# the host's logs -- a question that has cost several round trips already.
# Resets on restart, which is fine: last_webhook_at is the load-bearing field.
_hits = {"total": 0, "accepted": 0, "ignored": 0, "rejected": 0,
         "rejected_signature": 0, "rejected_secret": 0, "accepted_unsigned": 0,
         "last_at": None, "last_action": None, "last_reject": None,
         # Availability capture, split so "the webhook arrived" can be told apart
         # from "the row was written".
         "availability_stored": 0, "availability_replays": 0, "availability_failed": 0,
         # Composition of the traffic. One webhook URL receives every event
         # type Freshchat emits, so knowing the mix explains both the volume
         # and how much of it we discard.
         "by_action": {}}

# Last few rejected requests, kept in memory so the exact (signature, body)
# pair can be replayed offline against the public key. Bounded, never
# persisted, and only readable with the webhook secret.
_reject_samples: list[dict] = []


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _inflight.add(task)
    task.add_done_callback(_inflight.discard)
    task.add_done_callback(_log_task_exception)


def _log_task_exception(task: asyncio.Task) -> None:
    """Surface a crash in a background task instead of swallowing it."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("background task died: %r", exc, exc_info=exc)


def _walk_action(payload: dict) -> str | None:
    """Best-effort action name, for the /healthz tally."""
    return enrich._walk(payload, "action")


async def _broadcast(kind: str, data: dict) -> None:
    msg = json.dumps({"type": kind, **data}, default=str)
    for q in list(_subscribers):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


async def _enrich_row(row_id: int, conversation_id: str, resolved_at: datetime,
                      actor: dict | None = None) -> None:
    assert _client is not None
    try:
        fields = await enrich.enrich(_client, conversation_id, resolved_at, llm=_llm, payload_actor=actor)
        db.update_enrichment(row_id, fields)
        await _broadcast("resolved", {"conversation_id": conversation_id, **fields})
        # Last, and deliberately after the row is safely stored: a Slack outage
        # must never cost us the event.
        await alerts.maybe_alert(_client, conversation_id, fields)
    except Exception as e:  # noqa: BLE001 - row stays pending and the sweeper retries
        log.warning("enrichment failed for %s: %s", conversation_id, e)
        db.update_enrichment(row_id, {}, error=str(e)[:500])


async def _retry_sweeper() -> None:
    """Re-attempt rows whose enrichment failed, so no resolve stays unattributed."""
    while True:
        await asyncio.sleep(config.ENRICH_RETRY_SECONDS)
        try:
            for row in db.pending_enrichment():
                payload = row["raw"] or {}
                ts = enrich.resolved_at_from(payload)
                actor = payload.get("actor") if isinstance(payload.get("actor"), dict) else None
                await _enrich_row(row["id"], row["conversation_id"], ts, actor)
        except Exception as e:  # noqa: BLE001
            log.warning("retry sweeper error: %s", e)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _client, _llm
    db.init()
    _client = httpx.AsyncClient()
    # No key configured -> _llm stays None and enrichment skips the review
    # rather than failing; the structured fields still populate.
    if os.getenv("ANTHROPIC_API_KEY"):
        _llm = AsyncAnthropic()
        log.info("LLM review enabled (%s)", enrich.analyze.MODEL)
    else:
        log.warning("ANTHROPIC_API_KEY unset — LLM review disabled")
    if not config.FRESHCHAT_PUBLIC_KEY and not config.WEBHOOK_SECRET:
        log.warning(
            "webhook auth NOT configured — /webhook/freshchat will reject requests. "
            "Set FRESHCHAT_PUBLIC_KEY or WEBHOOK_SECRET."
            + (" (ALLOW_UNVERIFIED_WEBHOOK is on: accepting anyway)"
               if config.ALLOW_UNVERIFIED_WEBHOOK else "")
        )
    task = asyncio.create_task(_retry_sweeper())
    log.info("ready")
    yield
    task.cancel()
    await _client.aclose()
    if _llm is not None:
        await _llm.close()


app = FastAPI(title="Support Resolve Dashboard", lifespan=lifespan)


# --------------------------------------------------------------------------
# Webhook
# --------------------------------------------------------------------------
@app.post("/webhook/freshchat")
async def freshchat_webhook(request: Request):
    # Read the raw bytes first: the RSA signature covers the exact body, so it
    # must be verified before anything re-serializes the JSON.
    raw = await request.body()
    _hits["total"] += 1
    _hits["last_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Fail closed. With no signature key and no URL secret this endpoint would
    # accept anything from anyone, and forged resolves are indistinguishable
    # from real ones once they are in the table. Set FRESHCHAT_PUBLIC_KEY (or
    # WEBHOOK_SECRET), or opt out explicitly for local testing.
    if not config.FRESHCHAT_PUBLIC_KEY and not config.WEBHOOK_SECRET:
        if not config.ALLOW_UNVERIFIED_WEBHOOK:
            raise HTTPException(
                status_code=401,
                detail="webhook auth not configured: set FRESHCHAT_PUBLIC_KEY or "
                       "WEBHOOK_SECRET (or ALLOW_UNVERIFIED_WEBHOOK=true to test locally)",
            )

    # Both rejections below return a bare 401, which in the access log is
    # indistinguishable from the other. Say which check failed, and whether the
    # signature header was even present -- otherwise a key-format mismatch and a
    # wrong ?secret= look identical from outside, and silence from Freshchat is
    # impossible to tell apart from nobody having clicked Resolve.
    # The URL secret is checked FIRST and is the factor we actually rely on.
    #
    # Freshchat's signature proved unreliable in production: a captured failing
    # sample verified as PKCS#1 v1.5 + SHA-256 (exactly what we implement) with
    # the right key, but the digest it signed matched no representation of the
    # body it sent -- not the raw bytes, compact JSON, sorted JSON, or any
    # subset. Their signature simply does not cover their payload for some
    # events. Requiring it would discard genuine resolves.
    #
    # The secret is 64 random characters, known only to Freshchat and us and
    # sent over TLS -- the same model as a webhook URL token. Set
    # REQUIRE_SIGNATURE=true to demand both again if Freshchat ever fixes this.
    if config.WEBHOOK_SECRET:
        supplied = request.query_params.get("secret") or request.headers.get("x-webhook-secret") or ""
        if not hmac.compare_digest(supplied, config.WEBHOOK_SECRET):
            _hits["rejected"] += 1
            _hits["rejected_secret"] += 1
            _hits["last_reject"] = f"secret; supplied={len(supplied)} chars"
            log.warning("webhook REJECTED: ?secret= did not match (supplied %d chars)", len(supplied))
            raise HTTPException(status_code=401, detail="bad webhook secret")

    sig_header = request.headers.get("x-freshchat-signature")
    sig_ok = signature.verify(raw, sig_header)
    if not sig_ok and not config.REQUIRE_SIGNATURE and config.WEBHOOK_SECRET:
        _hits["accepted_unsigned"] += 1
        log.warning("webhook signature did not verify; accepted on URL secret alone "
                    "(body %d bytes, header %s)", len(raw), "present" if sig_header else "MISSING")
        sig_ok = True

    if not sig_ok:
        # Log the signature and the exact body on rejection. Neither is secret:
        # the signature is over this one body and cannot be replayed elsewhere,
        # and a resolution payload carries only ids -- no customer data. With
        # both, the verification can be reproduced offline against the public
        # key and the algorithm corrected, instead of guessing across redeploys.
        log.warning(
            "webhook REJECTED: signature check failed\n"
            "  sig-header: %s\n"
            "  body (%d bytes): %s\n"
            "  other headers: %s",
            sig_header or "MISSING",
            len(raw),
            raw[:800].decode("utf-8", "replace"),
            {k: v for k, v in request.headers.items()
             if k.lower().startswith(("x-", "user-agent", "content-type"))},
        )
        _hits["rejected"] += 1
        _hits["rejected_signature"] += 1
        _reject_samples.append({
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "signature": sig_header,
            "body": raw.decode("utf-8", "replace"),
            "headers": {k: v for k, v in request.headers.items()
                        if k.lower().startswith(("x-", "user-agent", "content-type"))},
        })
        del _reject_samples[:-5]
        _hits["last_reject"] = (
            f"signature; header={'yes' if sig_header else 'MISSING'}; "
            f"bytes={len(raw)}; ua={request.headers.get('user-agent', '?')[:40]}"
        )
        raise HTTPException(status_code=401, detail="bad webhook signature")

    if config.WEBHOOK_SECRET:
        supplied = request.query_params.get("secret") or request.headers.get("x-webhook-secret") or ""
        if not hmac.compare_digest(supplied, config.WEBHOOK_SECRET):
            log.warning("webhook REJECTED: ?secret= did not match (supplied %d chars)", len(supplied))
            _hits["rejected"] += 1
            _hits["rejected_secret"] += 1
            _hits["last_reject"] = f"secret; supplied={len(supplied)} chars"
            raise HTTPException(status_code=401, detail="bad webhook secret")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="body is not valid JSON")

    if not enrich.is_resolution(payload):
        # Log the whole payload, not just the action name. If Freshchat labels
        # a Status -> Resolved change with an action we do not recognise, this
        # line is the only place the real shape will ever appear -- the request
        # otherwise succeeds with 200 and vanishes. Ids only, no customer data.
        _hits["ignored"] += 1
        act = (_walk_action(payload) or "unknown")[:60]
        _hits["last_action"] = act
        _hits["by_action"][act] = _hits["by_action"].get(act, 0) + 1

        # Not a resolution, but agent availability changes are the only genuine
        # sign-in record in the system, so they get stored instead of discarded.
        # Wrapped: a capture bug must never turn a 200 into a 500, because
        # Freshchat retries failures and eventually disables the endpoint.
        if availability.is_availability(payload):
            try:
                ev = availability.extract(payload)
                row_id = db.insert_availability(ev)
                _hits["availability_stored"] += int(row_id is not None)
                if row_id is None:
                    _hits["availability_replays"] += 1
                log.info("availability %s for agent %s at %s (row %s)",
                         ev["status"], ev["agent_id"], ev["changed_at"], row_id)
            except Exception as e:  # noqa: BLE001
                _hits["availability_failed"] += 1
                log.warning("availability capture failed: %s", e)
            return {"ok": True, "stored": "agent availability"}

        log.warning("webhook IGNORED (not recognised as a resolution): %s",
                    json.dumps(payload)[:1200])
        return {"ok": True, "ignored": "not a resolution event"}

    conversation_id = enrich.extract_conversation_id(payload)
    if not conversation_id:
        log.warning("no conversation_id in payload: %s", json.dumps(payload)[:400])
        raise HTTPException(status_code=400, detail="no conversation_id in payload")

    resolved_at = enrich.resolved_at_from(payload)

    actor = payload.get("actor") if isinstance(payload.get("actor"), dict) else None
    resolver_id = (actor or {}).get("actor_id")

    # Store first, enrich after: a Freshchat or Metabase outage must never cost
    # us the event, and Freshchat gets its 200 without waiting on those calls.
    _hits["accepted"] += 1
    act = (_walk_action(payload) or "unknown")[:60]
    _hits["last_action"] = act
    _hits["by_action"][act] = _hits["by_action"].get(act, 0) + 1
    log.info("webhook ACCEPTED: conversation %s resolved at %s", conversation_id, resolved_at)
    row_id = db.insert_event(
        {
            "conversation_id": conversation_id,
            "resolved_at": resolved_at,
            "resolver_agent_id": resolver_id,
            "raw": payload,
        }
    )
    if row_id is None:
        return {"ok": True, "duplicate": True}

    _spawn(_enrich_row(row_id, conversation_id, resolved_at, actor))
    return {"ok": True, "id": row_id}


# --------------------------------------------------------------------------
# Query API
# --------------------------------------------------------------------------
def _resolve_range(range_key: str, dt_from: str | None, dt_to: str | None) -> dict:
    tz = ZoneInfo(config.TIMEZONE)
    now = datetime.now(tz)
    today = now.date()

    if range_key == "custom":
        if not dt_from or not dt_to:
            raise HTTPException(400, "custom range needs from and to (YYYY-MM-DD)")
        try:
            start = datetime.combine(datetime.fromisoformat(dt_from).date(), time.min, tz)
            # `to` is inclusive of the whole day the user picked.
            end = datetime.combine(datetime.fromisoformat(dt_to).date(), time.min, tz) + timedelta(days=1)
        except ValueError:
            raise HTTPException(400, "dates must be YYYY-MM-DD")
        if end <= start:
            raise HTTPException(400, "'to' must not be before 'from'")
        return {"dt_from": start, "dt_to": end}

    spans = {"today": 0, "yesterday": 1, "week": 7, "month": 30}
    if range_key == "yesterday":
        start = datetime.combine(today - timedelta(days=1), time.min, tz)
        return {"dt_from": start, "dt_to": start + timedelta(days=1)}
    days = spans.get(range_key, 7)
    start = datetime.combine(today - timedelta(days=days), time.min, tz)
    return {"dt_from": start, "dt_to": now + timedelta(minutes=1)}


FILTER_PARAMS = (
    "store", "fulfillment", "issue", "agent", "resolution", "sentiment", "search",
)


def _filters(request: Request, range_key: str, dt_from: str | None, dt_to: str | None) -> dict:
    """Shared filter set for all three pages."""
    f = _resolve_range(range_key, dt_from, dt_to)
    q = request.query_params
    for name in FILTER_PARAMS:
        f[name] = q.get(name) or None
    for name in ("cross_store", "sla_breached", "reopened"):
        v = q.get(name)
        f[name] = None if v in (None, "") else v.lower() in ("1", "true", "yes")
    return f


def _issue_col(request: Request) -> str:
    return "llm_issue" if request.query_params.get("issue_source", "llm") == "llm" else "issue_type"


def _json(payload) -> JSONResponse:
    return JSONResponse(json.loads(json.dumps(payload, default=str)))


# ---- Page 1: live ticket list -------------------------------------------
@app.get("/api/tickets")
def tickets(
    request: Request,
    range: str = Query("week"),
    dt_from: str | None = Query(None, alias="from"),
    dt_to: str | None = Query(None, alias="to"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("resolved_at"),
    direction: str = Query("desc"),
):
    f = _filters(request, range, dt_from, dt_to)
    col = _issue_col(request)
    return _json({
        "total": db.total_rows(f, col),
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "direction": direction,
        "rows": db.tickets(f, limit, offset, sort, direction, col),
    })


# ---- Page 2: issue-wise breakdown ---------------------------------------
@app.get("/api/stats")
def stats(
    request: Request,
    range: str = Query("week"),
    dt_from: str | None = Query(None, alias="from"),
    dt_to: str | None = Query(None, alias="to"),
    count_mode: str = Query("tickets", pattern="^(tickets|customers|orders)$"),
):
    f = _filters(request, range, dt_from, dt_to)
    col = _issue_col(request)
    cm = count_mode
    return _json({
        "range": {"from": f["dt_from"], "to": f["dt_to"]},
        "count_mode": cm,
        "issue_source": request.query_params.get("issue_source", "llm"),
        "summary": db.summary(f, col, cm),
        "by_issue": db.breakdown(f, col, col, cm),
        "by_issue_agent_tagged": db.breakdown(f, "issue_type", col, cm),
        "by_store": db.breakdown(f, "store", col, cm),
        "by_fulfillment": db.breakdown(f, "fulfillment", col, cm),
        "by_agent": db.breakdown(f, "resolver_agent_name", col, cm),
        "by_resolution": db.breakdown(f, "llm_resolution", col, cm),
        "by_sentiment": db.breakdown(f, "llm_sentiment", col, cm),
    })


# ---- Page 3: median resolution time -------------------------------------
@app.get("/api/median")
def median(
    request: Request,
    range: str = Query("week"),
    dt_from: str | None = Query(None, alias="from"),
    dt_to: str | None = Query(None, alias="to"),
    series: str = Query("fulfillment"),
    top_n: int = Query(0),
):
    """Date on Y, median resolution on X, one bar per `series` value."""
    if series not in db.SERIES_COLUMNS:
        raise HTTPException(400, f"series must be one of {sorted(db.SERIES_COLUMNS)}")
    f = _filters(request, range, dt_from, dt_to)
    col = _issue_col(request)
    return _json({
        "series_column": series,
        "agent": f.get("agent"),
        "points": db.median_by_day(f, series, top_n or config.TOP_N_ISSUES, col),
    })


@app.get("/api/filters")
def filters():
    return db.distinct_values()


@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _subscribers.add(q)

    async def gen():
        try:
            yield "retry: 5000\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _subscribers.discard(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# HEAD is declared explicitly rather than relying on the framework: Starlette
# <1.0 added it to every GET route automatically and 1.x does not, so uptime
# monitors that probe with HEAD (UptimeRobot does by default) got a 405 and
# reported the service down while it was perfectly healthy.
@app.get("/debug/rejects")
def debug_rejects(secret: str = Query("")):
    """The last few rejected webhooks, for debugging signature verification.

    Gated on the webhook secret: these payloads are ids only, but there is no
    reason to serve them to the open internet.
    """
    if not config.WEBHOOK_SECRET or not hmac.compare_digest(secret, config.WEBHOOK_SECRET):
        raise HTTPException(status_code=404, detail="not found")
    return {"count": len(_reject_samples), "samples": _reject_samples}


@app.get("/debug/availability")
def debug_availability(secret: str = Query(""), limit: int = Query(20, ge=1, le=100)):
    """Raw agent availability payloads, so the undocumented shape can be read off.

    This is the point of the whole capture: the public webhook docs list four
    events and agent availability is not one of them, so the only way to learn
    which key holds the status and which holds the timestamp is to look at what
    actually arrives. Gated on the webhook secret -- the payloads name real staff.
    """
    if not config.WEBHOOK_SECRET or not hmac.compare_digest(secret, config.WEBHOOK_SECRET):
        raise HTTPException(status_code=404, detail="not found")
    stats = db.availability_stats()
    return {
        "stats": stats,
        # If unknown_status or no_agent_id is climbing, the extraction guesses in
        # availability.py are wrong for the real payload -- fix them and rebuild
        # the columns from raw.
        "extraction_healthy": bool(stats.get("events"))
        and not stats.get("unknown_status") and not stats.get("no_agent_id"),
        "samples": db.availability_samples(limit),
    }


def _attendance_days(first_day, today, days: int) -> list:
    """The shift days to show, newest first, clipped to where data exists.

    Clipping matters: a day before the earliest resolve is a day we have no data
    for, which must not render as a day the whole team was absent.
    """
    out = [today - timedelta(days=i) for i in range(days)]
    if first_day is not None:
        out = [d for d in out if d >= first_day]
    return out


@app.get("/api/attendance")
async def api_attendance(days: int = Query(14, ge=1, le=120)):
    """Daily attendance derived from resolve activity.

    A proxy, not a sign-in record -- nothing here logs when an agent starts work.
    "Present" means the agent resolved at least one chat during that shift day,
    so someone who worked a full shift on chats a colleague closed reads as
    absent. It is a floor on attendance, and the page says so.
    """
    b = attendance.bounds()
    today = b.get("today_shift_day")
    if today is None:
        return {"days": [], "agents": [], "cells": {}, "note": "no resolve data yet"}

    rows = attendance.rows(days)
    try:
        assert _client is not None
        people = await roster.fetch(_client)
        roster_ok = True
    except Exception as e:  # noqa: BLE001 -- the grid is still useful without it
        log.warning("roster fetch failed: %s", e)
        people, roster_ok = [], False

    day_list = _attendance_days(b.get("first_day"), today, days)
    day_set = {d.isoformat() for d in day_list}
    rows = [r for r in rows if r["day"] in day_set]

    seen_names: dict[str, str] = {}
    for r in rows:
        seen_names.setdefault(r["agent"], r["agent_id"])

    # --- real attendance, where we have it -------------------------------
    # Availability webhooks give actual sign-in/sign-out. Where a day has them
    # they win over the activity proxy; elsewhere the proxy still fills in.
    tz = ZoneInfo(config.TIMEZONE)
    now_utc = datetime.now(timezone.utc)
    id_to_name = {p["id"]: p["name"] for p in people if p.get("id")}
    for r in rows:  # roster name wins, but a departed agent is still nameable
        id_to_name.setdefault(r["agent_id"], r["agent"])

    avail_by_name: dict[str, dict] = {}
    unmapped_ids: set[str] = set()
    try:
        by_agent: dict[str, list] = {}
        for ev in db.availability_events(days):
            by_agent.setdefault(ev["agent_id"], []).append(ev)
        for aid, evs in by_agent.items():
            name = id_to_name.get(aid)
            if not name:
                # An id we cannot tie to a known agent. Left out rather than
                # invented as a person -- if the extraction guessed the wrong
                # field this would otherwise conjure staff out of customer ids.
                unmapped_ids.add(aid)
                continue
            agg = availability.aggregate(
                availability.sessions_from_events(evs, now_utc, tz))
            if agg:
                avail_by_name[name] = agg
    except Exception as e:  # noqa: BLE001 -- proxy-only is still a working page
        log.warning("availability overlay failed: %s", e)

    # Someone signed in counts as present even if they resolved nothing.
    for name in avail_by_name:
        seen_names.setdefault(name, "")

    agents: dict[str, dict] = {}
    for p in people:
        # `expected` -- not merely "on the roster" -- decides who can be absent.
        # Owners and Admins have Freshchat seats but never work the queue, so
        # counting them made three people permanently absent.
        agents[p["name"]] = {**p, "in_roster": True, "expected": p["is_support"],
                             "ever_active": False}
    for name, aid in seen_names.items():
        if name in agents:
            agents[name]["ever_active"] = True
        else:
            # Resolved chats but not on the active roster: deactivated since.
            agents[name] = {"id": aid, "name": name, "email": None, "role": None,
                            "is_support": False, "availability_status": None,
                            "login_status": None, "in_roster": False,
                            "expected": False, "ever_active": True}

    # A non-support seat that resolved nothing is noise, not an absence.
    agents = {n: a for n, a in agents.items() if a["expected"] or a["ever_active"]}

    cells: dict[str, dict] = {}
    for r in rows:
        cells.setdefault(r["agent"], {})[r["day"]] = {**r, "source": "activity"}

    # Overlay the real record. Hours and the window come from the sign-in data;
    # the resolve count is kept from the proxy, since that is throughput and the
    # two answer different questions.
    for name, per_day in avail_by_name.items():
        for day, agg in per_day.items():
            if day not in day_set:
                continue
            existing = cells.setdefault(name, {}).get(day, {})
            first_local = agg["first"].astimezone(tz)
            last_local = agg["last"].astimezone(tz)
            span_hrs = round(agg["secs"] / 3600.0, 1)
            cells[name][day] = {
                **existing,
                "agent": name,
                "day": day,
                "first": first_local.strftime("%H:%M"),
                "last": last_local.strftime("%H:%M"),
                "span_hrs": span_hrs,
                "resolves": existing.get("resolves", 0),
                "short": span_hrs < config.SHORT_SHIFT_HOURS,
                "overnight": last_local.date() != first_local.date(),
                "sessions": agg["sessions"],
                "still_open": agg["open"],
                "source": "availability",
            }

    # The current shift day is still running, so it is excluded from every
    # denominator -- its absences are not absences yet.
    full_days = [d.isoformat() for d in day_list if d != today]
    for name, a in agents.items():
        mine = cells.get(name, {})
        present_days = [d for d in full_days if d in mine]
        a["days_present"] = len(present_days)
        a["days_expected"] = len(full_days) if a["expected"] else None
        a["resolves"] = sum(mine[d]["resolves"] for d in mine)
        spans = [mine[d]["span_hrs"] for d in present_days]
        a["avg_span_hrs"] = round(sum(spans) / len(spans), 1) if spans else 0.0
        a["total_hrs"] = round(sum(spans), 1)

    order = sorted(agents.values(), key=lambda a: (-a["days_present"], -a["resolves"], a["name"]))

    return {
        "shift_start_hour": config.SHIFT_START_HOUR,
        "timezone": config.TIMEZONE,
        "short_shift_hours": config.SHORT_SHIFT_HOURS,
        "support_role": config.SUPPORT_ROLE,
        "roster_ok": roster_ok,
        # How much of this grid is a real sign-in record rather than inferred.
        # Until the availability webhooks have accumulated this is 0 and the page
        # says so, so nobody mistakes the proxy for a punch clock.
        "availability_days": sum(
            1 for c in cells.values() for v in c.values() if v.get("source") == "availability"
        ),
        "availability_unmapped_ids": len(unmapped_ids),
        "today": today.isoformat(),
        "data_from": b["first_day"].isoformat() if b.get("first_day") else None,
        "days": [d.isoformat() for d in day_list],
        "agents": order,
        "cells": cells,
    }


@app.api_route("/healthz", methods=["GET", "HEAD"])
def healthz():
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n, MAX(resolved_at) AS latest FROM resolve_events")
        row = cur.fetchone()
    # `model` and `llm` are reported so we can confirm from outside what is
    # actually deployed. A config value that silently differs between .env and
    # the host has already cost us once (FRESHCHAT_API_BASE), and a model
    # mismatch would be invisible in the data until the bill arrived.
    return {
        "ok": True,
        "events": row["n"],
        "latest": str(row["latest"]),
        "subscribers": len(_subscribers),
        "model": enrich.analyze.MODEL,
        "llm": _llm is not None,
        # Whether Slack alerting is wired, without exposing the webhook URL.
        "webhook_hits": dict(_hits),
        "alerts": bool(config.SLACK_WEBHOOK_URL),
        "alert_delivery": dict(alerts.STATS),
        "alert_bar": config.ALERT_MIN_SIGNALS,
        # Whether the real attendance record is filling up, visible without a
        # secret so "is capture live yet?" is answerable from outside.
        "availability": db.availability_stats(),
    }


@app.api_route("/", methods=["GET", "HEAD"])
def index():
    # The UI is built after the backend. Until dashboard.html exists, say so
    # rather than raising a 500 -- a crashing root path makes a healthy deploy
    # look broken.
    page = STATIC / "dashboard.html"
    if not page.exists():
        return JSONResponse(
            {
                "service": "resolve-dashboard",
                "ui": "not built yet",
                "api": ["/healthz", "/api/tickets", "/api/stats", "/api/median", "/api/filters", "/api/stream"],
            }
        )
    return FileResponse(page)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
