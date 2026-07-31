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

from . import config, db, enrich, signature

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

    if not signature.verify(raw, request.headers.get("x-freshchat-signature")):
        raise HTTPException(status_code=401, detail="bad webhook signature")

    if config.WEBHOOK_SECRET:
        supplied = request.query_params.get("secret") or request.headers.get("x-webhook-secret") or ""
        if not hmac.compare_digest(supplied, config.WEBHOOK_SECRET):
            raise HTTPException(status_code=401, detail="bad webhook secret")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="body is not valid JSON")

    if not enrich.is_resolution(payload):
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
