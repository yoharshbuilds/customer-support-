from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()


def _req(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


# --- Postgres ---
# Supabase: use the "Session pooler" connection string, not the direct one --
# the free tier has few direct connection slots.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "5"))

# Salt for the customer dedupe hash. We never store a phone number or name --
# only a salted digest, which is enough to count distinct customers and
# useless to anyone who gets hold of the database. Keep this stable: changing
# it makes historical rows stop matching new ones.
CUSTOMER_HASH_SALT = os.getenv("CUSTOMER_HASH_SALT", "").strip()

# --- Freshchat ---
# Full API base incl. scheme and /v2, exactly as shown on the Freshchat
# Admin Settings -> API Tokens page. The host is account-specific and
# long-form, e.g. https://<account>-<long-id>.freshchat.com/v2
FRESHCHAT_API_BASE = os.getenv("FRESHCHAT_API_BASE", "").strip().rstrip("/")
FRESHCHAT_API_TOKEN = os.getenv("FRESHCHAT_API_TOKEN", "").strip()
# Freshchat signs webhook bodies with RSA-SHA256; paste the public key from the
# Freshchat webhook settings page. Blank = signature check skipped.
FRESHCHAT_PUBLIC_KEY = os.getenv("FRESHCHAT_PUBLIC_KEY", "")
# Optional extra guard: append ?secret=... to the webhook URL. Useful before the
# public key is configured, and as defence in depth after.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
# Escape hatch for local testing only. Never set this on a public deployment.
ALLOW_UNVERIFIED_WEBHOOK = os.getenv("ALLOW_UNVERIFIED_WEBHOOK", "").lower() in ("1", "true", "yes")
# Freshchat's signature does not reliably cover the body it sends (verified
# against a captured sample: right key, right scheme, digest matched nothing).
# Default off so a broken signature cannot discard a genuine resolve; the URL
# secret remains mandatory. Turn on if Freshchat ever fixes their signing.
REQUIRE_SIGNATURE = os.getenv("REQUIRE_SIGNATURE", "").lower() in ("1", "true", "yes")

# --- Metabase (store + fulfillment enrichment) ---
METABASE_URL = os.getenv("METABASE_URL", "").strip().rstrip("/")
METABASE_API_KEY = os.getenv("METABASE_API_KEY", "").strip()
METABASE_DB_ID = int(os.getenv("METABASE_DB_ID", "7"))

# --- Alerting ---
# Slack incoming webhook. Unset = alerting off, everything else unaffected.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
# How many independent failure signals a resolve must trip before it is worth
# interrupting someone. 1 fires on ~30% of a normal day, 2 on ~10%. Raise it if
# the channel gets noisy; no redeploy needed.
ALERT_MIN_SIGNALS = int(os.getenv("ALERT_MIN_SIGNALS", "2"))

# --- Post-resolution survey ---
# OFF by default and deliberately so: turning this on sends messages to real
# customers. Nothing goes out until it is explicitly enabled on the host.
CSAT_ENABLED = os.getenv("CSAT_ENABLED", "").lower() in ("1", "true", "yes")

# --- Behaviour ---
# Model that reviews each resolved transcript. Env-overridable so the model can
# be changed from the Render dashboard without a redeploy of new code.
ANALYZE_MODEL = os.getenv("ANALYZE_MODEL", "claude-sonnet-5").strip()
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")
# How many issue-type series to plot before collapsing the tail into "Other".
TOP_N_ISSUES = int(os.getenv("TOP_N_ISSUES", "8"))
ENRICH_RETRY_SECONDS = int(os.getenv("ENRICH_RETRY_SECONDS", "120"))

# --- Attendance page ---
# A shift starting 22:00 and ending 05:00 is ONE shift, and it belongs to the day
# it started. Bucketing on the calendar date instead splits it across midnight
# and reports a 23.9-hour span for a 7-hour night. Everything from this hour to
# the same hour next morning is one "shift day".
SHIFT_START_HOUR = int(os.getenv("SHIFT_START_HOUR", "6"))
# Below this many hours of observed activity a present agent is flagged rather
# than counted as a full shift. A prompt to go look, not a judgement.
SHORT_SHIFT_HOURS = float(os.getenv("SHORT_SHIFT_HOURS", "4"))
# The Freshchat role that means "works the support queue". Owners and Admins hold
# seats but no shifts, so only this role can be marked absent. Anyone else who
# resolves chats is still shown -- an Owner does work this queue.
SUPPORT_ROLE = os.getenv("SUPPORT_ROLE", "Customer Delight").strip()
# Longest credible single signed-in stretch. Agents end a shift by going
# UNAVAILABLE, which closes a session cleanly -- but someone who simply walks
# away leaves the session open, and closing it at "now" charged one agent 64
# hours to a single Friday. A session is capped at this many hours AND at the end
# of its own shift day, then flagged, so a forgotten toggle shows up as a data
# problem instead of as overtime.
MAX_SESSION_HOURS = float(os.getenv("MAX_SESSION_HOURS", "12"))

# First local day whose resolutions we hold in full.
#
# The solve rate needs to know when capture became COMPLETE, which is not the same
# as when it started. Deriving it from MIN(resolved_at) + 1 day put the boundary at
# 28 Jul, and the ticket chart then reported the team solving 1.5% of tickets on
# 28 Jul and 40.3% on 29 Jul. Those are holes in our capture, not the team's work:
# the webhook only began firing reliably part-way through, and scripts/test_integrity.py
# confirmed 30 Jul onwards matches Freshchat's own export exactly while 27-29 Jul
# does not (34.5% / 1.5% / 40.3% of the resolutions Freshchat has).
#
# So the derived boundary is a floor, never the whole answer. Days before this are
# shown as raised-only in neutral grey rather than as a catastrophic solve rate.
# Backfilling 27-29 Jul from the Extract API would let this move earlier; until
# then, understating our coverage is the safe direction to be wrong in.
RESOLVE_COMPLETE_FROM = os.getenv("RESOLVE_COMPLETE_FROM", "2026-07-30").strip() or None


# Every warehouse query goes to Redshift. It carries the same rds_public tables
# as the Postgres instance and is exactly as fresh -- both reported the same
# max(order_placed_at) to the millisecond -- while db 7's analytics schema is
# stale (analytics.orders_comparison there ends 2025-06-21). Redshift is also what
# Plazza Dashboard v4 reads, so numbers agree with what the team already sees.
METABASE_REDSHIFT_DB_ID = int(os.getenv("METABASE_REDSHIFT_DB_ID", "8"))


def _normalize_base(url: str) -> str:
    """Tolerate a hand-edited env file that lost its scheme.

    Without this, a base of 'host.freshchat.com/v2' reaches httpx as a
    schemeless URL and every enrichment dies with UnsupportedProtocol -- in the
    background task, where it is easy to miss.
    """
    url = url.strip().rstrip("/")
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def freshchat_base() -> str:
    if not FRESHCHAT_API_BASE:
        raise RuntimeError("Missing FRESHCHAT_API_BASE")
    return _normalize_base(FRESHCHAT_API_BASE)


def metabase_base() -> str:
    return _normalize_base(METABASE_URL)
