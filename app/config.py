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

# --- Behaviour ---
# Model that reviews each resolved transcript. Env-overridable so the model can
# be changed from the Render dashboard without a redeploy of new code.
ANALYZE_MODEL = os.getenv("ANALYZE_MODEL", "claude-sonnet-5").strip()
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")
# How many issue-type series to plot before collapsing the tail into "Other".
TOP_N_ISSUES = int(os.getenv("TOP_N_ISSUES", "8"))
ENRICH_RETRY_SECONDS = int(os.getenv("ENRICH_RETRY_SECONDS", "120"))


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
