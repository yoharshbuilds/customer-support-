"""Freshchat Extract API client.

The only API that exposes historical per-event data. Two steps: POST a job, then
GET it until COMPLETED, which returns pre-signed S3 links to ZIPs of CSVs.

Three things here are non-obvious and each cost a debugging round:
  * the download URL is nested at link["link"]["href"], not link["href"]
  * the payload is a ZIP of CSVs, never a bare CSV
  * the S3 links are pre-signed, so sending the Freshchat bearer token to them
    makes S3 reject the request -- download with no auth header

Rate limit is one POST per minute PER EVENT; exceeding it returns 429 with
"API abused". Jobs have completed in under a minute in practice, though the
documentation warns they can take an hour.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import zipfile

import httpx

from . import config

log = logging.getLogger(__name__)

# Event names are a fixed vocabulary; a typo returns "No matching event name
# found for the given input request" rather than an empty result.
EVENT_MESSAGE_SENT = "Message-Sent"
EVENT_CSAT = "CSAT-Score"
EVENT_CONVERSATION_CREATED = "Conversation-Created"
EVENT_CONVERSATION_RESOLVED = "Conversation-Resolved"

TERMINAL = ("COMPLETED", "FAILED", "ERROR", "EXPIRED")


def _headers() -> dict:
    return {"Authorization": f"Bearer {config.FRESHCHAT_API_TOKEN}",
            "Content-Type": "application/json"}


async def fetch(client: httpx.AsyncClient, event: str, start: str, end: str,
                poll_seconds: int = 15, max_polls: int = 40) -> list[dict]:
    """Run an extract job and return its rows. Raises on a failed trigger."""
    r = await client.post(f"{config.freshchat_base()}/reports/raw",
                          headers=_headers(),
                          json={"start": start, "end": end, "event": event, "format": "csv"},
                          timeout=60.0)
    if r.status_code == 429:
        raise RuntimeError(f"rate limited on {event}: {r.text[:120]}")
    if r.status_code not in (200, 201, 202):
        raise RuntimeError(f"extract trigger failed {r.status_code}: {r.text[:200]}")
    job_id = r.json().get("id")
    if not job_id:
        raise RuntimeError(f"no job id in response: {r.text[:200]}")

    body: dict = {}
    for _ in range(max_polls):
        g = await client.get(f"{config.freshchat_base()}/reports/raw/{job_id}",
                             headers=_headers(), timeout=60.0)
        g.raise_for_status()
        body = g.json()
        if (body.get("status") or "").upper() in TERMINAL:
            break
        await asyncio.sleep(poll_seconds)

    status = (body.get("status") or "").upper()
    if status != "COMPLETED":
        raise RuntimeError(f"{event} job {job_id} ended {status or 'PENDING'}")

    rows: list[dict] = []
    for link in body.get("links", []):
        href = (link.get("link") or {}).get("href")
        if not href or (link.get("status") or "").upper() != "COMPLETED":
            log.warning("extract chunk %s not ready: %s", link.get("from"), link.get("status"))
            continue
        # No auth header: a bearer token breaks the pre-signed URL.
        d = await client.get(href, timeout=180.0, follow_redirects=True)
        if d.status_code != 200:
            log.warning("extract chunk download failed %s", d.status_code)
            continue
        try:
            zf = zipfile.ZipFile(io.BytesIO(d.content))
        except zipfile.BadZipFile:
            log.warning("extract chunk was not a zip (%d bytes)", len(d.content))
            continue
        for name in zf.namelist():
            text = zf.read(name).decode("utf-8-sig", errors="replace")
            rows.extend(csv.DictReader(io.StringIO(text)))
    return rows
