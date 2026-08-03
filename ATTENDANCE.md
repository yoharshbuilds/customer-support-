# Attendance tab

The fourth tab of the dashboard. Answers **how much of the day the team was
available to take chats** — a staffing question, not a shift-compliance one.

That distinction is deliberate and it is the thing to keep straight: an agent who
is logged in but UNAVAILABLE (on a break, or doing admin work) does **not** count
as available here. They were at work; they were not takeable. If you ever need
"did this person show up and stay for their shift", this tab is the wrong number —
see *Hours at work* below.

## Two sources, never mixed silently

| source | what it is | how it looks |
|---|---|---|
| **availability webhooks** | real sign-in/sign-out from Freshchat `agent_availability` events | outlined cell, tooltip says "Signed in" |
| **resolve activity** | inferred: first→last resolve of the shift day | plain cell, tooltip says "inferred" |

Availability wins wherever it exists; activity fills the rest. The banner reports
how many agent-days are real, and the caveat text changes to match — so the two
kinds of number are never presented as the same measurement.

The activity proxy is a **floor**. An agent who worked a full shift on chats a
colleague closed reads as absent, and idle time inside a shift is invisible.

## Shift days, not calendar days

A shift starting 22:00 and ending 05:00 is one shift, filed under the day it
started. Grouping on the calendar date splits it across midnight and reported a
23.9-hour span for a 7-hour night. Boundary is 06:00 local, `SHIFT_START_HOUR`.

## Never went unavailable

Ending a shift by going UNAVAILABLE is the normal path — logout is not required
and is not used. But an agent who just walks away leaves the session open, and
closing it at "now" once charged 64 hours to a single Friday.

A session is truncated at whichever comes first: `MAX_SESSION_HOURS` (12) or the
end of its own shift day. Truncated days render as `≥8h` with a tooltip saying
why, so a forgotten toggle reads as a data problem rather than as overtime. A
genuine 22:00–05:00 night shift fits inside its shift-day window and is untouched.

## Who counts

Roster comes from the Freshchat `/agents` API. Metabase's
`freshworks.freshchat_agents` mirror **stopped syncing on 2026-06-02** and lists 8
agents where the API returns 17 — do not use it.

- **Expected** (can be marked absent): active agents with role `Customer Delight`.
- **Shown, never absent**: anyone who resolved chats without that role. An Owner
  works this queue, and filtering on role alone made her vanish.
- **Excluded**: deactivated/deleted agents, and non-support seats with no
  activity — Owners and Admins hold seats but no shifts, and counting them created
  three permanent absentees.

## Data boundaries

- The current shift day is **partial** and excluded from every denominator.
- Days before the earliest resolve show as no-data, not absent.
- A window longer than the data is clipped, not padded.

## Hours at work (not implemented)

Freshchat tracks login separately from availability, and agents do sit logged in
while unavailable. `login_status` is captured into its own column so this stays
possible, but nothing reads it: whether these webhooks even carry login state is
unknown until enough real payloads land. `/healthz` reports `with_login_status`
— if it stays 0, availability is all we get and shift compliance needs a
different signal.

## Diagnosing the capture

The `agent_availability` payload is **undocumented** (the public webhook article
lists four events; this is not one of them), so every extracted field is a guess
and `raw` is stored verbatim on every row.

```bash
curl "https://customer-support-ael1.onrender.com/healthz" | jq .availability
```

`unknown_status` or `no_agent_id` climbing means the guesses are wrong for the
real shape. Read the actual payloads, fix `availability.py`, then rebuild the
columns from `raw` — the events themselves are never lost.

```bash
curl "https://customer-support-ael1.onrender.com/debug/availability?secret=$WEBHOOK_SECRET" | jq .
```

Tests: `python -m scripts.test_availability` (61 cases, no database needed).
