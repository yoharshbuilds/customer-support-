"""Parse-layer tests against Freshchat's DOCUMENTED webhook payloads.

Payload shapes copied from Freshchat's "Webhooks - Payload Structure and
Authentication" article, not invented. This exercises the pure parsing
functions only -- it does NOT prove the endpoint works end to end (that needs
a database and a public URL).

Usage: python -m scripts.test_webhook_parse
"""
from __future__ import annotations

from app import enrich

RESOLUTION = {
    "actor": {"actor_type": "agent", "actor_id": "fc2d90bc-688b-46bc-a5f0-bcd60b7c2370"},
    "action": "conversation_resolution",
    "action_time": "2022-11-22T10:54:42.298Z",
    "data": {
        "resolve": {
            "resolver": "agent",
            "resolver_id": "fc2d90bc-688b-46bc-a5f0-bcd60b7c2370",
            "conversation": {
                "conversation_id": "35e80ec8-c697-42e4-8ce9-a47a050b1d1a",
                "status": "resolved",
                "app_id": "c6ec783e-4c89-490a-bbbc-32c97e2a8c3d",
                "channel_id": "304d0a73-5135-4be6-9df3-7a4aa2a0d855",
            },
        }
    },
}

REOPEN = {
    "actor": {"actor_type": "user", "actor_id": "aaa"},
    "action": "conversation_reopen",
    "action_time": "2022-11-22T11:00:00.000Z",
    "data": {
        "reopen": {
            "conversation": {
                "conversation_id": "35e80ec8-c697-42e4-8ce9-a47a050b1d1a",
                "status": "new",
            }
        }
    },
}

# A message sent on a conversation that is ALREADY resolved. This must not be
# counted as a resolve event -- otherwise every follow-up message on a closed
# ticket inflates the resolved count.
MESSAGE_ON_RESOLVED = {
    "actor": {"actor_type": "user", "actor_id": "bbb"},
    "action": "message_create",
    "action_time": "2022-11-22T11:05:00.000Z",
    "data": {
        "message": {
            "message_parts": [{"text": {"content": "hi again"}}],
            "conversation_id": "35e80ec8-c697-42e4-8ce9-a47a050b1d1a",
            "status": "resolved",
        }
    },
}

ASSIGNMENT = {
    "actor": {"actor_type": "agent", "actor_id": "ccc"},
    "action": "conversation_assignment",
    "action_time": "2022-11-22T10:00:00.000Z",
    "data": {"assignment": {"conversation": {"conversation_id": "35e80ec8", "status": "assigned"}}},
}

CASES = [
    ("conversation_resolution", RESOLUTION, True, "35e80ec8-c697-42e4-8ce9-a47a050b1d1a"),
    ("conversation_reopen", REOPEN, False, None),
    ("message_create on resolved conv", MESSAGE_ON_RESOLVED, False, None),
    ("conversation_assignment", ASSIGNMENT, False, None),
]


def main() -> int:
    failures = 0
    for name, payload, want_resolution, want_cid in CASES:
        got_resolution = enrich.is_resolution(payload)
        got_cid = enrich.extract_conversation_id(payload) if got_resolution else None
        ok = got_resolution == want_resolution and got_cid == want_cid
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
        if not ok:
            print(f"        is_resolution: got {got_resolution}, want {want_resolution}")
            print(f"        conversation_id: got {got_cid!r}, want {want_cid!r}")

    ts = enrich.resolved_at_from(RESOLUTION)
    ts_ok = ts.isoformat().startswith("2022-11-22T10:54:42")
    failures += not ts_ok
    print(f"{'PASS' if ts_ok else 'FAIL'}  resolved_at from action_time -> {ts.isoformat()}")

    print(f"\n{len(CASES) + 1 - failures}/{len(CASES) + 1} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
