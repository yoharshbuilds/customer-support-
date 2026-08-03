#!/usr/bin/env bash
# Run every test layer. MECE: the four layers are disjoint and together cover the
# system, so a pass here means logic, contract, numbers and render are all checked.
#
#   ./scripts/test_all.sh          # everything
#   ./scripts/test_all.sh --fast   # skip layer 3 (needs network + rate-limited API)
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
export PYTHONPATH=.
fail=0

run() {
  echo ""
  echo "=============================================================="
  echo "  $1"
  echo "=============================================================="
  shift
  if "$@" 2>&1 | tail -n "${TAIL:-6}"; then :; else fail=1; fi
  # tail swallows the exit code, so re-check explicitly
  "$@" >/dev/null 2>&1 || { echo "  ^ FAILED"; fail=1; }
}

run "layer 1a: pure logic (issues, orders maths, extract parsing, reconcile)" $PY -m scripts.test_units
run "layer 1b: availability sessions and shift-day boundaries"               $PY -m scripts.test_availability
run "layer 1c: documented Freshchat webhook payloads"                        $PY -m scripts.test_webhook_parse
run "layer 2:  API contract, validation bounds, response shape"              $PY -m scripts.test_api
run "layer 4:  UI render edge cases and animation standards"                 node scripts/test_ui.mjs

if [ "${1:-}" != "--fast" ]; then
  run "layer 3:  data integrity vs Metabase and Freshchat (network)"         $PY -m scripts.test_integrity --days 3
else
  echo ""
  echo "  layer 3 (data integrity) SKIPPED -- pass no arguments to include it"
fi

echo ""
if [ "$fail" -eq 0 ]; then
  echo "ALL LAYERS PASSED"
else
  echo "SOME LAYERS FAILED"
fi
exit $fail
