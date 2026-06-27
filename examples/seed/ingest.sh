#!/usr/bin/env bash
#
# Seed a running headwaters with rich demo lineage.
#
# Usage:
#   ./ingest.sh                       # ingest all bundled datasets at :8091
#   MARQUEZ_URL=http://host:8091 ./ingest.sh
#   ./ingest.sh headwaters_demo.json  # ingest specific file(s) only
#
# Start the server first (needs a Postgres DSN):
#   just lineage          # (DATABASE_URL=postgres://… in env or config)
#
# Each file is a JSON array of OpenLineage events posted to the batch endpoint
# (POST /api/v1/lineage/batch). Per-event failures are reported, not fatal.

set -euo pipefail

URL="${MARQUEZ_URL:-${LINEAGE_URL:-http://localhost:8091}}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Files to ingest, in order (the demo graph first so its namespaces sort first).
if [[ $# -gt 0 ]]; then
  FILES=("$@")
else
  FILES=("headwaters_demo.json" "marquez_food_delivery.json")
fi

have() { command -v "$1" >/dev/null 2>&1; }
have curl || { echo "error: curl is required" >&2; exit 1; }

echo "→ target: ${URL}"

# Wait for the service to be healthy (up to ~30s) so this is safe to run right
# after `just lineage` in a script.
echo -n "→ waiting for /health "
for _ in $(seq 1 30); do
  if curl -fsS "${URL}/health" >/dev/null 2>&1; then
    echo "ok"
    break
  fi
  echo -n "."
  sleep 1
done
if ! curl -fsS "${URL}/health" >/dev/null 2>&1; then
  echo
  echo "error: ${URL}/health never came up — is the server running? (just lineage)" >&2
  exit 1
fi

total_ok=0
total_fail=0
for name in "${FILES[@]}"; do
  # Allow either a bare name (resolved next to this script) or a path.
  file="${name}"
  [[ -f "${file}" ]] || file="${HERE}/${name}"
  if [[ ! -f "${file}" ]]; then
    echo "!! skipping ${name}: not found" >&2
    continue
  fi

  # Regenerate the headwaters demo from source if the generator is present, so
  # `ingest.sh` always sends the current graph.
  if [[ "$(basename "${file}")" == "headwaters_demo.json" && -f "${HERE}/generate.py" ]] && have python3; then
    python3 "${HERE}/generate.py" -o "${file}"
  fi

  count=$(grep -o '"eventType"' "${file}" | wc -l | tr -d ' ')
  echo "→ ingesting $(basename "${file}") (~${count} events) …"

  resp="$(curl -fsS -X POST "${URL}/api/v1/lineage/batch" \
            -H 'Content-Type: application/json' \
            --data-binary "@${file}")"

  # Parse the batch summary once. Falls back to a raw dump without python.
  # The response is passed via $RESP (not stdin) so the heredoc owns stdin.
  if have python3; then
    counts="$(RESP="${resp}" python3 - <<'PY'
import json, os, sys
r = json.loads(os.environ["RESP"])
s = r.get("summary", {})
ok = s.get("successful", 0)
fail = s.get("failed", 0)
print(f"   status={r.get('status')} received={s.get('received')} "
      f"successful={ok} failed={fail}", file=sys.stderr)
for fe in r.get("failed_events", [])[:10]:
    print(f"     ! [{fe.get('index')}] {fe.get('reason')}", file=sys.stderr)
print(ok, fail)  # stdout: machine-readable tally for the wrapper
PY
)"
    read -r ok fail <<<"${counts:-0 0}"
    total_ok=$((total_ok + ok))
    total_fail=$((total_fail + fail))
  else
    echo "   ${resp}"
  fi
done

echo
echo "✓ done — ${total_ok} events ingested, ${total_fail} failed."
echo "  Explore the UI: just ui-dev   (expects the service on ${URL})"
