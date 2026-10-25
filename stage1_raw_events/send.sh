#!/usr/bin/env bash
# Send hand-written OpenLineage events to Marquez with curl.
#
#   ./send.sh                 first pass: ingest -> stg -> daily revenue (FAILS), parent run FAILS
#   ./send.sh --schema-change ingest re-run with a new column (new dataset version)
#   ./send.sh --rerun         successful re-run of build_daily_revenue under a new parent run
#   ./send.sh 01 02           only the events with these number prefixes (e.g. just the ingest step)
#   ./send.sh --dry-run       print rendered events instead of sending them
#
# Run IDs are generated per invocation, so START and COMPLETE of one run must be sent in the
# same call (`./send.sh 01 02`, not `./send.sh 01` and later `./send.sh 02`).
#
# Placeholders in events/*.json are replaced at send time:
#   __NOW__            current UTC time
#   __RUN_*__          a fresh UUID per placeholder name (shared by START/COMPLETE of the same run)
set -euo pipefail

cd "$(dirname "$0")"
URL="${MARQUEZ_URL:-http://localhost:9000}/api/v1/lineage"
DRY_RUN=0
PICKED=()
FILES=(events/01_*.json events/02_*.json events/03_*.json events/04_*.json events/05_*.json events/06_*.json events/07_*.json events/08_*.json)

for arg in "$@"; do
  case "$arg" in
    --schema-change) FILES=(events/09_*.json) ;;
    --rerun)         FILES=(events/10_*.json events/11_*.json events/12_*.json events/13_*.json) ;;
    --dry-run)       DRY_RUN=1 ;;
    [0-9][0-9])      PICKED+=(events/${arg}_*.json) ;;
    *) echo "unknown option: $arg" >&2; exit 1 ;;
  esac
done
[ ${#PICKED[@]} -gt 0 ] && FILES=("${PICKED[@]}")

new_uuid() { uuidgen | tr '[:upper:]' '[:lower:]'; }
RUN_INGEST=$(new_uuid); RUN_INGEST2=$(new_uuid); RUN_STG=$(new_uuid)
RUN_REVENUE=$(new_uuid); RUN_REVENUE2=$(new_uuid); RUN_PARENT=$(new_uuid); RUN_PARENT2=$(new_uuid)

for f in "${FILES[@]}"; do
  now=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
  body=$(sed \
    -e "s/__NOW__/$now/g" \
    -e "s/__RUN_INGEST2__/$RUN_INGEST2/g" \
    -e "s/__RUN_INGEST__/$RUN_INGEST/g" \
    -e "s/__RUN_STG__/$RUN_STG/g" \
    -e "s/__RUN_REVENUE2__/$RUN_REVENUE2/g" \
    -e "s/__RUN_REVENUE__/$RUN_REVENUE/g" \
    -e "s/__RUN_PARENT2__/$RUN_PARENT2/g" \
    -e "s/__RUN_PARENT__/$RUN_PARENT/g" \
    "$f")
  if [ "$DRY_RUN" = 1 ]; then
    echo "--- $f"; echo "$body"; continue
  fi
  status=$(curl -s -o /tmp/ol_response.txt -w "%{http_code}" -X POST "$URL" \
    -H 'Content-Type: application/json' -d "$body")
  echo "$f -> HTTP $status"
  [ "$status" -ge 300 ] && { cat /tmp/ol_response.txt; echo; exit 1; }
  sleep 2   # makes run durations visible in the UI
done
