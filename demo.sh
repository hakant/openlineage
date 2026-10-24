#!/usr/bin/env bash
# One-shot helper: start the stack and populate Marquez with data from all stages.
#
#   ./demo.sh           start the stack (if needed) and run every stage once
#   ./demo.sh --reset   wipe Marquez + warehouse volumes first (clean slate)
set -euo pipefail
cd "$(dirname "$0")"

# Docker Desktop's CLI is not always on PATH
command -v docker >/dev/null 2>&1 || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
API=http://localhost:9000/api/v1

if [ "${1:-}" = "--reset" ]; then
  echo ">>> resetting stack and volumes"
  docker compose down -v
fi

echo ">>> starting stack"
docker compose up -d
echo -n ">>> waiting for Marquez API "
until curl -sf "$API/namespaces" >/dev/null; do echo -n "."; sleep 3; done
echo " ready"

echo ">>> stage 1: hand-written events"
./stage1_raw_events/send.sh
./stage1_raw_events/send.sh --schema-change
# (the failed build_daily_revenue run stays the latest on purpose; fix it later with send.sh --rerun)

echo ">>> stage 2: python pipeline (ok, schema change, failure - the failure is left as the latest run)"
uv run python stage2_python_pipeline/pipeline.py
uv run python stage2_python_pipeline/pipeline.py --schema-change
uv run python stage2_python_pipeline/pipeline.py --fail-at clean_orders || true

echo ">>> stage 3: dbt"
until docker exec warehouse-db pg_isready -U warehouse >/dev/null 2>&1; do sleep 2; done
./stage3_dbt/run_dbt_ol.sh

echo ">>> stage 4: tag PII"
curl -s -o /dev/null -X PUT "$API/tags/PII" -H 'Content-Type: application/json' \
  -d '{"description":"Personally identifiable information"}'
curl -s -o /dev/null -X POST \
  "$API/namespaces/postgres%3A%2F%2Flocalhost%3A5433/datasets/warehouse.public.stg_customers/fields/email/tags/PII"

echo ">>> done: open http://localhost:3000"
