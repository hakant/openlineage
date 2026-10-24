#!/usr/bin/env bash
# Run the dbt project through the OpenLineage wrapper `dbt-ol`.
#
#   ./run_dbt_ol.sh               plain seed/run/docs generate, then dbt-ol seed, run, test
#   ./run_dbt_ol.sh run test      only the given dbt commands (each through dbt-ol)
#
# dbt-ol wraps the normal dbt CLI and, once the command is finished, converts
# target/manifest.json + target/run_results.json into OpenLineage events.
set -euo pipefail
cd "$(dirname "$0")"

export OPENLINEAGE_URL="${OPENLINEAGE_URL:-http://localhost:9000}"
export OPENLINEAGE_NAMESPACE="${OPENLINEAGE_NAMESPACE:-poc_dbt}"

# dbt-ol shells out to `dbt`, so the virtualenv must be on PATH
export PATH="$(cd .. && pwd)/.venv/bin:$PATH"
DBT_OL="dbt-ol"
COMMON=(--profiles-dir . --project-dir .)

if [ $# -gt 0 ]; then
  for cmd in "$@"; do
    echo "=== dbt-ol $cmd"
    "$DBT_OL" "$cmd" "${COMMON[@]}"
  done
  exit 0
fi

# Default flow. dbt-ol enriches events with column names and types from target/catalog.json, which
# only exists after `dbt docs generate` - and that needs the tables to exist. So we first build
# everything with plain dbt (no events), generate the catalog, and only then run through dbt-ol.
# Without a catalog, Marquez would store untyped columns and later runs would add duplicates.
echo "=== dbt seed + run + docs generate (plain dbt, no events)"
dbt seed "${COMMON[@]}"
dbt run "${COMMON[@]}"
dbt docs generate "${COMMON[@]}"

echo "=== dbt-ol seed"; "$DBT_OL" seed "${COMMON[@]}"
echo "=== dbt-ol run";  "$DBT_OL" run "${COMMON[@]}"
echo "=== dbt-ol test"; "$DBT_OL" test "${COMMON[@]}"
