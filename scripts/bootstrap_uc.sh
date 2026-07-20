#!/usr/bin/env bash
# Bootstrap the Unity Catalog objects the `meridian` (serverless / uc_volume) bundle target needs
# on a Default-Storage workspace that has no ADLS -- the managed equivalent of what Terraform's
# unity-catalog + storage modules provision for the Azure targets.
#
# Idempotent: every statement is IF NOT EXISTS. Safe to re-run.
#
# Usage:
#   scripts/bootstrap_uc.sh <profile> <sql_warehouse_id>
# e.g.
#   scripts/bootstrap_uc.sh meridian-dev 59901b31d31db40a
#
# Note for Git Bash on Windows: export MSYS_NO_PATHCONV=1 first, or the leading-slash API paths
# get rewritten into Windows paths.
set -euo pipefail

PROFILE="${1:?usage: bootstrap_uc.sh <profile> <warehouse_id>}"
WAREHOUSE_ID="${2:?usage: bootstrap_uc.sh <profile> <warehouse_id>}"
CATALOG="ecom_dev"

run_sql() {
  local stmt="$1"
  local body
  body="$(python -c "import json,sys; print(json.dumps({'warehouse_id':sys.argv[1],'statement':sys.argv[2],'wait_timeout':'50s'}))" "$WAREHOUSE_ID" "$stmt")"
  echo "  $stmt"
  echo "$body" | databricks api post /api/2.0/sql/statements/ --json @/dev/stdin -p "$PROFILE" \
    | python -c "import json,sys; s=json.load(sys.stdin).get('status',{}); print('    ->', s.get('state'), s.get('error',{}).get('message',''))"
}

echo "Bootstrapping $CATALOG on profile $PROFILE ..."
run_sql "CREATE CATALOG IF NOT EXISTS $CATALOG COMMENT 'Ecom signal platform -- serverless/UC dev variant'"
run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.bronze"
run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.silver"
run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.gold"
run_sql "CREATE VOLUME IF NOT EXISTS $CATALOG.bronze.ops COMMENT 'Raw file landing + streaming checkpoints (uc_volume backend)'"
echo "Done. Upload source CSVs to /Volumes/$CATALOG/bronze/ops/bronze/raw/orders/ before running the batch job."
