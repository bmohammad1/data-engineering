#!/usr/bin/env bash
set -euo pipefail

ENV="${1:?Usage: bash scripts/run_redshift_migrations.sh <env>}"

# Resolve project root relative to this script's location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER_ID="data-horizon-${ENV}-cluster"
DATABASE="datahorizon"
DB_USER="admin"
REGION="us-east-1"
MIGRATIONS_DIR="${PROJECT_ROOT}/redshift/migrations"
POLL_INTERVAL=5

echo "Running Redshift migrations against ${CLUSTER_ID}..."

for sql_file in "${MIGRATIONS_DIR}"/*.sql; do
    filename=$(basename "$sql_file")

    # Skip empty files
    if [ ! -s "$sql_file" ]; then
        echo "  SKIP  ${filename} (empty)"
        continue
    fi

    echo "  RUN   ${filename}"
    sql_content=$(cat "$sql_file")

    # Execute the SQL via Redshift Data API
    statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --db-user "$DB_USER" \
        --region "$REGION" \
        --sql "$sql_content" \
        --query "Id" \
        --output text)

    # Poll until the statement finishes
    while true; do
        status=$(aws redshift-data describe-statement \
            --id "$statement_id" \
            --region "$REGION" \
            --query "Status" \
            --output text)

        case "$status" in
            FINISHED)
                echo "  OK    ${filename}"
                break
                ;;
            FAILED|ABORTED)
                error=$(aws redshift-data describe-statement \
                    --id "$statement_id" \
                    --region "$REGION" \
                    --query "Error" \
                    --output text)
                echo "  FAIL  ${filename}: ${error}"
                exit 1
                ;;
            *)
                sleep "$POLL_INTERVAL"
                ;;
        esac
    done
done

echo "All migrations completed."
