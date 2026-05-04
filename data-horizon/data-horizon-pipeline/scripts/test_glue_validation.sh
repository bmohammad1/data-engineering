#!/usr/bin/env bash
# Prepare the test environment for the Glue validation job.
#
# Prerequisites:
#   1. terraform apply completed in terraform/test-glue/
#   2. test_glue_transform.sh has been run and the transform job
#      completed successfully for run_id=test-run-001
#
# What it does:
#   1. Reads bucket/job names from Terraform outputs
#   2. Uploads validation_job.py and utils.zip to the scripts bucket
#   3. Seeds the DynamoDB TAG# and META items that fetch_transform_succeeded_tags
#      and update_run_validate_status depend on
#
# After this script completes, trigger the job manually in the Glue console.
# Set job parameter --run_id = test-run-001
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TF_DIR="${PROJECT_ROOT}/terraform/test-glue"

RUN_ID="test-run-001"
TAG_ID="TAG-001"

# ---------------------------------------------------------------------------
# Read Terraform outputs
# ---------------------------------------------------------------------------

echo "Reading Terraform outputs..."
cd "${TF_DIR}"

SCRIPTS_BUCKET=$(terraform output -raw scripts_bucket_name)
DYNAMO_TABLE=$(terraform output -raw 2>/dev/null | grep -v "^$" || true)
VALIDATED_BUCKET=$(terraform output -raw validated_bucket_name)
QUARANTINE_BUCKET=$(terraform output -raw quarantine_bucket_name)
VALIDATION_JOB=$(terraform output -raw validation_glue_job_name)

# DynamoDB table name is not a Terraform output — derive it from the known naming pattern.
# The table is always named "${local.prefix}-pipeline-state" where prefix = "data-horizon-test".
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DYNAMO_TABLE="data-horizon-test-pipeline-state"

cd "${PROJECT_ROOT}"

echo "  Scripts bucket  : ${SCRIPTS_BUCKET}"
echo "  Validated bucket: ${VALIDATED_BUCKET}"
echo "  Quarantine bucket: ${QUARANTINE_BUCKET}"
echo "  Validation job  : ${VALIDATION_JOB}"
echo "  DynamoDB table  : ${DYNAMO_TABLE}"
echo ""

# ---------------------------------------------------------------------------
# Upload validation_job.py and utils.zip
# ---------------------------------------------------------------------------

echo "Uploading validation_job.py..."
aws s3 cp \
  "${PROJECT_ROOT}/glue_jobs/scripts/validation_job.py" \
  "s3://${SCRIPTS_BUCKET}/scripts/validation_job.py"

echo "Uploading utils.zip..."
aws s3 cp \
  "${PROJECT_ROOT}/glue_jobs/utils.zip" \
  "s3://${SCRIPTS_BUCKET}/scripts/utils.zip"

echo ""

# ---------------------------------------------------------------------------
# Seed DynamoDB — TAG# item
#
# fetch_transform_succeeded_tags queries:
#   PK = RUN#<run_id>
#   SK begins_with TAG#
#   FilterExpression: overall_status = SUCCESS
#
# Without this item the validation job finds 0 tags, does all the table work,
# but writes no per-tag DynamoDB outcomes and logs no tag counts.
# ---------------------------------------------------------------------------

echo "Seeding DynamoDB TAG# item for ${TAG_ID}..."
aws dynamodb put-item \
  --table-name "${DYNAMO_TABLE}" \
  --item '{
    "PK":             {"S": "RUN#'"${RUN_ID}"'"},
    "SK":             {"S": "TAG#'"${TAG_ID}"'"},
    "overall_status": {"S": "SUCCESS"},
    "stage_status":   {"M": {"TRANSFORM": {"S": "SUCCESS"}}}
  }'

# ---------------------------------------------------------------------------
# Seed DynamoDB — META item
#
# update_run_validate_status calls update_item on PK=RUN#<run_id>, SK=META.
# DynamoDB update_item creates the item if it does not exist, so this seed is
# not strictly required — but seeding it avoids a sparse item on first write
# and ensures validate_status can be read back cleanly after the job.
# ---------------------------------------------------------------------------

echo "Seeding DynamoDB META item..."
aws dynamodb put-item \
  --table-name "${DYNAMO_TABLE}" \
  --item '{
    "PK":              {"S": "RUN#'"${RUN_ID}"'"},
    "SK":              {"S": "META"},
    "overall_status":  {"S": "SUCCESS"},
    "transform_status":{"S": "SUCCESS"}
  }'

echo ""
echo "========================================"
echo " Setup complete."
echo ""
echo " Next: open the Glue console and run:"
echo "   Job name : ${VALIDATION_JOB}"
echo "   --run_id : ${RUN_ID}"
echo ""
echo " After the job succeeds, verify output:"
echo ""
echo "   # Valid records written to validated bucket:"
echo "   aws s3 ls s3://${VALIDATED_BUCKET}/validated/${RUN_ID}/ --recursive"
echo ""
echo "   # Quarantined records (expect empty for clean test data):"
echo "   aws s3 ls s3://${QUARANTINE_BUCKET}/quarantine/${RUN_ID}/ --recursive"
echo ""
echo "   # Check per-run DynamoDB outcome:"
echo "   aws dynamodb get-item \\"
echo "     --table-name ${DYNAMO_TABLE} \\"
echo "     --key '{\"PK\":{\"S\":\"RUN#${RUN_ID}\"},\"SK\":{\"S\":\"META\"}}' \\"
echo "     --query 'Item.{status:validate_status.S,valid:validate_records_passed.N,rejected:validate_records_quarantined.N}'"
echo ""
echo "   # Check per-tag DynamoDB outcome:"
echo "   aws dynamodb get-item \\"
echo "     --table-name ${DYNAMO_TABLE} \\"
echo "     --key '{\"PK\":{\"S\":\"RUN#${RUN_ID}\"},\"SK\":{\"S\":\"TAG#${TAG_ID}\"}}' \\"
echo "     --query 'Item.{status:overall_status.S,valid:validate_records_passed.N,quarantined:validate_records_quarantined.N}'"
echo "========================================"
