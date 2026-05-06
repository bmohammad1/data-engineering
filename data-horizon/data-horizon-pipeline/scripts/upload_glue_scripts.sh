#!/usr/bin/env bash
# Upload Glue job scripts and the utils/shared zip to the scripts S3 bucket.
#
# Usage:
#   bash scripts/upload_glue_scripts.sh <env>
#
# Example:
#   bash scripts/upload_glue_scripts.sh dev
#   bash scripts/upload_glue_scripts.sh staging
set -euo pipefail

ENV="${1:-dev}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GLUE_SCRIPTS_DIR="${PROJECT_ROOT}/glue_jobs/scripts"
UTILS_ZIP="${PROJECT_ROOT}/glue_jobs/utils.zip"

# ---------------------------------------------------------------------------
# Resolve the scripts bucket from Terraform output
# ---------------------------------------------------------------------------

echo "Resolving scripts bucket for environment: ${ENV}"

SCRIPTS_BUCKET=$(
  cd "${PROJECT_ROOT}/terraform" && \
  terraform output -raw s3_scripts_bucket_name
)

if [ -z "${SCRIPTS_BUCKET}" ]; then
  echo "ERROR: Could not resolve s3_scripts_bucket_name from Terraform outputs."
  echo "       Run 'terraform apply' for the '${ENV}' environment first."
  exit 1
fi

echo "Scripts bucket: ${SCRIPTS_BUCKET}"

# ---------------------------------------------------------------------------
# Upload job scripts
# ---------------------------------------------------------------------------

echo "Uploading transform_job.py..."
aws s3 cp \
  "${GLUE_SCRIPTS_DIR}/transform_job.py" \
  "s3://${SCRIPTS_BUCKET}/scripts/transform_job.py"

echo "Uploading validation_job.py..."
aws s3 cp \
  "${GLUE_SCRIPTS_DIR}/validation_job.py" \
  "s3://${SCRIPTS_BUCKET}/scripts/validation_job.py"

# ---------------------------------------------------------------------------
# Upload utils zip (built by build_glue_zip.sh)
# ---------------------------------------------------------------------------

if [ ! -f "${UTILS_ZIP}" ]; then
  echo "ERROR: utils.zip not found at ${UTILS_ZIP}"
  echo "       Run 'bash scripts/build_glue_zip.sh' first."
  exit 1
fi

echo "Uploading utils.zip..."
aws s3 cp \
  "${UTILS_ZIP}" \
  "s3://${SCRIPTS_BUCKET}/scripts/utils.zip"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "Glue scripts uploaded to s3://${SCRIPTS_BUCKET}/scripts/"
echo "  transform_job.py"
echo "  validation_job.py"
echo "  utils.zip"
