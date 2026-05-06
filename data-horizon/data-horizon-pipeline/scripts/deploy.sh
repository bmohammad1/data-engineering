#!/usr/bin/env bash
# Build and upload all deployable artifacts.
#
# Run this in two phases:
#
#   Phase 1 — before terraform apply (builds artifacts locally):
#     bash scripts/deploy.sh <env> build
#
#   Phase 2 — after terraform apply (uploads artifacts to S3):
#     bash scripts/deploy.sh <env> upload
#
# Example:
#   bash scripts/deploy.sh staging build
#   cd terraform && terraform apply -var-file=environments/staging/terraform.tfvars
#   bash scripts/deploy.sh staging upload
set -euo pipefail

ENV="${1:-dev}"
PHASE="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${PHASE}" ]; then
  echo "Usage: bash scripts/deploy.sh <env> <build|upload>"
  echo ""
  echo "  build   — package Lambdas + build Glue utils.zip (run before terraform apply)"
  echo "  upload  — upload Glue scripts to S3             (run after  terraform apply)"
  exit 1
fi

echo "========================================"
echo " Data Horizon — ${PHASE}"
echo " Environment : ${ENV}"
echo "========================================"
echo ""

case "${PHASE}" in
  build)
    echo "[1/2] Packaging Lambda functions..."
    bash "${SCRIPT_DIR}/package_lambdas.sh"
    echo ""

    echo "[2/2] Building Glue utils.zip..."
    bash "${SCRIPT_DIR}/build_glue_zip.sh"
    echo ""

    echo "========================================"
    echo " Build complete. Next:"
    echo ""
    echo "   cd terraform"
    echo "   terraform apply -var-file=environments/${ENV}/terraform.tfvars"
    echo ""
    echo "   Then: bash scripts/deploy.sh ${ENV} upload"
    echo "========================================"
    ;;

  upload)
    echo "[1/1] Uploading Glue scripts to S3..."
    bash "${SCRIPT_DIR}/upload_glue_scripts.sh" "${ENV}"
    echo ""

    echo "========================================"
    echo " Upload complete. Pipeline is ready."
    echo "========================================"
    ;;

  *)
    echo "ERROR: Unknown phase '${PHASE}'. Use 'build' or 'upload'."
    exit 1
    ;;
esac
