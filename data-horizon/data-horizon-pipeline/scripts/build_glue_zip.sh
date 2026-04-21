#!/usr/bin/env bash
# Build utils.zip — the --extra-py-files package loaded by both Glue jobs.
#
# The zip contains two packages at its root:
#   utils/   — schema_definitions, spark_helpers, validation_rules, dynamodb_updater
#   shared/  — aws_clients, constants, exceptions, logger
#
# Glue extracts this zip and adds it to sys.path so that:
#   from utils.schema_definitions import TABLE_SCHEMAS
#   from shared.constants import STATUS_SUCCESS
# both resolve correctly inside the job.
#
# Usage:
#   bash scripts/build_glue_zip.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

UTILS_SRC="${PROJECT_ROOT}/glue_jobs/utils"
SHARED_SRC="${PROJECT_ROOT}/shared"
BUILD_DIR="${PROJECT_ROOT}/glue_jobs/.zip_build"
OUT_ZIP="${PROJECT_ROOT}/glue_jobs/utils.zip"

echo "Building utils.zip..."

# Clean and recreate staging area
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/utils" "${BUILD_DIR}/shared"

# Copy utils package (exclude __pycache__ and test files)
cp "${UTILS_SRC}"/*.py "${BUILD_DIR}/utils/"

# Copy shared package
cp "${SHARED_SRC}"/*.py "${BUILD_DIR}/shared/"

# Build the zip from the staging area so paths inside are utils/ and shared/
rm -f "${OUT_ZIP}"

if command -v zip &>/dev/null; then
  cd "${BUILD_DIR}"
  zip -r -q "${OUT_ZIP}" utils/ shared/
  cd "${PROJECT_ROOT}"
elif command -v python &>/dev/null; then
  python - "${BUILD_DIR}" "${OUT_ZIP}" <<'EOF'
import sys, os, zipfile

build_dir = sys.argv[1]
out_zip   = sys.argv[2]

with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
    for package in ("utils", "shared"):
        pkg_dir = os.path.join(build_dir, package)
        for fname in os.listdir(pkg_dir):
            if fname.endswith(".py"):
                zf.write(os.path.join(pkg_dir, fname), os.path.join(package, fname))
EOF
elif command -v powershell &>/dev/null; then
  powershell -Command "
    \$src = '${BUILD_DIR}'; \$dst = '${OUT_ZIP}'
    if (Test-Path \$dst) { Remove-Item \$dst -Force }
    Add-Type -Assembly 'System.IO.Compression.FileSystem'
    [System.IO.Compression.ZipFile]::CreateFromDirectory(\$src, \$dst)
  "
else
  echo "ERROR: No zip tool found (zip, python, or powershell required)"
  rm -rf "${BUILD_DIR}"
  exit 1
fi

rm -rf "${BUILD_DIR}"

SIZE=$(wc -c < "${OUT_ZIP}" | tr -d ' ')
echo "Built ${OUT_ZIP} ($(( SIZE / 1024 )) KB)"
echo "  utils/schema_definitions.py"
echo "  utils/spark_helpers.py"
echo "  utils/validation_rules.py"
echo "  utils/dynamodb_updater.py"
echo "  shared/aws_clients.py"
echo "  shared/constants.py"
echo "  shared/exceptions.py"
echo "  shared/logger.py"
