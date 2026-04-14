#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LAMBDAS_DIR="${PROJECT_ROOT}/lambdas"
SHARED_DIR="${PROJECT_ROOT}/shared"

package_lambda() {
    local name="$1"
    local lambda_dir="${LAMBDAS_DIR}/${name}"
    local package_dir="${lambda_dir}/package"
    local build_dir="${package_dir}/build"
    local zip_file="${package_dir}/lambda.zip"

    if [ ! -f "${lambda_dir}/requirements.txt" ]; then
        echo "  SKIP  ${name} (no requirements.txt)"
        return
    fi

    echo "  BUILD ${name}"

    rm -rf "${package_dir}"
    mkdir -p "${build_dir}"

    python -m pip install \
        --target "${build_dir}" \
        --platform manylinux2014_x86_64 \
        --implementation cp \
        --python-version 3.12 \
        --only-binary=:all: \
        --quiet \
        -r "${lambda_dir}/requirements.txt" 2>/dev/null || \
    python -m pip install \
        --target "${build_dir}" \
        --quiet \
        -r "${lambda_dir}/requirements.txt"

    cp -r "${SHARED_DIR}" "${build_dir}/shared"

    find "${lambda_dir}" -maxdepth 1 -name "*.py" -exec cp {} "${build_dir}/" \;

    cd "${build_dir}"
    zip -r -q "${zip_file}" .
    cd "${PROJECT_ROOT}"

    rm -rf "${build_dir}"

    local size
    size=$(wc -c < "${zip_file}" | tr -d ' ')
    echo "  OK    ${name} ($(( size / 1024 )) KB)"
}

if [ $# -gt 0 ]; then
    for name in "$@"; do
        package_lambda "$name"
    done
else
    echo "Packaging all Lambdas..."
    for lambda_dir in "${LAMBDAS_DIR}"/*/; do
        name=$(basename "${lambda_dir}")
        package_lambda "$name"
    done
fi

echo "Done."
