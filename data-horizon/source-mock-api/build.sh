#!/usr/bin/env bash
# Builds a Lambda deployment zip with Linux-compatible dependencies.
# Safe to run from any OS (Windows/macOS/Linux).
set -euo pipefail

BUILD_DIR="build/package"
ZIP_PATH="build/lambda.zip"

rm -rf build
mkdir -p "$BUILD_DIR"

# Install dependencies targeting the Lambda runtime (Amazon Linux x86_64).
# --only-binary ensures we never compile from source on the wrong platform.
pip install \
  --target "$BUILD_DIR" \
  -r requirements-lock.txt \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --no-cache-dir \
  --quiet

# Copy application code
cp -r app "$BUILD_DIR/app"
cp lambda_handler.py "$BUILD_DIR/"

# Remove bytecode and test bloat before zipping
find "$BUILD_DIR" -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name '*.dist-info' -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name 'tests' -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -name '*.pyc' -delete 2>/dev/null || true

# Create zip — use Python's zipfile since zip may not be available on Windows
python -c "
import zipfile, pathlib, sys
src = pathlib.Path('$BUILD_DIR')
with zipfile.ZipFile('$ZIP_PATH', 'w', zipfile.ZIP_DEFLATED) as zf:
    for f in sorted(src.rglob('*')):
        if f.is_file():
            zf.write(f, f.relative_to(src))
print(f'Built: $ZIP_PATH ({pathlib.Path(\"$ZIP_PATH\").stat().st_size / 1024:.0f} KB)')
"
