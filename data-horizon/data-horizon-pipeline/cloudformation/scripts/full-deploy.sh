#!/usr/bin/env bash
# One-script end-to-end CloudFormation deploy for the Data Horizon pipeline.
#
# Usage:
#   ./scripts/full-deploy.sh <dev|staging|prod> [region]
#
# Optional environment variables:
#   SOURCE_API_TOKEN          — Cognito M2M token for the source mock API. Seeded into
#                               SSM SecureString on first run. Prompted (silently) if
#                               unset and the SSM param does not exist.
#   REDSHIFT_MASTER_PASSWORD  — Redshift admin password. Same seeding behavior.
#   FORCE_RESEED=1            — Re-prompt and overwrite the 2 SSM SecureString params
#                               even if they already exist.
#
# This script is the single entry point. It runs, in order:
#   1. Prereq check (aws, sam, python3, credentials)
#   2. Lambda zips built if missing or stale
#   3. Glue utils.zip built if missing
#   4. SSM SecureString params seeded if missing
#   5. Existing deploy.sh invoked (renders ASL, deploys foundation + app stacks,
#      uploads Glue scripts and Lambda zips)
#   6. Prints a ready-to-paste `aws stepfunctions start-execution` command

set -euo pipefail

# ----- Args -----
ENVIRONMENT="${1:-}"
REGION="${2:-us-east-1}"

if [[ -z "$ENVIRONMENT" ]] || [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
  echo "Usage: $0 <dev|staging|prod> [region]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PIPELINE_DIR="$(cd "$CFN_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PIPELINE_DIR/.." && pwd)"

echo "==> Environment: $ENVIRONMENT   Region: $REGION"
echo "==> Pipeline dir: $PIPELINE_DIR"
echo ""

# =============================================================================
# 1. Prereq check
# =============================================================================
echo "==> Checking prerequisites..."

missing=0
check_tool() {
  local name="$1" install_url="$2"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "  MISSING: $name — install from $install_url" >&2
    missing=1
  else
    echo "  OK: $name ($("$name" --version 2>&1 | head -1))"
  fi
}

check_tool aws     "https://aws.amazon.com/cli/"
check_tool sam     "https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"
check_tool python3 "https://www.python.org/downloads/"

if ! command -v pip >/dev/null 2>&1 && ! command -v pip3 >/dev/null 2>&1; then
  echo "  MISSING: pip / pip3 — install Python with pip enabled" >&2
  missing=1
fi

if [[ $missing -ne 0 ]]; then
  echo "" >&2
  echo "Install the missing tools above and re-run." >&2
  exit 1
fi

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "  MISSING: AWS credentials — run 'aws configure' first" >&2
  exit 1
fi
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "  OK: AWS credentials (account $ACCOUNT_ID)"
echo ""

PIP="$(command -v pip3 || command -v pip)"

# =============================================================================
# 2. Build Lambda zips if missing or stale
# =============================================================================
build_lambda_if_needed() {
  local lambda_name="$1"
  local lambda_dir="$PIPELINE_DIR/lambdas/$lambda_name"
  local pkg_dir="$lambda_dir/package"
  local zip_path="$pkg_dir/lambda.zip"

  if [[ ! -d "$lambda_dir" ]]; then
    echo "  WARN: $lambda_dir does not exist — skipping" >&2
    return
  fi

  # Rebuild if zip missing OR any source file is newer than the zip.
  local need_rebuild=0
  if [[ ! -f "$zip_path" ]]; then
    need_rebuild=1
  else
    while IFS= read -r src; do
      if [[ "$src" -nt "$zip_path" ]]; then
        need_rebuild=1
        break
      fi
    done < <(find "$lambda_dir" -maxdepth 2 \( -name '*.py' -o -name 'requirements.txt' \) -not -path "$pkg_dir/*")
    # Also check shared/
    if [[ -d "$REPO_ROOT/shared" ]]; then
      while IFS= read -r src; do
        if [[ "$src" -nt "$zip_path" ]]; then
          need_rebuild=1
          break
        fi
      done < <(find "$REPO_ROOT/shared" -name '*.py')
    fi
  fi

  if [[ $need_rebuild -eq 0 ]]; then
    echo "  SKIP: $lambda_name (zip up to date)"
    return
  fi

  echo "  BUILD: $lambda_name"
  rm -rf "$pkg_dir"
  mkdir -p "$pkg_dir"

  if [[ -f "$lambda_dir/requirements.txt" ]]; then
    "$PIP" install --quiet --no-input -r "$lambda_dir/requirements.txt" -t "$pkg_dir"
  fi

  # Copy lambda's own .py files
  find "$lambda_dir" -maxdepth 1 -name '*.py' -exec cp {} "$pkg_dir/" \;

  # Copy shared/ from repo root if present
  if [[ -d "$REPO_ROOT/shared" ]]; then
    cp -R "$REPO_ROOT/shared" "$pkg_dir/shared"
    # Drop test caches / __pycache__
    find "$pkg_dir/shared" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
  fi

  # Zip the contents of package/ (not the package/ folder itself)
  ( cd "$pkg_dir" && zip -qr lambda.zip . -x 'lambda.zip' )
  echo "         wrote $zip_path"
}

echo "==> Building Lambda zips..."
build_lambda_if_needed orchestrator
build_lambda_if_needed map_state_processor
echo ""

# =============================================================================
# 3. Build Glue utils.zip if missing
# =============================================================================
echo "==> Building Glue artifacts..."
GLUE_UTILS_DIR="$PIPELINE_DIR/glue_jobs/utils"
GLUE_UTILS_ZIP="$PIPELINE_DIR/glue_jobs/utils.zip"

if [[ -d "$GLUE_UTILS_DIR" ]]; then
  if [[ ! -f "$GLUE_UTILS_ZIP" ]]; then
    ( cd "$PIPELINE_DIR/glue_jobs" && zip -qr utils.zip utils -x '*__pycache__*' )
    echo "  BUILD: utils.zip"
  else
    echo "  SKIP: utils.zip already present"
  fi
else
  echo "  WARN: $GLUE_UTILS_DIR does not exist — Glue jobs may fail at runtime" >&2
fi
echo ""

# =============================================================================
# 4. Seed SSM SecureString params if missing
# =============================================================================
echo "==> Seeding SSM SecureString parameters..."

seed_secure_param() {
  local param_name="$1" env_var_name="$2" prompt_text="$3"
  local env_value="${!env_var_name:-}"

  # Check existence unless forced.
  if [[ "${FORCE_RESEED:-0}" != "1" ]]; then
    if aws ssm get-parameter --name "$param_name" --with-decryption --region "$REGION" >/dev/null 2>&1; then
      echo "  SKIP: $param_name already exists"
      return
    fi
  fi

  local value="$env_value"
  if [[ -z "$value" ]]; then
    echo -n "  Enter $prompt_text (input hidden): "
    read -rs value
    echo ""
    if [[ -z "$value" ]]; then
      echo "  ERROR: empty value for $param_name" >&2
      exit 1
    fi
  fi

  aws ssm put-parameter \
    --name "$param_name" \
    --type SecureString \
    --value "$value" \
    --overwrite \
    --region "$REGION" >/dev/null
  echo "  SEED: $param_name"
}

seed_secure_param "/data-horizon/$ENVIRONMENT/source-api-token"         SOURCE_API_TOKEN         "source API Cognito token"
seed_secure_param "/data-horizon/$ENVIRONMENT/redshift-master-password" REDSHIFT_MASTER_PASSWORD "Redshift master password"
echo ""

# =============================================================================
# 5. Run the main deploy
# =============================================================================
echo "==> Running deploy.sh..."
echo ""
bash "$SCRIPT_DIR/deploy.sh" "$ENVIRONMENT" "$REGION"
echo ""

# =============================================================================
# 6. Print test-run command
# =============================================================================
APP_STACK="data-horizon-app-$ENVIRONMENT"
PARENT_SM_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$APP_STACK" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='ParentStateMachineArn'].OutputValue" \
  --output text 2>/dev/null || true)

echo "==> Deploy complete."
if [[ -n "$PARENT_SM_ARN" && "$PARENT_SM_ARN" != "None" ]]; then
  echo ""
  echo "To trigger a test pipeline run:"
  echo ""
  echo "  aws stepfunctions start-execution \\"
  echo "    --state-machine-arn $PARENT_SM_ARN \\"
  echo "    --input '{\"startFrom\":\"config_loader\"}' \\"
  echo "    --region $REGION"
  echo ""
  echo "Then watch progress in the AWS Console → Step Functions."
fi
