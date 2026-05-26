#!/usr/bin/env bash
# Deploy Data Horizon CloudFormation — two-pass (foundation, then app) using the
# AWS CLI directly (aws cloudformation package + deploy). No SAM dependency.
#
# Usage: deploy.sh <dev|staging|prod> [region]

set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${2:-us-east-1}"

if [[ -z "$ENVIRONMENT" ]] || [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
  echo "Usage: $0 <dev|staging|prod> [region]" >&2; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PIPELINE_DIR="$(cd "$CFN_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PIPELINE_DIR/.." && pwd)"

PARAMS_FILE="$CFN_DIR/params/${ENVIRONMENT}.json"
[[ -f "$PARAMS_FILE" ]] || { echo "Params file not found: $PARAMS_FILE" >&2; exit 1; }

# Resolve Python if not already exported by full-deploy.sh.
if [[ -z "${PYTHON:-}" ]]; then
  for candidate in python3 python py; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" --version >/dev/null 2>&1; then
      PYTHON="$candidate"; break
    fi
  done
  [[ -n "${PYTHON:-}" ]] || { echo "ERROR: no working python interpreter found" >&2; exit 1; }
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ACCOUNT_SUFFIX="${ACCOUNT_ID: -8}"
FOUNDATION_STACK="data-horizon-foundation-${ENVIRONMENT}"
APP_STACK="data-horizon-app-${ENVIRONMENT}"

# Shared artifact bucket for CFN packaged templates (auto-created if missing).
PACKAGE_BUCKET="data-horizon-cfn-artifacts-${ACCOUNT_ID}-${REGION}"
if ! aws s3api head-bucket --bucket "$PACKAGE_BUCKET" --region "$REGION" 2>/dev/null; then
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$PACKAGE_BUCKET" --region "$REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$PACKAGE_BUCKET" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
  fi
  aws s3api put-bucket-encryption --bucket "$PACKAGE_BUCKET" \
    --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
  aws s3api put-public-access-block --bucket "$PACKAGE_BUCKET" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null
  echo "Created CFN artifact bucket: $PACKAGE_BUCKET"
fi

echo "Account: $ACCOUNT_ID  AccountSuffix: $ACCOUNT_SUFFIX"
echo "Region: $REGION  Environment: $ENVIRONMENT"
echo "Foundation stack: $FOUNDATION_STACK"
echo "App stack:        $APP_STACK"
echo ""

# A failed initial create leaves the stack in ROLLBACK_COMPLETE, which CFN
# refuses to update — the only path forward is to delete and recreate.
clean_stuck_stack() {
  local stack="$1"
  local status
  status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$REGION" \
    --query "Stacks[0].StackStatus" --output text 2>/dev/null || true)
  if [[ "$status" == "ROLLBACK_COMPLETE" || "$status" == "ROLLBACK_FAILED" || "$status" == "CREATE_FAILED" ]]; then
    echo "Stack $stack is in $status — deleting before redeploy..."
    aws cloudformation delete-stack --stack-name "$stack" --region "$REGION"
    aws cloudformation wait stack-delete-complete --stack-name "$stack" --region "$REGION"
    echo "Deleted $stack."
  fi
}
clean_stuck_stack "$FOUNDATION_STACK"
clean_stuck_stack "$APP_STACK"

echo "Rendering Step Functions template from ASL JSON..."
"$PYTHON" "$SCRIPT_DIR/render-stepfunctions.py"
echo ""

# Build parameter overrides into a NUL-delimited list (so values with spaces,
# e.g. "rate(6 hours)", survive into bash without being word-split).
# Usage: read array via `mapfile -d '' arr < <(build_overrides ...extras)`.
build_overrides() {
  "$PYTHON" - "$PARAMS_FILE" "$ACCOUNT_SUFFIX" "$@" <<'PY'
import json, sys
params_file, account_suffix, *extras = sys.argv[1:]
with open(params_file) as f: params = json.load(f)
pairs = [f"AccountSuffix={account_suffix}"]
for k, v in params.items(): pairs.append(f"{k}={v}")
for e in extras: pairs.append(e)
sys.stdout.write("\0".join(pairs) + "\0")
PY
}

# ==========================================================================
# Pass A — Foundation
# ==========================================================================
echo "===== Pass A: Foundation ====="
mapfile -d '' FOUNDATION_OVERRIDES < <(build_overrides)

echo "Packaging foundation template..."
FOUNDATION_PACKAGED="$CFN_DIR/parent-foundation.packaged.yaml"
aws cloudformation package \
  --template-file "$CFN_DIR/parent-foundation.yaml" \
  --s3-bucket "$PACKAGE_BUCKET" \
  --s3-prefix "cfn-templates/foundation" \
  --output-template-file "$FOUNDATION_PACKAGED" \
  --region "$REGION" >/dev/null

aws cloudformation deploy \
  --template-file "$FOUNDATION_PACKAGED" \
  --stack-name "$FOUNDATION_STACK" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --no-fail-on-empty-changeset \
  --parameter-overrides "${FOUNDATION_OVERRIDES[@]}"

# Read foundation outputs. Parse JSON via python so we don't have to deal with
# the AWS CLI's tab/CRLF text format quirks on Windows.
echo ""
echo "Reading foundation outputs..."
declare -A OUT
OUTPUTS_JSON=$(aws cloudformation describe-stacks --stack-name "$FOUNDATION_STACK" --region "$REGION" \
  --query "Stacks[0].Outputs" --output json)

while IFS=$'\t' read -r k v; do
  # Strip any trailing CR (Python on Windows emits \r\n line endings).
  k="${k%$'\r'}"; v="${v%$'\r'}"
  [[ -n "$k" ]] && OUT[$k]="$v"
done < <("$PYTHON" -c "
import json, sys
sys.stdout.reconfigure(newline='\n')
for o in json.loads(sys.argv[1]):
    sys.stdout.write(f\"{o['OutputKey']}\t{o['OutputValue']}\n\")
" "$OUTPUTS_JSON")

SCRIPTS_BUCKET="${OUT[ScriptsBucketName]:-}"
CONFIG_BUCKET="${OUT[ConfigBucketName]:-}"
echo "Scripts bucket: $SCRIPTS_BUCKET"
echo "Config bucket:  $CONFIG_BUCKET"

if [[ -z "$SCRIPTS_BUCKET" || -z "$CONFIG_BUCKET" ]]; then
  echo "ERROR: failed to read bucket names from foundation outputs." >&2
  echo "OUT keys: ${!OUT[*]}" >&2
  exit 1
fi

# put-object uses --bucket and --key as separate args, sidestepping the
# Git Bash/MSYS path-conversion bug that mangles s3:// URIs on Windows.
put_object() {
  local file="$1" bucket="$2" key="$3"
  echo "  put_object: file='$file' bucket='$bucket' key='$key' region='$REGION'"
  if [[ -z "$file" || -z "$bucket" || -z "$key" || -z "$REGION" ]]; then
    echo "ERROR: put_object called with an empty argument" >&2
    return 1
  fi
  if [[ ! -f "$file" ]]; then
    echo "ERROR: put_object body file not found: $file" >&2
    return 1
  fi
  aws s3api put-object --bucket "$bucket" --key "$key" --body "$file" --region "$REGION" >/dev/null
}

# Upload Glue scripts.
TRANSFORM="$PIPELINE_DIR/glue_jobs/scripts/transform_job.py"
VALIDATION="$PIPELINE_DIR/glue_jobs/scripts/validation_job.py"
UTILS_ZIP="$PIPELINE_DIR/glue_jobs/utils.zip"
[[ -f "$TRANSFORM"  ]] && put_object "$TRANSFORM"  "$SCRIPTS_BUCKET" "scripts/transform_job.py"  && echo "Uploaded transform_job.py"
[[ -f "$VALIDATION" ]] && put_object "$VALIDATION" "$SCRIPTS_BUCKET" "scripts/validation_job.py" && echo "Uploaded validation_job.py"
[[ -f "$UTILS_ZIP"  ]] && put_object "$UTILS_ZIP"  "$SCRIPTS_BUCKET" "scripts/utils.zip"         && echo "Uploaded utils.zip"

# source_config/ placeholder.
TMP=$(mktemp)
put_object "$TMP" "$CONFIG_BUCKET" "source_config/.placeholder"
rm -f "$TMP"
echo "Created source_config/ placeholder."

# Upload Lambda zips with content-hashed keys.
CL_ZIP="$PIPELINE_DIR/lambdas/orchestrator/package/lambda.zip"
MP_ZIP="$PIPELINE_DIR/lambdas/map_state_processor/package/lambda.zip"
[[ -f "$CL_ZIP" ]] || { echo "Missing $CL_ZIP — run scripts/package_lambdas.sh first." >&2; exit 1; }
[[ -f "$MP_ZIP" ]] || { echo "Missing $MP_ZIP — run scripts/package_lambdas.sh first." >&2; exit 1; }

CL_HASH=$(sha256sum "$CL_ZIP" | cut -c1-16)
MP_HASH=$(sha256sum "$MP_ZIP" | cut -c1-16)
CL_KEY="lambda/config-loader-$CL_HASH.zip"
MP_KEY="lambda/map-state-processor-$MP_HASH.zip"

put_object "$CL_ZIP" "$SCRIPTS_BUCKET" "$CL_KEY"
put_object "$MP_ZIP" "$SCRIPTS_BUCKET" "$MP_KEY"
echo "Uploaded Lambda zips: $CL_KEY, $MP_KEY"

# ==========================================================================
# Pass B — App
# ==========================================================================
# Compute Step Functions task-timeout (80% of the Lambda timeout, in ms).
read -r CL_TIMEOUT_MS MP_TIMEOUT_MS < <("$PYTHON" -c "
import json, sys
with open(sys.argv[1]) as f: p = json.load(f)
cl = int(int(p['ConfigLoaderTimeout']) * 1000 * 0.8)
mp = int(int(p['MapProcessorTimeout']) * 1000 * 0.8)
print(f'{cl} {mp}')
" "$PARAMS_FILE")

mapfile -d '' APP_OVERRIDES < <(build_overrides \
  "ScriptsBucketName=${OUT[ScriptsBucketName]}" \
  "ValidatedBucketArn=${OUT[ValidatedBucketArn]}" \
  "OrchestrationBucketName=${OUT[OrchestrationBucketName]}" \
  "ValidatedBucketName=${OUT[ValidatedBucketName]}" \
  "DynamoDbTableName=${OUT[DynamoDbTableName]}" \
  "DynamoDbTableArn=${OUT[DynamoDbTableArn]}" \
  "ExtractionFailuresQueueUrl=${OUT[ExtractionFailuresQueueUrl]}" \
  "ExtractionFailuresQueueName=${OUT[ExtractionFailuresQueueName]}" \
  "EventBridgeFailuresQueueArn=${OUT[EventBridgeFailuresQueueArn]}" \
  "SnsTopicArn=${OUT[SnsTopicArn]}" \
  "PrivateSubnetIds=${OUT[PrivateSubnetIds]}" \
  "RedshiftSecurityGroupId=${OUT[RedshiftSecurityGroupId]}" \
  "LambdaConfigLoaderRoleArn=${OUT[LambdaConfigLoaderRoleArn]}" \
  "LambdaMapProcessorRoleArn=${OUT[LambdaMapProcessorRoleArn]}" \
  "GlueRoleArn=${OUT[GlueRoleArn]}" \
  "StepFunctionsRoleArn=${OUT[StepFunctionsRoleArn]}" \
  "EventBridgeRoleArn=${OUT[EventBridgeRoleArn]}" \
  "ArtifactBucket=${OUT[ScriptsBucketName]}" \
  "ConfigLoaderS3Key=$CL_KEY" \
  "MapProcessorS3Key=$MP_KEY" \
  "ConfigLoaderTimeoutMs=$CL_TIMEOUT_MS" \
  "MapProcessorTimeoutMs=$MP_TIMEOUT_MS"
)

echo ""
echo "===== Pass B: App ====="
echo "Packaging app template..."
APP_PACKAGED="$CFN_DIR/parent-app.packaged.yaml"
aws cloudformation package \
  --template-file "$CFN_DIR/parent-app.yaml" \
  --s3-bucket "$PACKAGE_BUCKET" \
  --s3-prefix "cfn-templates/app" \
  --output-template-file "$APP_PACKAGED" \
  --region "$REGION" >/dev/null

aws cloudformation deploy \
  --template-file "$APP_PACKAGED" \
  --stack-name "$APP_STACK" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --no-fail-on-empty-changeset \
  --parameter-overrides "${APP_OVERRIDES[@]}"

echo ""
echo "Deploy complete."
aws cloudformation describe-stacks --stack-name "$APP_STACK" --region "$REGION" --query "Stacks[0].Outputs" --output table
