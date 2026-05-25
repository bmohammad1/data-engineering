#!/usr/bin/env bash
# Deploy Data Horizon CloudFormation — two-pass (foundation, then app) using SAM CLI.
# See deploy.ps1 for full description and prerequisites.
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

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ACCOUNT_SUFFIX="${ACCOUNT_ID: -8}"
FOUNDATION_STACK="data-horizon-foundation-${ENVIRONMENT}"
APP_STACK="data-horizon-app-${ENVIRONMENT}"

echo "Account: $ACCOUNT_ID  AccountSuffix: $ACCOUNT_SUFFIX"
echo "Region: $REGION  Environment: $ENVIRONMENT"
echo "Foundation stack: $FOUNDATION_STACK"
echo "App stack:        $APP_STACK"
echo ""

echo "Rendering Step Functions template from ASL JSON..."
python3 "$SCRIPT_DIR/render-stepfunctions.py"
echo ""

# Build parameter overrides as "k1=v1 k2=v2 ..." from the JSON file.
build_overrides() {
  python3 - "$PARAMS_FILE" "$ACCOUNT_SUFFIX" "$@" <<'PY'
import json, sys
params_file, account_suffix, *extras = sys.argv[1:]
with open(params_file) as f: params = json.load(f)
pairs = [f"AccountSuffix={account_suffix}"]
for k, v in params.items(): pairs.append(f"{k}={v}")
for e in extras: pairs.append(e)
print(" ".join(pairs))
PY
}

# ==========================================================================
# Pass A — Foundation
# ==========================================================================
echo "===== Pass A: Foundation ====="
FOUNDATION_OVERRIDES=$(build_overrides)
sam deploy \
  --template-file "$CFN_DIR/parent-foundation.yaml" \
  --stack-name "$FOUNDATION_STACK" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --resolve-s3 \
  --no-fail-on-empty-changeset \
  --no-confirm-changeset \
  --parameter-overrides $FOUNDATION_OVERRIDES

# Read foundation outputs.
echo ""
echo "Reading foundation outputs..."
declare -A OUT
while IFS=$'\t' read -r k v; do OUT[$k]="$v"; done < <(
  aws cloudformation describe-stacks --stack-name "$FOUNDATION_STACK" --region "$REGION" \
    --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output text
)

SCRIPTS_BUCKET="${OUT[ScriptsBucketName]}"
CONFIG_BUCKET="${OUT[ConfigBucketName]}"
echo "Scripts bucket: $SCRIPTS_BUCKET"
echo "Config bucket:  $CONFIG_BUCKET"

# Upload Glue scripts.
TRANSFORM="$PIPELINE_DIR/glue_jobs/scripts/transform_job.py"
VALIDATION="$PIPELINE_DIR/glue_jobs/scripts/validation_job.py"
UTILS_ZIP="$PIPELINE_DIR/glue_jobs/utils.zip"
[[ -f "$TRANSFORM"  ]] && aws s3 cp "$TRANSFORM"  "s3://$SCRIPTS_BUCKET/scripts/transform_job.py"  --region "$REGION" >/dev/null && echo "Uploaded transform_job.py"
[[ -f "$VALIDATION" ]] && aws s3 cp "$VALIDATION" "s3://$SCRIPTS_BUCKET/scripts/validation_job.py" --region "$REGION" >/dev/null && echo "Uploaded validation_job.py"
[[ -f "$UTILS_ZIP"  ]] && aws s3 cp "$UTILS_ZIP"  "s3://$SCRIPTS_BUCKET/scripts/utils.zip"         --region "$REGION" >/dev/null && echo "Uploaded utils.zip"

# source_config/ placeholder.
TMP=$(mktemp)
aws s3 cp "$TMP" "s3://$CONFIG_BUCKET/source_config/.placeholder" --region "$REGION" >/dev/null
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

aws s3 cp "$CL_ZIP" "s3://$SCRIPTS_BUCKET/$CL_KEY" --region "$REGION" >/dev/null
aws s3 cp "$MP_ZIP" "s3://$SCRIPTS_BUCKET/$MP_KEY" --region "$REGION" >/dev/null
echo "Uploaded Lambda zips: $CL_KEY, $MP_KEY"

# ==========================================================================
# Pass B — App
# ==========================================================================
CL_TIMEOUT_SEC=$(jq -r '.ConfigLoaderTimeout' "$PARAMS_FILE")
MP_TIMEOUT_SEC=$(jq -r '.MapProcessorTimeout' "$PARAMS_FILE")
CL_TIMEOUT_MS=$(awk -v s="$CL_TIMEOUT_SEC" 'BEGIN{print int(s*1000*0.8)}')
MP_TIMEOUT_MS=$(awk -v s="$MP_TIMEOUT_SEC" 'BEGIN{print int(s*1000*0.8)}')

APP_OVERRIDES=$(build_overrides \
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
sam deploy \
  --template-file "$CFN_DIR/parent-app.yaml" \
  --stack-name "$APP_STACK" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --resolve-s3 \
  --no-fail-on-empty-changeset \
  --no-confirm-changeset \
  --parameter-overrides $APP_OVERRIDES

echo ""
echo "Deploy complete."
aws cloudformation describe-stacks --stack-name "$APP_STACK" --region "$REGION" --query "Stacks[0].Outputs" --output table
