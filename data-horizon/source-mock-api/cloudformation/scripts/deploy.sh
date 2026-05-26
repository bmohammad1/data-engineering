#!/usr/bin/env bash
# One-script end-to-end CloudFormation deploy for the Mock Source API.
#
# Usage:
#   ./scripts/deploy.sh <dev|staging|prod> [region]
#
# Steps, in order:
#   1. Prereq check (aws, python3, credentials)
#   2. Build Lambda zip via ../../build.sh if missing or stale
#   3. Ensure artifact S3 bucket exists (mock-source-api-artifacts-<account>-<region>)
#   4. Upload Lambda zip with content-hashed key (so Lambda updates only on real change)
#   5. aws cloudformation package + deploy parent.yaml
#   6. Print stack outputs + Cognito client secret + a test-token-request command

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
API_ROOT="$(cd "$CFN_DIR/.." && pwd)"

PARAMS_FILE="$CFN_DIR/params/${ENVIRONMENT}.json"
[[ -f "$PARAMS_FILE" ]] || { echo "Params file not found: $PARAMS_FILE" >&2; exit 1; }

echo "==> Environment: $ENVIRONMENT   Region: $REGION"
echo "==> API root:    $API_ROOT"
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
    echo "  OK: $name"
  fi
}
check_tool aws     "https://aws.amazon.com/cli/"

# Resolve Python interpreter. On Windows, `python` may resolve to the Microsoft
# Store stub which prints an install prompt instead of running — verify by
# actually invoking --version.
PYTHON=""
for candidate in python3 python py; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" --version >/dev/null 2>&1; then
    PYTHON="$candidate"
    break
  fi
done
if [[ -z "$PYTHON" ]]; then
  echo "  MISSING: working python (python3/python/py) — install from https://www.python.org/downloads/" >&2
  echo "  (If 'python' opens the Microsoft Store on Windows, disable the App Execution Alias under Settings > Apps > Advanced app settings.)" >&2
  missing=1
else
  echo "  OK: $PYTHON ($($PYTHON --version 2>&1))"
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

STACK_NAME="mock-source-api-${ENVIRONMENT}"
ARTIFACT_BUCKET="mock-source-api-artifacts-${ACCOUNT_ID}-${REGION}"
LAMBDA_ZIP="$API_ROOT/build/lambda.zip"

# =============================================================================
# 2. Build Lambda zip if missing or stale
# =============================================================================
echo "==> Building Lambda zip..."
need_build=0
if [[ ! -f "$LAMBDA_ZIP" ]]; then
  need_build=1
else
  # Rebuild if any source file is newer than the zip.
  while IFS= read -r src; do
    if [[ "$src" -nt "$LAMBDA_ZIP" ]]; then
      need_build=1
      break
    fi
  done < <(find "$API_ROOT/app" "$API_ROOT/lambda_handler.py" "$API_ROOT/requirements-lock.txt" 2>/dev/null)
fi

if [[ $need_build -eq 1 ]]; then
  ( cd "$API_ROOT" && bash build.sh )
else
  echo "  SKIP: build/lambda.zip is up to date"
fi
echo ""

# =============================================================================
# 3. Ensure artifact bucket exists
# =============================================================================
echo "==> Ensuring artifact bucket..."
if ! aws s3api head-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" 2>/dev/null; then
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
  fi
  aws s3api put-bucket-encryption --bucket "$ARTIFACT_BUCKET" \
    --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
  aws s3api put-public-access-block --bucket "$ARTIFACT_BUCKET" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null
  echo "  CREATED: $ARTIFACT_BUCKET"
else
  echo "  OK: $ARTIFACT_BUCKET already exists"
fi
echo ""

# =============================================================================
# 4. Upload Lambda zip with content-hashed key
# =============================================================================
echo "==> Uploading Lambda zip..."
HASH=$(sha256sum "$LAMBDA_ZIP" | cut -c1-16)
LAMBDA_KEY="lambda/mock-source-api-${HASH}.zip"
aws s3 cp "$LAMBDA_ZIP" "s3://$ARTIFACT_BUCKET/$LAMBDA_KEY" --region "$REGION" >/dev/null
echo "  s3://$ARTIFACT_BUCKET/$LAMBDA_KEY"
echo ""

# =============================================================================
# 5. Package + deploy via aws cloudformation
# =============================================================================
echo "==> Packaging nested templates..."
PACKAGED_TEMPLATE="$CFN_DIR/parent.packaged.yaml"
aws cloudformation package \
  --template-file "$CFN_DIR/parent.yaml" \
  --s3-bucket "$ARTIFACT_BUCKET" \
  --s3-prefix "cfn-templates" \
  --output-template-file "$PACKAGED_TEMPLATE" \
  --region "$REGION" >/dev/null
echo "  OK: $PACKAGED_TEMPLATE"
echo ""

echo "==> Deploying stack $STACK_NAME..."
OVERRIDES=$("$PYTHON" - "$PARAMS_FILE" "$ARTIFACT_BUCKET" "$LAMBDA_KEY" <<'PY'
import json, sys
params_file, bucket, key = sys.argv[1:]
with open(params_file) as f: p = json.load(f)
pairs = [f"{k}={v}" for k, v in p.items()]
pairs.append(f"ArtifactBucket={bucket}")
pairs.append(f"LambdaS3Key={key}")
print(" ".join(pairs))
PY
)

aws cloudformation deploy \
  --template-file "$PACKAGED_TEMPLATE" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --no-fail-on-empty-changeset \
  --parameter-overrides $OVERRIDES
echo ""

# =============================================================================
# 6. Print outputs + Cognito client secret + test command
# =============================================================================
echo "==> Stack outputs:"
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs" --output table
echo ""

POOL_ID=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoUserPoolId'].OutputValue" --output text)
CLIENT_ID=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoClientId'].OutputValue" --output text)
TOKEN_URL=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoTokenUrl'].OutputValue" --output text)
API_URL=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)

if [[ -n "$POOL_ID" && "$POOL_ID" != "None" ]]; then
  CLIENT_SECRET=$(aws cognito-idp describe-user-pool-client \
    --user-pool-id "$POOL_ID" \
    --client-id "$CLIENT_ID" \
    --region "$REGION" \
    --query UserPoolClient.ClientSecret \
    --output text 2>/dev/null || echo "")

  # -------------------------------------------------------------------------
  # Fetch a live access token (does not call the API itself).
  # -------------------------------------------------------------------------
  echo "==> Fetching access token from Cognito..."
  TOKEN_JSON=$(curl -s -X POST "$TOKEN_URL" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -u "$CLIENT_ID:$CLIENT_SECRET" \
    -d 'grant_type=client_credentials&scope=mock-source-api/read')

  TOKEN=$("$PYTHON" -c "import sys, json; print(json.loads(sys.argv[1]).get('access_token',''))" "$TOKEN_JSON")

  if [[ -z "$TOKEN" ]]; then
    echo "  FAILED to obtain token. Cognito response:" >&2
    echo "  $TOKEN_JSON" >&2
  else
    echo "  OK: token acquired (length ${#TOKEN})"
    echo ""

    # ----- Reusable instructions for the user --------------------------------
    echo "============================================================"
    echo "Test instructions"
    echo "============================================================"
    echo ""
    echo "Access token (valid for 24 hour):"
    echo ""
    echo "  $TOKEN"
    echo ""
    echo "Call the API (replace <TOKEN> with the value above):"
    echo ""
    echo "  curl -s '$API_URL/tags'             -H 'Authorization: Bearer <TOKEN>'"
    echo "  curl -s '$API_URL/tag/TAG-00001'    -H 'Authorization: Bearer <TOKEN>'"
    echo ""
    echo "When the token expires, fetch a new one:"
    echo ""
    echo "  TOKEN=\$(curl -s -X POST '$TOKEN_URL' \\"
    echo "    -H 'Content-Type: application/x-www-form-urlencoded' \\"
    echo "    -u '$CLIENT_ID:$CLIENT_SECRET' \\"
    echo "    -d 'grant_type=client_credentials&scope=mock-source-api/read' \\"
    echo "    | $PYTHON -c \"import sys, json; print(json.load(sys.stdin)['access_token'])\")"
    echo ""
    echo "Cognito details:"
    echo "  Token URL:     $TOKEN_URL"
    echo "  Client ID:     $CLIENT_ID"
    echo "  Client Secret: $CLIENT_SECRET"
    echo "  Scope:         mock-source-api/read"
    echo "  Grant type:    client_credentials"
    echo "============================================================"
    echo ""
  fi
fi

echo "==> Deploy complete."
