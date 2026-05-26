#!/usr/bin/env bash
# Bootstrap — see bootstrap.ps1 for full description.
# Usage: bootstrap.sh <env> [region] [--source-api-token X] [--redshift-master-password Y] [--create-artifact-bucket]

set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${2:-us-east-1}"
SOURCE_API_TOKEN=""
REDSHIFT_PASSWORD=""
CREATE_BUCKET=false

shift 2 || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-api-token)         SOURCE_API_TOKEN="$2"; shift 2 ;;
    --redshift-master-password) REDSHIFT_PASSWORD="$2"; shift 2 ;;
    --create-artifact-bucket)   CREATE_BUCKET=true; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$ENVIRONMENT" ]] || [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
  echo "Usage: $0 <dev|staging|prod> [region]" >&2; exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account: $ACCOUNT_ID  Region: $REGION  Environment: $ENVIRONMENT"

set_secure() {
  local name="$1" value="$2"
  if [[ -z "$value" ]]; then echo "Skipping $name (no value provided)"; return; fi
  aws ssm put-parameter --name "$name" --type SecureString --value "$value" --overwrite --region "$REGION" >/dev/null
  echo "Seeded SSM SecureString: $name"
}

set_secure "/data-horizon/$ENVIRONMENT/source-api-token"         "$SOURCE_API_TOKEN"
set_secure "/data-horizon/$ENVIRONMENT/redshift-master-password" "$REDSHIFT_PASSWORD"

if [[ "$CREATE_BUCKET" == true ]]; then
  BUCKET="data-horizon-cfn-artifacts-$ACCOUNT_ID-$REGION"
  if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    if [[ "$REGION" == "us-east-1" ]]; then
      aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
    else
      aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
        --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    fi
    aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled >/dev/null
    aws s3api put-bucket-encryption --bucket "$BUCKET" \
      --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
    aws s3api put-public-access-block --bucket "$BUCKET" \
      --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null
    echo "Created artifact bucket $BUCKET"
  else
    echo "Artifact bucket $BUCKET already exists."
  fi
fi

echo "Bootstrap complete."
