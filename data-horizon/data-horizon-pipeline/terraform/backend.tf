# S3 backend with DynamoDB state locking.
# Configured per environment via backend.hcl files:
#   terraform init -backend-config=environments/<env>/backend.hcl

terraform {
  backend "s3" {}
}
