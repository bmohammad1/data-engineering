locals {
  base_path = "/data-horizon/${var.environment}"
}

# =============================================================================
# SecureString parameters — sensitive values managed by Terraform, encrypted
# at rest with the default SSM KMS key.
# =============================================================================

resource "aws_ssm_parameter" "source_api_token" {
  name  = "${local.base_path}/source-api-token"
  type  = "SecureString"
  value = var.source_api_token
  tags  = var.tags
}

resource "aws_ssm_parameter" "redshift_master_password" {
  name  = "${local.base_path}/redshift-master-password"
  type  = "SecureString"
  value = var.redshift_master_password
  tags  = var.tags
}

# =============================================================================
# String parameters — non-sensitive runtime config for Lambda and Glue.
# Lambdas and Glue jobs fetch these at cold start via GetParametersByPath,
# removing the need to redeploy when a value changes.
# =============================================================================

resource "aws_ssm_parameter" "pipeline_state_table" {
  name  = "${local.base_path}/pipeline-state-table"
  type  = "String"
  value = var.pipeline_state_table
  tags  = var.tags
}

resource "aws_ssm_parameter" "source_api_base_url" {
  name  = "${local.base_path}/source-api-base-url"
  type  = "String"
  value = var.source_api_base_url
  tags  = var.tags
}

resource "aws_ssm_parameter" "config_bucket_name" {
  name  = "${local.base_path}/config-bucket-name"
  type  = "String"
  value = var.config_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "orchestration_bucket_name" {
  name  = "${local.base_path}/orchestration-bucket-name"
  type  = "String"
  value = var.orchestration_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "raw_bucket_name" {
  name  = "${local.base_path}/raw-bucket-name"
  type  = "String"
  value = var.raw_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "cleaned_bucket_name" {
  name  = "${local.base_path}/cleaned-bucket-name"
  type  = "String"
  value = var.cleaned_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "validated_bucket_name" {
  name  = "${local.base_path}/validated-bucket-name"
  type  = "String"
  value = var.validated_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "quarantine_bucket_name" {
  name  = "${local.base_path}/quarantine-bucket-name"
  type  = "String"
  value = var.quarantine_bucket_name
  tags  = var.tags
}

resource "aws_ssm_parameter" "glue_database" {
  name  = "${local.base_path}/glue-database"
  type  = "String"
  value = var.glue_database
  tags  = var.tags
}

resource "aws_ssm_parameter" "map_state_concurrency" {
  name  = "${local.base_path}/map-state-concurrency"
  type  = "String"
  value = tostring(var.map_state_concurrency)
  tags  = var.tags
}
