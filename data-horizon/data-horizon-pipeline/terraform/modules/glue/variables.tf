variable "name_prefix" {
  description = "Naming prefix for Glue resources"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "scripts_bucket_name" {
  description = "S3 bucket name where Glue scripts are stored"
  type        = string
}

variable "secret_name" {
  description = "Secrets Manager secret name for runtime config"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw API response data"
  type        = string
}

variable "cleaned_bucket_name" {
  description = "S3 bucket name for transformed/cleaned data"
  type        = string
}

variable "validated_bucket_name" {
  description = "S3 bucket name for validated Parquet data"
  type        = string
}

variable "quarantine_bucket_name" {
  description = "S3 bucket name for quarantined invalid records (bad bucket)"
  type        = string
}

variable "pipeline_state_table" {
  description = "DynamoDB table name for pipeline audit state"
  type        = string
}

variable "glue_database" {
  description = "Glue Data Catalog database name"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
