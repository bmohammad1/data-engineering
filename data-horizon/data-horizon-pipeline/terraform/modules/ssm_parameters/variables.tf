variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "source_api_token" {
  description = "Cognito JWT token for the source API (SecureString)"
  type        = string
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift cluster master password (SecureString)"
  type        = string
  sensitive   = true
}

variable "pipeline_state_table" {
  description = "DynamoDB PipelineAudit table name"
  type        = string
}

variable "source_api_base_url" {
  description = "Base URL of the source mock API"
  type        = string
}

variable "config_bucket_name" {
  description = "S3 bucket name holding the tags CSV config"
  type        = string
}

variable "orchestration_bucket_name" {
  description = "S3 bucket name for Map State orchestration JSON"
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
  description = "S3 bucket name for quarantined invalid records"
  type        = string
}

variable "glue_database" {
  description = "Glue Data Catalog database name"
  type        = string
}

variable "map_state_concurrency" {
  description = "Max concurrency for the Step Functions Map State"
  type        = number
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
