variable "name_prefix" {
  description = "Naming prefix for Secrets Manager resources"
  type        = string
}

variable "raw_bucket_name" {
  description = "Raw data S3 bucket name"
  type        = string
}

variable "cleaned_bucket_name" {
  description = "Cleaned data S3 bucket name"
  type        = string
}

variable "validated_bucket_name" {
  description = "Parquet data S3 bucket name"
  type        = string
}

variable "bad_bucket_name" {
  description = "Bad data S3 bucket name"
  type        = string
}

variable "scripts_bucket_name" {
  description = "Glue scripts S3 bucket name"
  type        = string
}

variable "orchestration_bucket_name" {
  description = "Orchestration map file S3 bucket name"
  type        = string
}

variable "config_bucket_name" {
  description = "Config S3 bucket name"
  type        = string
}

variable "dynamodb_table_name" {
  description = "DynamoDB pipeline state table name"
  type        = string
}

variable "source_api_base_url" {
  description = "Base URL of the source mock API"
  type        = string
}

variable "source_api_token" {
  description = "Cognito token for source API"
  type        = string
}


variable "redshift_host" {
  description = "Redshift cluster endpoint hostname"
  type        = string
}

variable "redshift_database" {
  description = "Redshift database name"
  type        = string
}

variable "eventbridge_failures_queue_url" {
  description = "EventBridge failures SQS queue URL"
  type        = string
}

variable "extraction_failures_queue_url" {
  description = "Extraction failures SQS queue URL"
  type        = string
}

variable "sns_topic_arn" {
  description = "Pipeline failure alerts SNS topic ARN"
  type        = string
}

variable "map_state_concurrency" {
  description = "Max concurrency for Step Functions Map State"
  type        = number
}

variable "glue_dpu_count" {
  description = "Number of DPUs for Glue jobs"
  type        = number
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
