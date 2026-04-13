variable "name_prefix" {
  description = "Naming prefix for IAM resources"
  type        = string
}

variable "secret_arn" {
  description = "Secrets Manager secret ARN for pipeline config"
  type        = string
}

variable "s3_raw_bucket_arn" {
  description = "Raw data S3 bucket ARN"
  type        = string
}

variable "s3_cleaned_bucket_arn" {
  description = "Cleaned data S3 bucket ARN"
  type        = string
}

variable "s3_validated_bucket_arn" {
  description = "Parquet data S3 bucket ARN"
  type        = string
}

variable "s3_bad_bucket_arn" {
  description = "Bad data S3 bucket ARN"
  type        = string
}

variable "s3_scripts_bucket_arn" {
  description = "Glue scripts S3 bucket ARN"
  type        = string
}

variable "s3_orchestration_bucket_arn" {
  description = "Orchestration map file S3 bucket ARN"
  type        = string
}

variable "s3_config_bucket_arn" {
  description = "Config S3 bucket ARN"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "DynamoDB pipeline state table ARN"
  type        = string
}

variable "extraction_failures_queue_arn" {
  description = "Extraction failures SQS queue ARN"
  type        = string
}

variable "eventbridge_failures_queue_arn" {
  description = "EventBridge failures SQS queue ARN"
  type        = string
}

variable "sns_topic_arn" {
  description = "Pipeline failure alerts SNS topic ARN"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
