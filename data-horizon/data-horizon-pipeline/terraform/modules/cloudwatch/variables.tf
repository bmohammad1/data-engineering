variable "name_prefix" {
  description = "Naming prefix for CloudWatch resources"
  type        = string
}

variable "retention_days" {
  description = "Log retention in days"
  type        = number
  default     = 30
}

variable "config_loader_function_name" {
  description = "Config Loader Lambda function name"
  type        = string
}

variable "map_state_processor_function_name" {
  description = "Map state processor Lambda function name"
  type        = string
}

variable "transform_glue_job_name" {
  description = "Glue transform job name"
  type        = string
}

variable "validation_glue_job_name" {
  description = "Glue validation job name"
  type        = string
}

variable "parent_state_machine_arn" {
  description = "Parent Step Function state machine ARN"
  type        = string
}


variable "extraction_failures_queue_name" {
  description = "Extraction failures SQS queue name"
  type        = string
}

variable "sns_topic_arn" {
  description = "Pipeline failure alerts SNS topic ARN (alarm target)"
  type        = string
}

variable "dynamodb_table_name" {
  description = "Pipeline state DynamoDB table name — set to null when using RDS PostgreSQL"
  type        = string
  default     = null
}

variable "config_loader_timeout_ms" {
  description = "Config Loader Lambda timeout in milliseconds (used to compute 80% duration alarm threshold)"
  type        = number
}

variable "map_processor_timeout_ms" {
  description = "Map state processor Lambda timeout in milliseconds (used to compute 80% duration alarm threshold)"
  type        = number
}

variable "glue_heap_threshold_bytes" {
  description = "JVM heap alarm threshold in bytes — set to 80% of G.1X driver heap (10 GB = 10737418240, 80% = 8589934592)"
  type        = number
  default     = 8589934592
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
