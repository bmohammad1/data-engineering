variable "name_prefix" {
  description = "Naming prefix for Step Functions resources"
  type        = string
}

variable "step_functions_role_arn" {
  description = "IAM role ARN for Step Functions"
  type        = string
}

variable "orchestrator_lambda_arn" {
  description = "Orchestrator Lambda function ARN"
  type        = string
}

variable "map_state_processor_lambda_arn" {
  description = "Map state processor Lambda function ARN"
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

variable "redshift_cluster_id" {
  description = "Redshift cluster identifier"
  type        = string
}

variable "redshift_database" {
  description = "Redshift database name"
  type        = string
}

variable "redshift_master_username" {
  description = "Redshift master username"
  type        = string
}

variable "validated_bucket_name" {
  description = "S3 validated bucket name (source for COPY)"
  type        = string
}

variable "redshift_iam_role_arn" {
  description = "Redshift IAM role ARN (for COPY command)"
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

variable "orchestration_bucket_name" {
  description = "S3 orchestration bucket name (holds map state input JSON files)"
  type        = string
}

variable "statemachine_dir" {
  description = "Directory containing ASL JSON files"
  type        = string
}

variable "retention_days" {
  description = "CloudWatch log group retention in days"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
