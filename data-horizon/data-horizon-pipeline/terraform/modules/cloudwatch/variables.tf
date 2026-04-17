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

variable "child1_state_machine_arn" {
  description = "Child1 (config) state machine ARN"
  type        = string
}

variable "child2_state_machine_arn" {
  description = "Child2 (extraction) state machine ARN"
  type        = string
}

variable "child3_state_machine_arn" {
  description = "Child3 (transformation) state machine ARN"
  type        = string
}

variable "child4_state_machine_arn" {
  description = "Child4 (Redshift load) state machine ARN"
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

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
