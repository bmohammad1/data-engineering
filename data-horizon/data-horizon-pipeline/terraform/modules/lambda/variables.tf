variable "name_prefix" {
  description = "Naming prefix for Lambda resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "secret_name" {
  description = "Secrets Manager secret name for pipeline config"
  type        = string
}

variable "config_loader_role_arn" {
  description = "IAM role ARN for the config loader Lambda"
  type        = string
}

variable "map_processor_role_arn" {
  description = "IAM role ARN for the map state processor Lambda"
  type        = string
}

variable "memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 256
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}

variable "pipeline_state_table" {
  description = "DynamoDB PipelineAudit table name"
  type        = string
}

variable "raw_bucket_name" {
  description = "Raw data S3 bucket name"
  type        = string
}

variable "config_bucket_name" {
  description = "Config S3 bucket name (holds tags CSV)"
  type        = string
}

variable "orchestration_bucket_name" {
  description = "Orchestration S3 bucket name (holds map state JSON)"
  type        = string
}

variable "source_api_base_url" {
  description = "Base URL of the source mock API"
  type        = string
}

variable "map_state_concurrency" {
  description = "Max concurrency for the Step Functions Map State"
  type        = number
  default     = 5
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
