variable "name_prefix" {
  description = "Naming prefix for Lambda resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
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

variable "config_loader_memory" {
  description = "Memory in MB for the config loader Lambda"
  type        = number
  default     = 256
}

variable "config_loader_timeout" {
  description = "Timeout in seconds for the config loader Lambda"
  type        = number
  default     = 60
}

variable "map_processor_memory" {
  description = "Memory in MB for the map state processor Lambda"
  type        = number
  default     = 256
}

variable "map_processor_timeout" {
  description = "Timeout in seconds for the map state processor Lambda"
  type        = number
  default     = 60
}

variable "subnet_ids" {
  description = "Private subnet IDs for Lambda VPC placement"
  type        = list(string)
}

variable "lambda_security_group_id" {
  description = "Security group ID for Lambda functions in VPC"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
