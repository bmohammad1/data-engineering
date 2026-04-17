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

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
