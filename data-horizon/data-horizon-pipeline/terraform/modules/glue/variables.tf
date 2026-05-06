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

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "transform_workers" {
  description = "Number of workers for the transform Glue job"
  type        = number
  default     = 2
}

variable "transform_worker_type" {
  description = "Worker type for the transform Glue job (G.1X, G.2X, G.4X, G.8X)"
  type        = string
  default     = "G.1X"
}

variable "transform_timeout" {
  description = "Timeout in minutes for the transform Glue job"
  type        = number
  default     = 60
}

variable "validation_workers" {
  description = "Number of workers for the validation Glue job"
  type        = number
  default     = 2
}

variable "validation_worker_type" {
  description = "Worker type for the validation Glue job (G.1X, G.2X, G.4X, G.8X)"
  type        = string
  default     = "G.1X"
}

variable "validation_timeout" {
  description = "Timeout in minutes for the validation Glue job"
  type        = number
  default     = 60
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
