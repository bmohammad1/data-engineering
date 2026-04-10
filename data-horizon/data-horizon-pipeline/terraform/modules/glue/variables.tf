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

variable "secret_name" {
  description = "Secrets Manager secret name for runtime config"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
