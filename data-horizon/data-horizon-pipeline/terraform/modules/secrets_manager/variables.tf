variable "name_prefix" {
  description = "Naming prefix for Secrets Manager resources"
  type        = string
}

variable "source_api_token" {
  description = "Cognito token for source API"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
