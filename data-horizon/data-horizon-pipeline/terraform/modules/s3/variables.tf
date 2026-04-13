variable "name_prefix" {
  description = "Naming prefix for all S3 buckets"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
