variable "name_prefix" {
  description = "Naming prefix for SQS resources"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
