variable "name_prefix" {
  description = "Naming prefix for DynamoDB resources"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
