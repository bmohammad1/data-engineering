variable "name_prefix" {
  description = "Naming prefix for SNS resources"
  type        = string
}

variable "alert_email" {
  description = "Email address for failure alert subscriptions"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
