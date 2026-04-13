variable "name_prefix" {
  description = "Naming prefix for EventBridge resources"
  type        = string
}

variable "parent_state_machine_arn" {
  description = "Parent Step Function state machine ARN to trigger"
  type        = string
}

variable "eventbridge_role_arn" {
  description = "IAM role ARN for EventBridge to invoke Step Functions"
  type        = string
}

variable "eventbridge_failures_queue_arn" {
  description = "SQS queue ARN for EventBridge delivery failures"
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge schedule expression (cron or rate)"
  type        = string
  default     = "rate(6 hours)"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
