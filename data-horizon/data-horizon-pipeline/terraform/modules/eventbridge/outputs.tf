output "rule_arn" {
  description = "EventBridge schedule rule ARN"
  value       = aws_cloudwatch_event_rule.pipeline_schedule.arn
}

output "rule_name" {
  description = "EventBridge schedule rule name"
  value       = aws_cloudwatch_event_rule.pipeline_schedule.name
}
