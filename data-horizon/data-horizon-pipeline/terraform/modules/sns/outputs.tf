output "topic_arn" {
  description = "Pipeline failure alerts SNS topic ARN"
  value       = aws_sns_topic.pipeline_failure_alerts.arn
}
