output "eventbridge_failures_queue_arn" {
  description = "EventBridge failures SQS queue ARN"
  value       = aws_sqs_queue.eventbridge_failures.arn
}

output "eventbridge_failures_queue_url" {
  description = "EventBridge failures SQS queue URL"
  value       = aws_sqs_queue.eventbridge_failures.url
}

output "extraction_failures_queue_arn" {
  description = "Extraction failures SQS queue ARN"
  value       = aws_sqs_queue.extraction_failures.arn
}

output "extraction_failures_queue_url" {
  description = "Extraction failures SQS queue URL"
  value       = aws_sqs_queue.extraction_failures.url
}
