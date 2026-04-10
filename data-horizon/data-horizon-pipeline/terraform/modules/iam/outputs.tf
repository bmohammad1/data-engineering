output "lambda_orchestrator_role_arn" {
  description = "IAM role ARN for the orchestrator Lambda"
  value       = aws_iam_role.lambda_orchestrator.arn
}

output "lambda_map_processor_role_arn" {
  description = "IAM role ARN for the map state processor Lambda"
  value       = aws_iam_role.lambda_map_processor.arn
}

output "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  value       = aws_iam_role.glue.arn
}

output "step_functions_role_arn" {
  description = "IAM role ARN for Step Functions"
  value       = aws_iam_role.step_functions.arn
}

output "eventbridge_role_arn" {
  description = "IAM role ARN for EventBridge"
  value       = aws_iam_role.eventbridge.arn
}

output "redshift_role_arn" {
  description = "IAM role ARN for Redshift S3 COPY"
  value       = aws_iam_role.redshift.arn
}
