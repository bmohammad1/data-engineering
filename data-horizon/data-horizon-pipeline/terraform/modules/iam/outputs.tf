output "lambda_config_loader_role_arn" {
  description = "IAM role ARN for the config loader Lambda"
  value       = aws_iam_role.lambda_config_loader.arn
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

