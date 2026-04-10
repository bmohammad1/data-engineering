output "orchestrator_function_name" {
  description = "Orchestrator Lambda function name"
  value       = aws_lambda_function.orchestrator.function_name
}

output "orchestrator_function_arn" {
  description = "Orchestrator Lambda function ARN"
  value       = aws_lambda_function.orchestrator.arn
}

output "map_state_processor_function_name" {
  description = "Map state processor Lambda function name"
  value       = aws_lambda_function.map_state_processor.function_name
}

output "map_state_processor_function_arn" {
  description = "Map state processor Lambda function ARN"
  value       = aws_lambda_function.map_state_processor.arn
}
