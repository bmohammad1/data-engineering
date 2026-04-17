output "config_loader_function_name" {
  description = "Config Loader Lambda function name"
  value       = aws_lambda_function.config_loader.function_name
}

output "config_loader_function_arn" {
  description = "Config Loader Lambda function ARN"
  value       = aws_lambda_function.config_loader.arn
}

output "map_state_processor_function_name" {
  description = "Map state processor Lambda function name"
  value       = aws_lambda_function.map_state_processor.function_name
}

output "map_state_processor_function_arn" {
  description = "Map state processor Lambda function ARN"
  value       = aws_lambda_function.map_state_processor.arn
}
