output "api_url" {
  description = "API Gateway base URL"
  value       = aws_api_gateway_stage.this.invoke_url
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.this.function_name
}

output "cognito_user_pool_id" {
  description = "Cognito User Pool ID — create your app client under this pool"
  value       = aws_cognito_user_pool.this.id
}

output "cognito_token_url" {
  description = "Cognito token endpoint for client_credentials grant"
  value       = "https://${aws_cognito_user_pool_domain.this.domain}.auth.${var.aws_region}.amazoncognito.com/oauth2/token"
}

output "cognito_custom_scope" {
  description = "Custom scope to assign to your app client"
  value       = "mock-source-api/read"
}

output "cognito_client_id" {
  description = "Cognito app client ID for M2M authentication"
  value       = aws_cognito_user_pool_client.this.id
}

output "cognito_client_secret" {
  description = "Cognito app client secret for M2M authentication"
  value       = aws_cognito_user_pool_client.this.client_secret
  sensitive   = true
}
