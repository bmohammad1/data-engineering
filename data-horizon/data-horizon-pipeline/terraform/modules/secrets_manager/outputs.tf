output "secret_arn" {
  description = "Pipeline config secret ARN"
  value       = aws_secretsmanager_secret.pipeline_config.arn
}

output "secret_name" {
  description = "Pipeline config secret name"
  value       = aws_secretsmanager_secret.pipeline_config.name
}
