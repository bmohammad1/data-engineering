output "instance_endpoint" {
  description = "RDS PostgreSQL instance endpoint"
  value       = aws_db_instance.pipeline_audit.endpoint
}

output "instance_address" {
  description = "RDS PostgreSQL instance address (hostname only)"
  value       = aws_db_instance.pipeline_audit.address
}

output "instance_arn" {
  description = "RDS PostgreSQL instance ARN"
  value       = aws_db_instance.pipeline_audit.arn
}

output "security_group_id" {
  description = "Security group ID for the RDS instance"
  value       = aws_security_group.rds.id
}

output "pg_connection_string_ssm_name" {
  description = "SSM parameter name storing the Postgres connection string"
  value       = aws_ssm_parameter.pg_connection_string.name
}

output "rds_instance_arn_ssm_name" {
  description = "SSM parameter name storing the RDS instance ARN"
  value       = aws_ssm_parameter.rds_instance_arn.name
}
