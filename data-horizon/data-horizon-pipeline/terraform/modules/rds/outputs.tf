output "endpoint" {
  description = "RDS instance endpoint (host:port)"
  value       = aws_db_instance.pipeline_audit.endpoint
}

output "security_group_id" {
  description = "Security group ID for the RDS instance"
  value       = aws_security_group.rds.id
}

output "pg_connection_string_ssm_name" {
  description = "SSM parameter name storing the Postgres connection string"
  value       = aws_ssm_parameter.pg_connection_string.name
}
