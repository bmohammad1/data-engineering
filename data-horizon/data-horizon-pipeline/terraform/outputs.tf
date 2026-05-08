# --- S3 Buckets ---

output "s3_raw_bucket_name" {
  description = "S3 bucket for raw API response data"
  value       = module.s3.raw_bucket_name
}

output "s3_cleaned_bucket_name" {
  description = "S3 bucket for cleaned/transformed data"
  value       = module.s3.cleaned_bucket_name
}

output "s3_validated_bucket_name" {
  description = "S3 bucket for validated output"
  value       = module.s3.validated_bucket_name
}

output "s3_orchestration_bucket_name" {
  description = "S3 bucket for orchestration map files"
  value       = module.s3.orchestration_bucket_name
}

output "s3_scripts_bucket_name" {
  description = "S3 bucket for Glue scripts and utils zip"
  value       = module.s3.scripts_bucket_name
}

# --- RDS PostgreSQL ---

output "rds_instance_endpoint" {
  description = "RDS PostgreSQL instance endpoint for the pipeline_audit database"
  value       = module.rds.instance_endpoint
}

output "rds_instance_address" {
  description = "RDS PostgreSQL instance hostname"
  value       = module.rds.instance_address
}

output "rds_instance_arn" {
  description = "RDS PostgreSQL instance ARN"
  value       = module.rds.instance_arn
}

output "rds_pg_connection_string_ssm_name" {
  description = "SSM parameter name storing the Postgres connection string"
  value       = module.rds.pg_connection_string_ssm_name
}

output "rds_instance_arn_ssm_name" {
  description = "SSM parameter name storing the RDS instance ARN"
  value       = module.rds.rds_instance_arn_ssm_name
}

# --- DynamoDB (commented out — replaced by RDS PostgreSQL) ---
# output "dynamodb_table_name" {
#   description = "DynamoDB pipeline state table name"
#   value       = module.dynamodb.table_name
# }

# --- Step Functions ---

output "parent_state_machine_arn" {
  description = "Parent Step Function state machine ARN"
  value       = module.step_function.parent_state_machine_arn
}

# --- Redshift ---

output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = module.redshift.cluster_endpoint
}

output "redshift_database_name" {
  description = "Redshift database name"
  value       = module.redshift.database_name
}

# --- SSM Parameter Store ---

output "ssm_parameter_path_prefix" {
  description = "SSM Parameter Store path prefix for all pipeline config"
  value       = module.ssm_parameters.parameter_path_prefix
}

# --- Lambda ---

output "config_loader_function_name" {
  description = "Config Loader Lambda function name"
  value       = module.lambda.config_loader_function_name
}

output "map_state_processor_function_name" {
  description = "Map state processor Lambda function name"
  value       = module.lambda.map_state_processor_function_name
}
