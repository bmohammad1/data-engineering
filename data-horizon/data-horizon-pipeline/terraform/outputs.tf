# --- S3 Buckets ---

output "s3_raw_bucket_name" {
  description = "S3 bucket for raw API response data"
  value       = module.s3.raw_bucket_name
}

output "s3_cleaned_bucket_name" {
  description = "S3 bucket for cleaned/transformed data"
  value       = module.s3.cleaned_bucket_name
}

output "s3_parquet_bucket_name" {
  description = "S3 bucket for parquet output"
  value       = module.s3.parquet_bucket_name
}

output "s3_orchestration_bucket_name" {
  description = "S3 bucket for orchestration map files"
  value       = module.s3.orchestration_bucket_name
}

# --- DynamoDB ---

output "dynamodb_table_name" {
  description = "DynamoDB pipeline state table name"
  value       = module.dynamodb.table_name
}

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

# --- Secrets Manager ---

output "secret_name" {
  description = "Secrets Manager secret name for pipeline config"
  value       = module.secrets_manager.secret_name
}

# --- Lambda ---

output "orchestrator_function_name" {
  description = "Orchestrator Lambda function name"
  value       = module.lambda.orchestrator_function_name
}

output "map_state_processor_function_name" {
  description = "Map state processor Lambda function name"
  value       = module.lambda.map_state_processor_function_name
}
