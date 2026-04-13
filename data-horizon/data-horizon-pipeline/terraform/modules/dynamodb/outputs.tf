output "table_name" {
  description = "DynamoDB pipeline state table name"
  value       = aws_dynamodb_table.pipeline_state.name
}

output "table_arn" {
  description = "DynamoDB pipeline state table ARN"
  value       = aws_dynamodb_table.pipeline_state.arn
}
