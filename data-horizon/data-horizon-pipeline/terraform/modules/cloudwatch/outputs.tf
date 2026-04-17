output "glue_transform_log_group_name" {
  description = "Log group name for Glue transform job"
  value       = aws_cloudwatch_log_group.glue_transform.name
}

output "glue_validation_log_group_name" {
  description = "Log group name for Glue validation job"
  value       = aws_cloudwatch_log_group.glue_validation.name
}
