output "transform_job_name" {
  description = "Glue transform job name"
  value       = aws_glue_job.transform.name
}

output "validation_job_name" {
  description = "Glue validation job name"
  value       = aws_glue_job.validation.name
}
