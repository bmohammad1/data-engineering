output "raw_bucket_name" {
  description = "Raw data bucket name"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "Raw data bucket ARN"
  value       = aws_s3_bucket.raw.arn
}

output "cleaned_bucket_name" {
  description = "Cleaned data bucket name"
  value       = aws_s3_bucket.cleaned.bucket
}

output "cleaned_bucket_arn" {
  description = "Cleaned data bucket ARN"
  value       = aws_s3_bucket.cleaned.arn
}

output "validated_bucket_name" {
  description = "Parquet data bucket name"
  value       = aws_s3_bucket.validated.bucket
}

output "validated_bucket_arn" {
  description = "Parquet data bucket ARN"
  value       = aws_s3_bucket.validated.arn
}

output "bad_bucket_name" {
  description = "Bad data bucket name"
  value       = aws_s3_bucket.bad.bucket
}

output "bad_bucket_arn" {
  description = "Bad data bucket ARN"
  value       = aws_s3_bucket.bad.arn
}

output "scripts_bucket_name" {
  description = "Glue scripts bucket name"
  value       = aws_s3_bucket.scripts.bucket
}

output "scripts_bucket_arn" {
  description = "Glue scripts bucket ARN"
  value       = aws_s3_bucket.scripts.arn
}

output "orchestration_bucket_name" {
  description = "Orchestration map file bucket name"
  value       = aws_s3_bucket.orchestration.bucket
}

output "orchestration_bucket_arn" {
  description = "Orchestration map file bucket ARN"
  value       = aws_s3_bucket.orchestration.arn
}

output "config_bucket_name" {
  description = "Config bucket name"
  value       = aws_s3_bucket.config.bucket
}

output "config_bucket_arn" {
  description = "Config bucket ARN"
  value       = aws_s3_bucket.config.arn
}
