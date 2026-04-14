output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.this.id
}

output "private_subnet_id" {
  description = "First private subnet ID"
  value       = aws_subnet.private.id
}

output "subnet_ids" {
  description = "Both private subnet IDs (for Redshift subnet group)"
  value       = [aws_subnet.private.id, aws_subnet.private_2.id]
}

output "redshift_security_group_id" {
  description = "Security group ID for Redshift cluster"
  value       = aws_security_group.redshift.id
}
