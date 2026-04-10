output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.this.id
}

output "private_subnet_id" {
  description = "Private subnet ID for Redshift"
  value       = aws_subnet.private.id
}

output "redshift_security_group_id" {
  description = "Security group ID for Redshift cluster"
  value       = aws_security_group.redshift.id
}
