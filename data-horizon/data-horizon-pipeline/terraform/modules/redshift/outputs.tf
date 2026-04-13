output "cluster_identifier" {
  description = "Redshift cluster identifier"
  value       = aws_redshift_cluster.this.cluster_identifier
}

output "cluster_endpoint" {
  description = "Redshift cluster endpoint (host:port)"
  value       = aws_redshift_cluster.this.endpoint
}

output "cluster_host" {
  description = "Redshift cluster hostname (without port)"
  value       = aws_redshift_cluster.this.dns_name
}

output "database_name" {
  description = "Redshift database name"
  value       = aws_redshift_cluster.this.database_name
}
