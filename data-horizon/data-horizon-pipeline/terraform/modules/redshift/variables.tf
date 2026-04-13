variable "name_prefix" {
  description = "Naming prefix for Redshift resources"
  type        = string
}

variable "node_type" {
  description = "Redshift node type"
  type        = string
}

variable "number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
}

variable "database_name" {
  description = "Redshift database name"
  type        = string
}

variable "master_username" {
  description = "Redshift master username"
  type        = string
}

variable "master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "subnet_id" {
  description = "Private subnet ID for the Redshift cluster"
  type        = string
}

variable "security_group_id" {
  description = "Security group ID for the Redshift cluster"
  type        = string
}

variable "s3_validated_bucket_arn" {
  description = "Parquet S3 bucket ARN (for the COPY IAM policy)"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
