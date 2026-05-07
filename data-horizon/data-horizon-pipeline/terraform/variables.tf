variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "config_loader_memory" {
  description = "Memory in MB for the config loader Lambda"
  type        = number
  default     = 256
}

variable "config_loader_timeout" {
  description = "Timeout in seconds for the config loader Lambda"
  type        = number
  default     = 60
}

variable "map_processor_memory" {
  description = "Memory in MB for the map state processor Lambda"
  type        = number
  default     = 256
}

variable "map_processor_timeout" {
  description = "Timeout in seconds for the map state processor Lambda"
  type        = number
  default     = 60
}

variable "transform_workers" {
  description = "Number of workers for the transform Glue job"
  type        = number
  default     = 2
}

variable "transform_worker_type" {
  description = "Worker type for the transform Glue job (G.1X, G.2X, G.4X, G.8X)"
  type        = string
  default     = "G.1X"
}

variable "transform_timeout" {
  description = "Timeout in minutes for the transform Glue job"
  type        = number
  default     = 60
}

variable "validation_workers" {
  description = "Number of workers for the validation Glue job"
  type        = number
  default     = 2
}

variable "validation_worker_type" {
  description = "Worker type for the validation Glue job (G.1X, G.2X, G.4X, G.8X)"
  type        = string
  default     = "G.1X"
}

variable "validation_timeout" {
  description = "Timeout in minutes for the validation Glue job"
  type        = number
  default     = 60
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes (1 for dev, more for prod)"
  type        = number
  default     = 1
}

variable "redshift_master_username" {
  description = "Redshift master username"
  type        = string
  default     = "admin"
}

variable "redshift_master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "redshift_database_name" {
  description = "Redshift database name"
  type        = string
  default     = "datahorizon"
}

variable "map_state_concurrency" {
  description = "Max concurrency for Step Functions Map State"
  type        = number
  default     = 10
}

variable "source_api_base_url" {
  description = "Base URL of the source mock API"
  type        = string
}

variable "source_api_token" {
  description = "Cognito token for source API"
  type        = string
}



variable "alert_email" {
  description = "Email address for SNS failure alert notifications"
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log group retention in days"
  type        = number
  default     = 30
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet (hosts the NAT Gateway)"
  type        = string
  default     = "10.0.0.0/24"
}

variable "private_subnet_cidr" {
  description = "CIDR block for the first private subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_subnet_cidr_2" {
  description = "CIDR block for the second private subnet (different AZ, required by Redshift subnet group)"
  type        = string
  default     = "10.0.2.0/24"
}
variable "sechedule_expression_for_eventbridge" {
  description ="Schedule hours in event bridge"
  type        = string
  default     = "rate(6 hours)"
}

variable "rds_instance_class" {
  description = "RDS instance class for the pipeline_audit PostgreSQL database"
  type        = string
  default     = "db.t3.medium"
}

variable "rds_db_password" {
  description = "Master password for the RDS pipeline_audit instance"
  type        = string
  sensitive   = true
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ for RDS (true for prod, false for dev/staging)"
  type        = bool
  default     = false
}

variable "rds_skip_final_snapshot" {
  description = "Skip final snapshot on RDS destroy (true for dev/test, false for prod)"
  type        = bool
  default     = false
}
