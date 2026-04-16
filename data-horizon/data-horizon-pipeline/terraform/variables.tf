variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "lambda_memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 256
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}

variable "glue_dpu_count" {
  description = "Number of DPUs for Glue jobs"
  type        = number
  default     = 2
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