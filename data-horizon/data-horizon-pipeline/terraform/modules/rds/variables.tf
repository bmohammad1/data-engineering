variable "name_prefix" {
  description = "Prefix applied to all resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the RDS instance is placed"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block — used for the RDS security group ingress rule"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for the RDS subnet group (minimum two, different AZs)"
  type        = list(string)
}

variable "instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "allocated_storage" {
  description = "Initial allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Name of the initial PostgreSQL database"
  type        = string
  default     = "pipeline_audit"
}

variable "db_username" {
  description = "Master username for the RDS instance"
  type        = string
  default     = "pipeline_admin"
}

variable "db_password" {
  description = "Master password for the RDS instance"
  type        = string
  sensitive   = true
}

variable "multi_az" {
  description = "Enable Multi-AZ deployment for high availability"
  type        = bool
  default     = false
}

variable "deletion_protection" {
  description = "Prevent accidental deletion of the RDS instance"
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Skip final snapshot on destroy (set true for dev/test)"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
