variable "name_prefix" {
  description = "Prefix applied to all resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the Aurora cluster is placed"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block — used for the Aurora security group ingress rule"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for the Aurora subnet group (minimum two, different AZs)"
  type        = list(string)
}

variable "instance_class" {
  description = "Aurora instance class (e.g., db.t4g.medium)"
  type        = string
  default     = "db.t4g.medium"
}

variable "db_name" {
  description = "Name of the initial PostgreSQL database"
  type        = string
  default     = "pipeline_audit"
}

variable "db_username" {
  description = "Master username for the Aurora cluster"
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
  description = "Prevent accidental deletion of the Aurora cluster"
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Skip final snapshot on destroy (set true for dev/test)"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
