variable "name_prefix" {
  description = "Naming prefix for Glue Catalog resources"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "cleaned_bucket_name" {
  description = "S3 bucket name for cleaned data"
  type        = string
}

variable "validated_bucket_name" {
  description = "S3 bucket name for validated data"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
