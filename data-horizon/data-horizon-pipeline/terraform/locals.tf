locals {
  account_id   = data.aws_caller_identity.current.account_id
  region       = data.aws_region.current.name
  project_name = "data-horizon-${var.environment}"

  # Common naming prefix used across all resources.
  name_prefix = local.project_name

  common_tags = {
    Project     = "data-horizon-pipeline"
    ManagedBy   = "terraform"
    Environment = var.environment
  }
}
