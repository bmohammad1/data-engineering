output "parameter_path_prefix" {
  description = "SSM path prefix for all pipeline parameters (use with GetParametersByPath)"
  value       = "/data-horizon/${var.environment}"
}
