output "parent_state_machine_arn" {
  description = "Modular Orchestrator state machine ARN"
  value       = aws_sfn_state_machine.modular_orchestrator.arn
}

output "child1_state_machine_arn" {
  description = "Config Loader state machine ARN"
  value       = aws_sfn_state_machine.config_loader.arn
}

output "child2_state_machine_arn" {
  description = "Data Extractor state machine ARN"
  value       = aws_sfn_state_machine.data_extractor.arn
}

output "child3_state_machine_arn" {
  description = "Transformation state machine ARN"
  value       = aws_sfn_state_machine.transformation.arn
}

output "child4_state_machine_arn" {
  description = "Redshift Load state machine ARN"
  value       = aws_sfn_state_machine.redshift_load.arn
}
