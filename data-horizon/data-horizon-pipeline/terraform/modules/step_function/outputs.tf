output "parent_state_machine_arn" {
  description = "Parent Step Function state machine ARN"
  value       = aws_sfn_state_machine.parent_pipeline.arn
}

output "child1_state_machine_arn" {
  description = "Child1 (config) state machine ARN"
  value       = aws_sfn_state_machine.child1_config.arn
}

output "child2_state_machine_arn" {
  description = "Child2 (extraction) state machine ARN"
  value       = aws_sfn_state_machine.child2_extraction.arn
}

output "child3_state_machine_arn" {
  description = "Child3 (transformation) state machine ARN"
  value       = aws_sfn_state_machine.child3_transformation.arn
}

output "child4_state_machine_arn" {
  description = "Child4 (Redshift load) state machine ARN"
  value       = aws_sfn_state_machine.child4_redshift_load.arn
}
