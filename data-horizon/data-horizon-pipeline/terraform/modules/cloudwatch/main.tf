# Lambda log groups are created in the lambda module. Glue job logs are
# written to /aws-glue/jobs/* by default. Step Functions log groups are
# managed in the step_function module alongside the state machines.

resource "aws_cloudwatch_log_group" "glue_transform" {
  name              = "/aws-glue/jobs/${var.transform_glue_job_name}"
  retention_in_days = var.retention_days

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "glue_validation" {
  name              = "/aws-glue/jobs/${var.validation_glue_job_name}"
  retention_in_days = var.retention_days

  tags = var.tags
}
