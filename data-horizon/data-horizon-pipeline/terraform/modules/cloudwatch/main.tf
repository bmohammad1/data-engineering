# Lambda log groups are created in the lambda module. Glue job logs are
# written to /aws-glue/jobs/* by default. This module provisions log groups
# for Step Functions and any auxiliary log groups, plus all alarms.

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-parent-pipeline"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-step-functions-logs"
  })
}

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
