resource "aws_cloudwatch_event_rule" "pipeline_schedule" {
  name                = "${var.name_prefix}-pipeline-schedule"
  description         = "Triggers the data-horizon parent pipeline every 6 hours"
  schedule_expression = var.schedule_expression

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-pipeline-schedule"
  })
}

resource "aws_cloudwatch_event_target" "parent_state_machine" {
  rule     = aws_cloudwatch_event_rule.pipeline_schedule.name
  arn      = var.parent_state_machine_arn
  role_arn = var.eventbridge_role_arn

  dead_letter_config {
    arn = var.eventbridge_failures_queue_arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 600
    maximum_retry_attempts       = 3
  }
}
