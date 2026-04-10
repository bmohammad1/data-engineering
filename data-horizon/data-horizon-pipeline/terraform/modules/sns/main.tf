resource "aws_sns_topic" "pipeline_failure_alerts" {
  name = "${var.name_prefix}-pipeline-failure-alerts"

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-pipeline-failure-alerts"
  })
}
