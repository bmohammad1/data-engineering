resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_failure_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
