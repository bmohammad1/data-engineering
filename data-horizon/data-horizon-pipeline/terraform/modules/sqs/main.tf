resource "aws_sqs_queue" "eventbridge_failures" {
  name                      = "${var.name_prefix}-eventbridge-failures"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-eventbridge-failures"
  })
}

resource "aws_sqs_queue" "extraction_failures" {
  name                      = "${var.name_prefix}-extraction-failures"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-extraction-failures"
  })
}
