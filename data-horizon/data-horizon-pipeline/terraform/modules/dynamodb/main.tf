resource "aws_dynamodb_table" "pipeline_state" {
  name         = "${var.name_prefix}-pipeline-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "RunId"

  attribute {
    name = "RunId"
    type = "S"
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-pipeline-state"
  })
}
