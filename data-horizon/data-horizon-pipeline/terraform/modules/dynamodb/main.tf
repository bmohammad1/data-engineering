/*
  DynamoDB PipelineAudit table — PRESERVED FOR ROLLBACK REFERENCE
  Replaced by Amazon RDS PostgreSQL tables in the pipeline_audit schema.
  To re-enable: remove this block comment, delete the rds module from
  terraform/main.tf, and revert the commented-out DynamoDB code in the
  Python source files.

resource "aws_dynamodb_table" "pipeline_state" {
  name         = "PipelineAudit"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  attribute {
    name = "GSI1PK"
    type = "S"
  }

  attribute {
    name = "GSI1SK"
    type = "S"
  }

  global_secondary_index {
    name            = "GSI1_RunByPipeline"
    hash_key        = "GSI1PK"
    range_key       = "GSI1SK"
    projection_type = "ALL"
  }

  tags = merge(var.tags, {
    Name = "PipelineAudit"
  })
}
*/
