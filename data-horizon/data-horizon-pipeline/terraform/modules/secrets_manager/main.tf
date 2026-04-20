resource "aws_secretsmanager_secret" "pipeline_config" {
  name        = "${var.name_prefix}-pipeline-config"
  description = "Runtime configuration for the data-horizon pipeline"

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-pipeline-config"
  })
}

resource "aws_secretsmanager_secret_version" "pipeline_config" {
  secret_id = aws_secretsmanager_secret.pipeline_config.id

  secret_string = jsonencode({
    source_api_token = var.source_api_token
  })
}
