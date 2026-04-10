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
    # S3 bucket names
    raw_bucket_name           = var.raw_bucket_name
    cleaned_bucket_name       = var.cleaned_bucket_name
    parquet_bucket_name       = var.parquet_bucket_name
    bad_bucket_name           = var.bad_bucket_name
    scripts_bucket_name       = var.scripts_bucket_name
    orchestration_bucket_name = var.orchestration_bucket_name
    config_bucket_name        = var.config_bucket_name

    # DynamoDB
    pipeline_state_table = var.dynamodb_table_name

    # Source API
    source_api_base_url      = var.source_api_base_url
    source_api_token_url     = var.source_api_token_url
    source_api_client_id     = var.source_api_client_id
    source_api_client_secret = var.source_api_client_secret

    # Redshift
    redshift_host     = var.redshift_host
    redshift_port     = 5439
    redshift_database = var.redshift_database
    redshift_schema   = "public"

    # SQS
    eventbridge_failures_queue_url = var.eventbridge_failures_queue_url
    extraction_failures_queue_url  = var.extraction_failures_queue_url

    # SNS
    alert_topic_arn = var.sns_topic_arn

    # Pipeline settings
    map_state_concurrency = var.map_state_concurrency

    # Glue job parameters
    glue_dpu_count  = var.glue_dpu_count
    glue_timeout_min = 60
  })
}
