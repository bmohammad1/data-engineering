# =============================================================================
# Config Loader — loads source config and generates the extraction map file
# =============================================================================

resource "aws_sfn_state_machine" "config_loader" {
  name     = "${var.name_prefix}-config-loader"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/config_loader.asl.json", {
    orchestrator_lambda_arn = var.config_loader_lambda_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-config-loader"
  })
}

# =============================================================================
# Data Extractor — Map State fan-out invoking map_state_processor Lambda
# =============================================================================

resource "aws_sfn_state_machine" "data_extractor" {
  name     = "${var.name_prefix}-data-extractor"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/data_extractor.asl.json", {
    map_state_processor_lambda_arn = var.map_state_processor_lambda_arn
    orchestration_bucket_name      = var.orchestration_bucket_name
    extraction_failures_queue_url  = var.extraction_failures_queue_url
    pipeline_state_table           = var.pipeline_state_table
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-data-extractor"
  })
}

# =============================================================================
# Transformation — invokes Glue transform + validation jobs
# =============================================================================

resource "aws_sfn_state_machine" "transformation" {
  name     = "${var.name_prefix}-transformation"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/transformation.asl.json", {
    transform_glue_job_name  = var.transform_glue_job_name
    validation_glue_job_name = var.validation_glue_job_name
    sns_topic_arn            = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-transformation"
  })
}

# =============================================================================
# Redshift Load — truncate + COPY to Redshift staging via Redshift Data API
# =============================================================================

resource "aws_sfn_state_machine" "redshift_load" {
  name     = "${var.name_prefix}-redshift-load"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/redshift_load.asl.json", {
    redshift_cluster_id      = var.redshift_cluster_id
    redshift_database        = var.redshift_database
    redshift_master_username = var.redshift_master_username
    validated_bucket_name    = var.validated_bucket_name
    redshift_iam_role_arn    = var.redshift_iam_role_arn
    pipeline_state_table     = var.pipeline_state_table
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-redshift-load"
  })
}

# =============================================================================
# Modular Orchestrator — coordinates Config Loader → Data Extractor → Transformation → Redshift Load
# =============================================================================

resource "aws_sfn_state_machine" "modular_orchestrator" {
  name     = "${var.name_prefix}-modular-orchestrator"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/modular_orchestrator.asl.json", {
    config_loader_arn  = aws_sfn_state_machine.config_loader.arn
    data_extractor_arn = aws_sfn_state_machine.data_extractor.arn
    transformation_arn = aws_sfn_state_machine.transformation.arn
    redshift_load_arn  = aws_sfn_state_machine.redshift_load.arn
    sns_topic_arn      = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-modular-orchestrator"
  })
}
