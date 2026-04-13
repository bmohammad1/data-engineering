# =============================================================================
# Child1: Config & Map Generation — invokes orchestrator Lambda
# =============================================================================

resource "aws_sfn_state_machine" "child1_config" {
  name     = "${var.name_prefix}-child1-config"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/child1_config.asl.json", {
    orchestrator_lambda_arn = var.orchestrator_lambda_arn
    sns_topic_arn           = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-child1-config"
  })
}

# =============================================================================
# Child2: Extraction — Map State invoking map_state_processor Lambda
# =============================================================================

resource "aws_sfn_state_machine" "child2_extraction" {
  name     = "${var.name_prefix}-child2-extraction"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/child2_extraction.asl.json", {
    map_state_processor_lambda_arn = var.map_state_processor_lambda_arn
    map_state_concurrency          = var.map_state_concurrency
    sns_topic_arn                  = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-child2-extraction"
  })
}

# =============================================================================
# Child3: Transformation — invokes Glue transform + validation jobs
# =============================================================================

resource "aws_sfn_state_machine" "child3_transformation" {
  name     = "${var.name_prefix}-child3-transformation"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/child3_transformation.asl.json", {
    transform_glue_job_name  = var.transform_glue_job_name
    validation_glue_job_name = var.validation_glue_job_name
    sns_topic_arn            = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-child3-transformation"
  })
}

# =============================================================================
# Child4: Redshift Staging Load — truncate + COPY via Redshift Data API
# =============================================================================

resource "aws_sfn_state_machine" "child4_redshift_load" {
  name     = "${var.name_prefix}-child4-redshift-load"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/child4_redshift_load.asl.json", {
    redshift_cluster_id      = var.redshift_cluster_id
    redshift_database        = var.redshift_database
    redshift_master_username = var.redshift_master_username
    parquet_bucket_name      = var.parquet_bucket_name
    redshift_iam_role_arn    = var.redshift_iam_role_arn
    sns_topic_arn            = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-child4-redshift-load"
  })
}

# =============================================================================
# Parent: orchestrates Child1 → Child2 → Child3 → Child4
# =============================================================================

resource "aws_sfn_state_machine" "parent_pipeline" {
  name     = "${var.name_prefix}-parent-pipeline"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/parent_pipeline.asl.json", {
    child1_arn    = aws_sfn_state_machine.child1_config.arn
    child2_arn    = aws_sfn_state_machine.child2_extraction.arn
    child3_arn    = aws_sfn_state_machine.child3_transformation.arn
    child4_arn    = aws_sfn_state_machine.child4_redshift_load.arn
    sns_topic_arn = var.sns_topic_arn
  })

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-parent-pipeline"
  })
}
