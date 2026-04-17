# =============================================================================
# CloudWatch Log Groups for Step Functions execution logging
# =============================================================================

resource "aws_cloudwatch_log_group" "modular_orchestrator" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-modular-orchestrator"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-modular-orchestrator-logs"
  })
}

resource "aws_cloudwatch_log_group" "config_loader" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-config-loader"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-config-loader-logs"
  })
}

resource "aws_cloudwatch_log_group" "data_extractor" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-data-extractor"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-data-extractor-logs"
  })
}

resource "aws_cloudwatch_log_group" "transformation" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-transformation"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-transformation-logs"
  })
}

resource "aws_cloudwatch_log_group" "redshift_load" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-redshift-load"
  retention_in_days = var.retention_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-redshift-load-logs"
  })
}

# =============================================================================
# Config Loader — loads source config and generates the extraction map file
# =============================================================================

resource "aws_sfn_state_machine" "config_loader" {
  name     = "${var.name_prefix}-config-loader"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${var.statemachine_dir}/config_loader.asl.json", {
    orchestrator_lambda_arn = var.orchestrator_lambda_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.config_loader.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

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
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.data_extractor.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

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

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.transformation.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

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
    sns_topic_arn            = var.sns_topic_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.redshift_load.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

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

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.modular_orchestrator.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-modular-orchestrator"
  })
}
