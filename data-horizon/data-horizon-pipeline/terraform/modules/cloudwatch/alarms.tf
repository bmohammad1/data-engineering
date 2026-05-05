# =============================================================================
# Step Functions — primary failure signal
#
# ExecutionsFailed on the parent is the canonical pipeline failure alarm.
# Every unrecovered Lambda, Glue, or Redshift failure propagates here via
# child Fail states caught by the parent States.ALL handler.
# Child alarms are added independently because a caught child failure does
# not always increment the parent ExecutionsFailed metric.
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "parent_pipeline_failed" {
  alarm_name          = "${var.name_prefix}-parent-pipeline-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Parent pipeline Step Function had a failed execution — canonical pipeline failure signal"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.parent_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "child1_config_failed" {
  alarm_name          = "${var.name_prefix}-child1-config-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Config Loader (Child1) Step Function had a failed execution"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.child1_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "child2_extraction_failed" {
  alarm_name          = "${var.name_prefix}-child2-extraction-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Data Extractor (Child2) Step Function had a failed execution"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.child2_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "child3_transformation_failed" {
  alarm_name          = "${var.name_prefix}-child3-transformation-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Transform + Validation (Child3) Step Function had a failed execution"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.child3_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "child4_redshift_load_failed" {
  alarm_name          = "${var.name_prefix}-child4-redshift-load-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Redshift Load (Child4) Step Function had a failed execution"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.child4_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "parent_pipeline_timed_out" {
  alarm_name          = "${var.name_prefix}-parent-pipeline-timed-out"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsTimedOut"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Parent pipeline execution timed out — usually a stuck Glue job or Lambda"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.parent_state_machine_arn
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "parent_pipeline_throttled" {
  alarm_name          = "${var.name_prefix}-parent-pipeline-throttled"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionThrottled"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Parent pipeline Step Function executions are being throttled"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.parent_state_machine_arn
  }

  tags = var.tags
}

# =============================================================================
# Lambda — pre-failure signals only
#
# Lambda Errors are NOT alarmed separately — they are retried by child Step
# Functions and surface as ExecutionsFailed above if unrecoverable.
# Throttles and Duration are alarmed because they occur before SF retry logic
# sees the invocation (throttles are rejected at the Lambda API layer).
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "config_loader_throttles" {
  alarm_name          = "${var.name_prefix}-config-loader-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Config Loader Lambda is being throttled — requests rejected before SF retry logic"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = var.config_loader_function_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "map_processor_throttles" {
  alarm_name          = "${var.name_prefix}-map-processor-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Map state processor Lambda is being throttled — high concurrency fan-out hitting account limits"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = var.map_state_processor_function_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "config_loader_duration" {
  alarm_name          = "${var.name_prefix}-config-loader-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.config_loader_timeout_ms * 0.8
  alarm_description   = "Config Loader Lambda duration exceeds 80% of configured timeout — at risk of hard kill"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = var.config_loader_function_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "map_processor_duration" {
  alarm_name          = "${var.name_prefix}-map-processor-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.map_processor_timeout_ms * 0.8
  alarm_description   = "Map state processor Lambda duration exceeds 80% of configured timeout"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = var.map_state_processor_function_name
  }

  tags = var.tags
}

# =============================================================================
# Glue jobs — internal health signals
#
# SF only knows pass/fail per job run. These metrics surface problems
# developing inside the job before it terminates, and silent data loss
# even when the job reports success.
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "glue_transform_failed_tasks" {
  alarm_name          = "${var.name_prefix}-glue-transform-failed-tasks"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Glue transform job has Spark task failures — job may still succeed but indicates data skew or corrupt records"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName  = var.transform_glue_job_name
    JobRunId = "ALL"
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "glue_validation_failed_tasks" {
  alarm_name          = "${var.name_prefix}-glue-validation-failed-tasks"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Glue validation job has Spark task failures — job may still succeed but indicates data skew or corrupt records"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName  = var.validation_glue_job_name
    JobRunId = "ALL"
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "glue_transform_heap" {
  alarm_name          = "${var.name_prefix}-glue-transform-heap"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "glue.driver.jvm.heap.used"
  namespace           = "Glue"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.glue_heap_threshold_bytes
  alarm_description   = "Glue transform job JVM heap usage exceeds 80% — OOM risk from large tag batches or wide shuffles"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName  = var.transform_glue_job_name
    JobRunId = "ALL"
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "glue_validation_heap" {
  alarm_name          = "${var.name_prefix}-glue-validation-heap"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "glue.driver.jvm.heap.used"
  namespace           = "Glue"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.glue_heap_threshold_bytes
  alarm_description   = "Glue validation job JVM heap usage exceeds 80% — OOM risk"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName  = var.validation_glue_job_name
    JobRunId = "ALL"
  }

  tags = var.tags
}

# =============================================================================
# Custom business metrics (emitted by Glue jobs via put_metric_data)
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "validation_rejection_rate_high" {
  alarm_name          = "${var.name_prefix}-validation-rejection-rate-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ValidationRejectionRate"
  namespace           = "DataHorizon/Validation"
  period              = 3600
  statistic           = "Maximum"
  threshold           = 15
  alarm_description   = "Validation rejection rate exceeds 15% — possible upstream schema change or API mutation rate shift"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "tags_failed_per_run_high" {
  alarm_name          = "${var.name_prefix}-tags-failed-per-run-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "TagsFailedPerRun"
  namespace           = "DataHorizon/Validation"
  period              = 3600
  statistic           = "Maximum"
  threshold           = 50
  alarm_description   = "More than 50 tags had all records quarantined in a single run (>1% of 5000 tags)"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "pipeline_run_duration_high" {
  alarm_name          = "${var.name_prefix}-pipeline-run-duration-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "PipelineRunDurationMinutes"
  namespace           = "DataHorizon/Pipeline"
  period              = 3600
  statistic           = "Maximum"
  threshold           = 300
  alarm_description   = "Pipeline run exceeded 300 minutes — next EventBridge trigger will fire before this run completes, causing DynamoDB conflicts"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  tags = var.tags
}

# =============================================================================
# SQS Dead Letter Queue
#
# The Data Extractor map state tolerates 50% item failure. A failed item
# does NOT fail the child state machine — the DLQ is the only signal for
# partial extraction failures (missing tags in this run).
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "extraction_failures_present" {
  alarm_name          = "${var.name_prefix}-extraction-failures-present"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Messages in extraction-failures DLQ — at least one tag batch failed after all retries, data gap in this run"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = var.extraction_failures_queue_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "extraction_failures_age" {
  alarm_name          = "${var.name_prefix}-extraction-failures-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 21600
  alarm_description   = "Extraction DLQ messages older than 6 hours — failures accumulating across runs without investigation"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = var.extraction_failures_queue_name
  }

  tags = var.tags
}

# =============================================================================
# DynamoDB — infrastructure health
#
# DynamoDB errors do not immediately fail Lambda or Glue in all code paths.
# A write failure mid-job can leave pipeline state corrupt without killing
# the job, so these are monitored directly rather than relying on SF propagation.
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "dynamodb_system_errors" {
  alarm_name          = "${var.name_prefix}-dynamodb-system-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "SystemErrors"
  namespace           = "AWS/DynamoDB"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "DynamoDB 5xx errors — transient service-side failures on pipeline state table"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = var.dynamodb_table_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "dynamodb_user_errors" {
  alarm_name          = "${var.name_prefix}-dynamodb-user-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "UserErrors"
  namespace           = "AWS/DynamoDB"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "DynamoDB 4xx errors — bad requests against pipeline state table, likely a code bug (wrong table name, schema mismatch)"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = var.dynamodb_table_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "dynamodb_throttled_requests" {
  alarm_name          = "${var.name_prefix}-dynamodb-throttled-requests"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ThrottledRequests"
  namespace           = "AWS/DynamoDB"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "DynamoDB throttled requests — bulk tag status writes from Glue hitting on-demand capacity limits"
  alarm_actions       = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = var.dynamodb_table_name
  }

  tags = var.tags
}
