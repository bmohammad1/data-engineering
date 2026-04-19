# =============================================================================
# Lambda error alarms
# =============================================================================

# resource "aws_cloudwatch_metric_alarm" "config_loader_errors" {
#   alarm_name          = "${var.name_prefix}-config-loader-errors"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 5
#   alarm_description   = "Config Loader Lambda error count exceeded threshold"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     FunctionName = var.config_loader_function_name
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "map_processor_errors" {
#   alarm_name          = "${var.name_prefix}-map-processor-errors"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 5
#   alarm_description   = "Map state processor Lambda error count exceeded threshold"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     FunctionName = var.map_state_processor_function_name
#   }
#
#   tags = var.tags
# }

# =============================================================================
# Step Function execution failure alarm
# =============================================================================

# resource "aws_cloudwatch_metric_alarm" "parent_pipeline_failed" {
#   alarm_name          = "${var.name_prefix}-parent-pipeline-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ExecutionsFailed"
#   namespace           = "AWS/States"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Parent pipeline Step Function had a failed execution"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     StateMachineArn = var.parent_state_machine_arn
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "child1_config_failed" {
#   alarm_name          = "${var.name_prefix}-child1-config-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ExecutionsFailed"
#   namespace           = "AWS/States"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Config Loader Step Function had a failed execution"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     StateMachineArn = var.child1_state_machine_arn
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "child2_extraction_failed" {
#   alarm_name          = "${var.name_prefix}-child2-extraction-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ExecutionsFailed"
#   namespace           = "AWS/States"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Data Extractor Step Function had a failed execution"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     StateMachineArn = var.child2_state_machine_arn
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "child3_transformation_failed" {
#   alarm_name          = "${var.name_prefix}-child3-transformation-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ExecutionsFailed"
#   namespace           = "AWS/States"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Transformation Step Function had a failed execution"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     StateMachineArn = var.child3_state_machine_arn
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "child4_redshift_load_failed" {
#   alarm_name          = "${var.name_prefix}-child4-redshift-load-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ExecutionsFailed"
#   namespace           = "AWS/States"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Redshift Load Step Function had a failed execution"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     StateMachineArn = var.child4_state_machine_arn
#   }
#
#   tags = var.tags
# }

# =============================================================================
# Glue job failure alarms
# =============================================================================

# resource "aws_cloudwatch_metric_alarm" "glue_transform_failed" {
#   alarm_name          = "${var.name_prefix}-glue-transform-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "glue.driver.aggregate.numFailedTasks"
#   namespace           = "Glue"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Glue transform job reported failed tasks"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     JobName  = var.transform_glue_job_name
#     JobRunId = "ALL"
#   }
#
#   tags = var.tags
# }

# resource "aws_cloudwatch_metric_alarm" "glue_validation_failed" {
#   alarm_name          = "${var.name_prefix}-glue-validation-failed"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "glue.driver.aggregate.numFailedTasks"
#   namespace           = "Glue"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Glue validation job reported failed tasks"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     JobName  = var.validation_glue_job_name
#     JobRunId = "ALL"
#   }
#
#   tags = var.tags
# }

# =============================================================================
# DLQ / extraction failures alarm
# =============================================================================

# resource "aws_cloudwatch_metric_alarm" "extraction_failures_present" {
#   alarm_name          = "${var.name_prefix}-extraction-failures-present"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ApproximateNumberOfMessagesVisible"
#   namespace           = "AWS/SQS"
#   period              = 300
#   statistic           = "Maximum"
#   threshold           = 0
#   alarm_description   = "Messages present in extraction-failures queue"
#   alarm_actions       = [var.sns_topic_arn]
#   treat_missing_data  = "notBreaching"
#
#   dimensions = {
#     QueueName = var.extraction_failures_queue_name
#   }
#
#   tags = var.tags
# }
