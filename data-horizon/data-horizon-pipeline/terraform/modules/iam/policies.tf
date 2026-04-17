# =============================================================================
# Lambda Orchestrator Policy
# =============================================================================

data "aws_iam_policy_document" "orchestrator" {
  # S3: read config, write orchestration
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_config_bucket_arn,
      "${var.s3_config_bucket_arn}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${var.s3_orchestration_bucket_arn}/*",
    ]
  }

  # DynamoDB: read and write run metadata and tag records
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:BatchWriteItem",
    ]
    resources = [var.dynamodb_table_arn]
  }

  # Secrets Manager: read pipeline config
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.secret_arn]
  }
}

# =============================================================================
# Lambda Map State Processor Policy
# =============================================================================

data "aws_iam_policy_document" "map_processor" {
  # S3: write raw data
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${var.s3_raw_bucket_arn}/*"]
  }

  # DynamoDB: read and write item status
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
    ]
    resources = [var.dynamodb_table_arn]
  }

  # SQS: send to extraction failures queue
  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.extraction_failures_queue_arn]
  }

  # Secrets Manager: read pipeline config
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.secret_arn]
  }
}

# =============================================================================
# Glue Policy
# =============================================================================

data "aws_iam_policy_document" "glue" {
  # S3: read raw, write cleaned/validated/bad
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_raw_bucket_arn,
      "${var.s3_raw_bucket_arn}/*",
      var.s3_scripts_bucket_arn,
      "${var.s3_scripts_bucket_arn}/*",
    ]
  }

  statement {
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "${var.s3_cleaned_bucket_arn}/*",
      "${var.s3_validated_bucket_arn}/*",
      "${var.s3_bad_bucket_arn}/*",
    ]
  }

  # Glue Data Catalog
  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:UpdateTable",
    ]
    resources = ["*"]
  }

  # Secrets Manager: read pipeline config
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.secret_arn]
  }
}

# =============================================================================
# Step Functions Policy
# =============================================================================

data "aws_iam_policy_document" "step_functions" {
  # Invoke and monitor child Step Functions
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution",
      "states:DescribeExecution",
      "states:StopExecution",
    ]
    resources = ["arn:aws:states:*:*:stateMachine:${var.name_prefix}-*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["states:DescribeExecution", "states:StopExecution"]
    resources = ["arn:aws:states:*:*:execution:${var.name_prefix}-*:*"]
  }

  # EventBridge managed rules (required for .sync integrations)
  statement {
    effect = "Allow"
    actions = [
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
    ]
    resources = ["arn:aws:events:*:*:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"]
  }

  # S3: read map input JSON for distributed Map State ItemReader
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_orchestration_bucket_arn,
      "${var.s3_orchestration_bucket_arn}/*",
    ]
  }

  # Invoke Lambdas
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:*:*:function:${var.name_prefix}-*"]
  }

  # Start Glue jobs
  statement {
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }

  # Redshift Data API (for Child4)
  statement {
    effect = "Allow"
    actions = [
      "redshift-data:ExecuteStatement",
      "redshift-data:DescribeStatement",
      "redshift-data:GetStatementResult",
    ]
    resources = ["*"]
  }

  # Redshift credentials
  statement {
    effect    = "Allow"
    actions   = ["redshift:GetClusterCredentials"]
    resources = ["*"]
  }

  # SNS: publish failure notifications
  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }

  # CloudWatch Logs — delivery management
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

# =============================================================================
# EventBridge Policy
# =============================================================================

data "aws_iam_policy_document" "eventbridge" {
  # Invoke parent Step Function
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = ["arn:aws:states:*:*:stateMachine:${var.name_prefix}-*"]
  }

  # SQS: send to eventbridge failures queue
  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.eventbridge_failures_queue_arn]
  }
}

