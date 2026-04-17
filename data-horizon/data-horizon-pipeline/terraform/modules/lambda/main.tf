locals {
  config_loader_zip       = "${path.module}/../../../lambdas/orchestrator/package/lambda.zip"
  map_state_processor_zip = "${path.module}/../../../lambdas/map_state_processor/package/lambda.zip"
}

# =============================================================================
# Config Loader Lambda
# =============================================================================

resource "aws_lambda_function" "config_loader" {
  function_name    = "${var.name_prefix}-config-loader"
  role             = var.config_loader_role_arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = local.config_loader_zip
  source_code_hash = filebase64sha256(local.config_loader_zip)
  memory_size      = var.memory_size
  timeout          = var.timeout
  architectures    = ["x86_64"]

  environment {
    variables = {
      SECRET_NAME = var.secret_name
      ENVIRONMENT = var.environment
    }
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-config-loader"
  })
}

resource "aws_cloudwatch_log_group" "config_loader" {
  name              = "/aws/lambda/${aws_lambda_function.config_loader.function_name}"
  retention_in_days = 30

  tags = var.tags
}

# =============================================================================
# Map State Processor Lambda
# =============================================================================

resource "aws_lambda_function" "map_state_processor" {
  function_name    = "${var.name_prefix}-map-state-processor"
  role             = var.map_processor_role_arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = local.map_state_processor_zip
  source_code_hash = filebase64sha256(local.map_state_processor_zip)
  memory_size      = var.memory_size
  timeout          = var.timeout
  architectures    = ["x86_64"]

  environment {
    variables = {
      SECRET_NAME = var.secret_name
      ENVIRONMENT = var.environment
    }
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-map-state-processor"
  })
}

resource "aws_cloudwatch_log_group" "map_state_processor" {
  name              = "/aws/lambda/${aws_lambda_function.map_state_processor.function_name}"
  retention_in_days = 30

  tags = var.tags
}
