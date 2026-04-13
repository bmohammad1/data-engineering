# =============================================================================
# Lambda Orchestrator Role
# =============================================================================

resource "aws_iam_role" "lambda_orchestrator" {
  name = "${var.name_prefix}-lambda-orchestrator-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "orchestrator_basic" {
  role       = aws_iam_role.lambda_orchestrator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "orchestrator_custom" {
  name   = "${var.name_prefix}-orchestrator-policy"
  role   = aws_iam_role.lambda_orchestrator.id
  policy = data.aws_iam_policy_document.orchestrator.json
}

# =============================================================================
# Lambda Map State Processor Role
# =============================================================================

resource "aws_iam_role" "lambda_map_processor" {
  name = "${var.name_prefix}-lambda-map-processor-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "map_processor_basic" {
  role       = aws_iam_role.lambda_map_processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "map_processor_custom" {
  name   = "${var.name_prefix}-map-processor-policy"
  role   = aws_iam_role.lambda_map_processor.id
  policy = data.aws_iam_policy_document.map_processor.json
}

# =============================================================================
# Glue Role
# =============================================================================

resource "aws_iam_role" "glue" {
  name = "${var.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_custom" {
  name   = "${var.name_prefix}-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

# =============================================================================
# Step Functions Role
# =============================================================================

resource "aws_iam_role" "step_functions" {
  name = "${var.name_prefix}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "step_functions_custom" {
  name   = "${var.name_prefix}-step-functions-policy"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}

# =============================================================================
# EventBridge Role
# =============================================================================

resource "aws_iam_role" "eventbridge" {
  name = "${var.name_prefix}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "eventbridge_custom" {
  name   = "${var.name_prefix}-eventbridge-policy"
  role   = aws_iam_role.eventbridge.id
  policy = data.aws_iam_policy_document.eventbridge.json
}

