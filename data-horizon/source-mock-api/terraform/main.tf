data "aws_caller_identity" "current" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  function_name = "mock-source-api-${var.environment}"
  lambda_zip    = "${path.module}/../build/lambda.zip"
}

# =============================================================================
# IAM Role for Lambda
# =============================================================================

resource "aws_iam_role" "lambda" {
  name = "${local.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# =============================================================================
# IAM Role for API Gateway CloudWatch Logging
# =============================================================================

resource "aws_iam_role" "apigw_cloudwatch" {
  name = "api-gateway-cloudwatch-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "apigateway.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "apigw_cloudwatch" {
  role       = aws_iam_role.apigw_cloudwatch.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}

resource "aws_api_gateway_account" "this" {
  cloudwatch_role_arn = aws_iam_role.apigw_cloudwatch.arn

  depends_on = [aws_iam_role_policy_attachment.apigw_cloudwatch]
}

# =============================================================================
# Lambda Function (Zip)
# =============================================================================

resource "aws_lambda_function" "this" {
  function_name    = local.function_name
  role             = aws_iam_role.lambda.arn
  handler          = "lambda_handler.handler"
  runtime          = "python3.12"
  filename         = local.lambda_zip
  source_code_hash = filebase64sha256(local.lambda_zip)
  memory_size      = var.lambda_memory_size
  timeout          = var.lambda_timeout
  architectures    = ["x86_64"]
}

# =============================================================================
# API Gateway REST API
# =============================================================================

resource "aws_api_gateway_rest_api" "this" {
  name = "mock-source-api-${var.environment}"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

# ---------- Cognito Authorizer ----------

resource "aws_api_gateway_authorizer" "cognito" {
  name          = "cognito-m2m"
  rest_api_id   = aws_api_gateway_rest_api.this.id
  type          = "COGNITO_USER_POOLS"
  provider_arns = [aws_cognito_user_pool.this.arn]
}

# ---------- Resources ----------

# /tags
resource "aws_api_gateway_resource" "tags" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "tags"
}

# /tag
resource "aws_api_gateway_resource" "tag" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "tag"
}

# /tag/{tag_id}
resource "aws_api_gateway_resource" "tag_id" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.tag.id
  path_part   = "{tag_id}"
}

# ---------- GET /tags ----------

resource "aws_api_gateway_method" "get_tags" {
  rest_api_id          = aws_api_gateway_rest_api.this.id
  resource_id          = aws_api_gateway_resource.tags.id
  http_method          = "GET"
  authorization        = "COGNITO_USER_POOLS"
  authorizer_id        = aws_api_gateway_authorizer.cognito.id
  authorization_scopes = ["mock-source-api/read"]
}

resource "aws_api_gateway_integration" "get_tags" {
  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = aws_api_gateway_resource.tags.id
  http_method             = aws_api_gateway_method.get_tags.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.this.invoke_arn
}

# ---------- GET /tag/{tag_id} ----------

resource "aws_api_gateway_method" "get_tag" {
  rest_api_id          = aws_api_gateway_rest_api.this.id
  resource_id          = aws_api_gateway_resource.tag_id.id
  http_method          = "GET"
  authorization        = "COGNITO_USER_POOLS"
  authorizer_id        = aws_api_gateway_authorizer.cognito.id
  authorization_scopes = ["mock-source-api/read"]
}

resource "aws_api_gateway_integration" "get_tag" {
  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = aws_api_gateway_resource.tag_id.id
  http_method             = aws_api_gateway_method.get_tag.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.this.invoke_arn
}

# ---------- CORS (OPTIONS /tags) ----------

resource "aws_api_gateway_method" "options_tags" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = aws_api_gateway_resource.tags.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options_tags" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tags.id
  http_method = aws_api_gateway_method.options_tags.http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options_tags" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tags.id
  http_method = aws_api_gateway_method.options_tags.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options_tags" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tags.id
  http_method = aws_api_gateway_method.options_tags.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Authorization,Content-Type'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }

  depends_on = [aws_api_gateway_integration.options_tags]
}

# ---------- CORS (OPTIONS /tag/{tag_id}) ----------

resource "aws_api_gateway_method" "options_tag" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = aws_api_gateway_resource.tag_id.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options_tag" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tag_id.id
  http_method = aws_api_gateway_method.options_tag.http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options_tag" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tag_id.id
  http_method = aws_api_gateway_method.options_tag.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options_tag" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.tag_id.id
  http_method = aws_api_gateway_method.options_tag.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Authorization,Content-Type'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }

  depends_on = [aws_api_gateway_integration.options_tag]
}

# ---------- Deployment & Stage ----------

resource "aws_api_gateway_deployment" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.tags.id,
      aws_api_gateway_resource.tag_id.id,
      aws_api_gateway_method.get_tags.id,
      aws_api_gateway_method.get_tag.id,
      aws_api_gateway_integration.get_tags.id,
      aws_api_gateway_integration.get_tag.id,
      aws_api_gateway_method.options_tags.id,
      aws_api_gateway_method.options_tag.id,
      aws_api_gateway_authorizer.cognito.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  deployment_id = aws_api_gateway_deployment.this.id
  stage_name    = var.environment

  depends_on = [aws_api_gateway_account.this]

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      requestTime    = "$context.requestTime"
      httpMethod     = "$context.httpMethod"
      resourcePath   = "$context.resourcePath"
      status         = "$context.status"
      protocol       = "$context.protocol"
      responseLength = "$context.responseLength"
    })
  }
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/apigateway/mock-source-api-${var.environment}"
  retention_in_days = 14
}

# ---------- Lambda permission ----------

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# =============================================================================
# Cognito User Pool (M2M)
# =============================================================================

resource "aws_cognito_user_pool" "this" {
  name           = "mock-source-api-${var.environment}"
  user_pool_tier = "LITE"

  # No user sign-up needed — this pool is only for M2M client_credentials
  admin_create_user_config {
    allow_admin_create_user_only = true
  }
}

resource "aws_cognito_user_pool_domain" "this" {
  domain       = "mock-source-api-${local.account_id}-${var.environment}"
  user_pool_id = aws_cognito_user_pool.this.id
}

# ---------- Resource Server with custom scope ----------

resource "aws_cognito_resource_server" "this" {
  user_pool_id = aws_cognito_user_pool.this.id
  identifier   = "mock-source-api"
  name         = "MockSourceAPI"

  scope {
    scope_name        = "read"
    scope_description = "Read access to tag data"
  }
}

# ---------- App Client (M2M client_credentials) ----------

resource "aws_cognito_user_pool_client" "this" {
  name         = "mock-source-api-client-${var.environment}"
  user_pool_id = aws_cognito_user_pool.this.id

  generate_secret                      = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = ["mock-source-api/read"]
  allowed_oauth_flows_user_pool_client = true
  supported_identity_providers         = ["COGNITO"]

  # Cognito maximum — access tokens cannot exceed 24 hours
  access_token_validity = 1440
  token_validity_units {
    access_token = "minutes"
  }

  # Client must wait for the resource server that defines the custom scope
  depends_on = [aws_cognito_resource_server.this]
}

