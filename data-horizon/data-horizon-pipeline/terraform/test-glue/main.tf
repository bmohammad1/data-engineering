data "aws_caller_identity" "current" {}

locals {
  account_suffix = substr(data.aws_caller_identity.current.account_id, -8, -1)
  prefix         = "data-horizon-test"
}

# ---------------------------------------------------------------------------
# S3 Buckets
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.prefix}-raw-${local.account_suffix}"
  force_destroy = true
  tags          = { Name = "${local.prefix}-raw" }
}

resource "aws_s3_bucket" "cleaned" {
  bucket        = "${local.prefix}-cleaned-${local.account_suffix}"
  force_destroy = true
  tags          = { Name = "${local.prefix}-cleaned" }
}

resource "aws_s3_bucket" "scripts" {
  bucket        = "${local.prefix}-scripts-${local.account_suffix}"
  force_destroy = true
  tags          = { Name = "${local.prefix}-scripts" }
}

resource "aws_s3_bucket" "validated" {
  bucket        = "${local.prefix}-validated-${local.account_suffix}"
  force_destroy = true
  tags          = { Name = "${local.prefix}-validated" }
}

resource "aws_s3_bucket" "quarantine" {
  bucket        = "${local.prefix}-quarantine-${local.account_suffix}"
  force_destroy = true
  tags          = { Name = "${local.prefix}-quarantine" }
}

# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "pipeline_state" {
  name         = "${local.prefix}-pipeline-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  tags = { Name = "${local.prefix}-pipeline-state" }
}

# ---------------------------------------------------------------------------
# IAM Role for Glue
# ---------------------------------------------------------------------------

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_custom" {
  name = "${local.prefix}-glue-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.cleaned.arn,
          "${aws_s3_bucket.cleaned.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*",
          aws_s3_bucket.validated.arn,
          "${aws_s3_bucket.validated.arn}/*",
          aws_s3_bucket.quarantine.arn,
          "${aws_s3_bucket.quarantine.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"]
        Resource = aws_dynamodb_table.pipeline_state.arn
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParametersByPath", "ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/data-horizon/test/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
      },
      {
        Effect = "Allow"
        Action = ["glue:CreateTable", "glue:UpdateTable", "glue:GetDatabase", "glue:GetTable"]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/data_horizon_test",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/data_horizon_test/*",
        ]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# SSM Parameters (consumed by load_ssm_config("test"))
# ---------------------------------------------------------------------------

resource "aws_ssm_parameter" "raw_bucket" {
  name  = "/data-horizon/test/raw-bucket-name"
  type  = "String"
  value = aws_s3_bucket.raw.bucket
}

resource "aws_ssm_parameter" "cleaned_bucket" {
  name  = "/data-horizon/test/cleaned-bucket-name"
  type  = "String"
  value = aws_s3_bucket.cleaned.bucket
}

resource "aws_ssm_parameter" "pipeline_state_table" {
  name  = "/data-horizon/test/pipeline-state-table"
  type  = "String"
  value = aws_dynamodb_table.pipeline_state.name
}

resource "aws_ssm_parameter" "validated_bucket" {
  name  = "/data-horizon/test/validated-bucket-name"
  type  = "String"
  value = aws_s3_bucket.validated.bucket
}

resource "aws_ssm_parameter" "quarantine_bucket" {
  name  = "/data-horizon/test/quarantine-bucket-name"
  type  = "String"
  value = aws_s3_bucket.quarantine.bucket
}

resource "aws_ssm_parameter" "glue_database" {
  name  = "/data-horizon/test/glue-database"
  type  = "String"
  value = aws_glue_catalog_database.test.name
}

# ---------------------------------------------------------------------------
# Glue Data Catalog Database
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "test" {
  name = "data_horizon_test"
}

# ---------------------------------------------------------------------------
# Glue Job
# ---------------------------------------------------------------------------

resource "aws_glue_job" "transform" {
  name     = "${local.prefix}-transform"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/scripts/transform_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--ENVIRONMENT"                      = "test"
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.scripts.bucket}/scripts/utils.zip"
    "--conf"                             = "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.unsafe=true"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = { Name = "${local.prefix}-transform" }
}

resource "aws_glue_job" "validation" {
  name     = "${local.prefix}-validation"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/scripts/validation_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--ENVIRONMENT"                      = "test"
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.scripts.bucket}/scripts/utils.zip"
    "--conf"                             = "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.unsafe=true"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = { Name = "${local.prefix}-validation" }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "cleaned_bucket_name" {
  value = aws_s3_bucket.cleaned.bucket
}

output "scripts_bucket_name" {
  value = aws_s3_bucket.scripts.bucket
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}

output "validated_bucket_name" {
  value = aws_s3_bucket.validated.bucket
}

output "quarantine_bucket_name" {
  value = aws_s3_bucket.quarantine.bucket
}

output "validation_glue_job_name" {
  value = aws_glue_job.validation.name
}
