resource "aws_glue_job" "transform" {
  name     = "${var.name_prefix}-transform"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/scripts/transform_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SECRET_NAME"                      = var.secret_name
    "--RAW_BUCKET"                       = var.raw_bucket_name
    "--CLEANED_BUCKET"                   = var.cleaned_bucket_name
    "--PIPELINE_STATE_TABLE"             = var.pipeline_state_table
    "--ENVIRONMENT"                      = var.environment
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${var.scripts_bucket_name}/scripts/utils.zip"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-transform"
  })
}

resource "aws_glue_job" "validation" {
  name     = "${var.name_prefix}-validation"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/scripts/validation_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SECRET_NAME"                      = var.secret_name
    "--CLEANED_BUCKET"                   = var.cleaned_bucket_name
    "--VALIDATED_BUCKET"                 = var.validated_bucket_name
    "--QUARANTINE_BUCKET"                = var.quarantine_bucket_name
    "--PIPELINE_STATE_TABLE"             = var.pipeline_state_table
    "--GLUE_DATABASE"                    = var.glue_database
    "--ENVIRONMENT"                      = var.environment
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${var.scripts_bucket_name}/scripts/utils.zip"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-validation"
  })
}
