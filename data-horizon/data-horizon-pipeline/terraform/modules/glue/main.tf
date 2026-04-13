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
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-validation"
  })
}
