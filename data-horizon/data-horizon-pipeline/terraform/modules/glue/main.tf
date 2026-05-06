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
    "--ENVIRONMENT"                      = var.environment
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${var.scripts_bucket_name}/scripts/utils.zip"
    "--conf"                             = "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.unsafe=true"
  }

  glue_version      = "4.0"
  number_of_workers = var.transform_workers
  worker_type       = var.transform_worker_type
  timeout           = var.transform_timeout

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
    "--ENVIRONMENT"                      = var.environment
    "--LOG_LEVEL"                        = "INFO"
    "--extra-py-files"                   = "s3://${var.scripts_bucket_name}/scripts/utils.zip"
    "--conf"                             = "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.unsafe=true"
  }

  glue_version      = "4.0"
  number_of_workers = var.validation_workers
  worker_type       = var.validation_worker_type
  timeout           = var.validation_timeout

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-validation"
  })
}
