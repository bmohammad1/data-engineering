# =============================================================================
# Module composition — wires all infrastructure components together.
# =============================================================================

# --- VPC (for Redshift only) ---
module "vpc" {
  source = "./modules/vpc"

  name_prefix           = local.name_prefix
  vpc_cidr              = var.vpc_cidr
  private_subnet_cidr   = var.private_subnet_cidr
  private_subnet_cidr_2 = var.private_subnet_cidr_2
  tags                  = local.common_tags
}

# --- S3 Buckets ---
module "s3" {
  source = "./modules/s3"

  name_prefix = local.name_prefix
  tags        = local.common_tags
}

# --- DynamoDB ---
module "dynamodb" {
  source = "./modules/dynamodb"

  name_prefix = local.name_prefix
  tags        = local.common_tags
}

# --- SQS (eventbridge-failures + extraction-failures) ---
module "sqs" {
  source = "./modules/sqs"

  name_prefix = local.name_prefix
  tags        = local.common_tags
}

# --- SNS (failure alerts) ---
module "sns" {
  source = "./modules/sns"

  name_prefix = local.name_prefix
  alert_email = var.alert_email
  tags        = local.common_tags
}

# --- Redshift ---
module "redshift" {
  source = "./modules/redshift"

  name_prefix           = local.name_prefix
  node_type             = var.redshift_node_type
  number_of_nodes       = var.redshift_number_of_nodes
  database_name         = var.redshift_database_name
  master_username       = var.redshift_master_username
  master_password       = var.redshift_master_password
  subnet_ids            = module.vpc.subnet_ids
  security_group_id     = module.vpc.redshift_security_group_id
  s3_validated_bucket_arn = module.s3.validated_bucket_arn
  tags                  = local.common_tags
}

# --- Secrets Manager (API token only) ---
module "secrets_manager" {
  source = "./modules/secrets_manager"

  name_prefix      = local.name_prefix
  source_api_token = var.source_api_token
  tags             = local.common_tags
}

# --- IAM (roles + policies) ---
module "iam" {
  source = "./modules/iam"

  name_prefix = local.name_prefix
  secret_arn  = module.secrets_manager.secret_arn

  s3_raw_bucket_arn           = module.s3.raw_bucket_arn
  s3_cleaned_bucket_arn       = module.s3.cleaned_bucket_arn
  s3_validated_bucket_arn     = module.s3.validated_bucket_arn
  s3_bad_bucket_arn           = module.s3.bad_bucket_arn
  s3_scripts_bucket_arn       = module.s3.scripts_bucket_arn
  s3_orchestration_bucket_arn = module.s3.orchestration_bucket_arn
  s3_config_bucket_arn        = module.s3.config_bucket_arn

  dynamodb_table_arn = module.dynamodb.table_arn

  extraction_failures_queue_arn  = module.sqs.extraction_failures_queue_arn
  eventbridge_failures_queue_arn = module.sqs.eventbridge_failures_queue_arn

  sns_topic_arn = module.sns.topic_arn

  tags = local.common_tags
}

# --- Lambda functions ---
module "lambda" {
  source = "./modules/lambda"

  name_prefix            = local.name_prefix
  environment            = var.environment
  secret_name            = module.secrets_manager.secret_name
  config_loader_role_arn = module.iam.lambda_config_loader_role_arn
  map_processor_role_arn = module.iam.lambda_map_processor_role_arn
  memory_size            = var.lambda_memory_size
  timeout                = var.lambda_timeout

  pipeline_state_table      = module.dynamodb.table_name
  raw_bucket_name           = module.s3.raw_bucket_name
  config_bucket_name        = module.s3.config_bucket_name
  orchestration_bucket_name = module.s3.orchestration_bucket_name
  source_api_base_url       = var.source_api_base_url
  map_state_concurrency = var.map_state_concurrency

  tags = local.common_tags
}

# --- Glue jobs ---
module "glue" {
  source = "./modules/glue"

  name_prefix         = local.name_prefix
  glue_role_arn       = module.iam.glue_role_arn
  scripts_bucket_name = module.s3.scripts_bucket_name
  secret_name         = module.secrets_manager.secret_name
  tags                = local.common_tags
}

# --- Glue Data Catalog ---
module "glue_catalog" {
  source = "./modules/glue_catalog"

  name_prefix         = local.name_prefix
  raw_bucket_name     = module.s3.raw_bucket_name
  cleaned_bucket_name = module.s3.cleaned_bucket_name
  validated_bucket_name = module.s3.validated_bucket_name
  tags                  = local.common_tags
}

# --- Step Functions (parent + 4 children) ---
module "step_function" {
  source = "./modules/step_function"

  name_prefix                    = local.name_prefix
  step_functions_role_arn        = module.iam.step_functions_role_arn
  config_loader_lambda_arn       = module.lambda.config_loader_function_arn
  map_state_processor_lambda_arn = module.lambda.map_state_processor_function_arn
  transform_glue_job_name        = module.glue.transform_job_name
  validation_glue_job_name       = module.glue.validation_job_name
  redshift_cluster_id            = module.redshift.cluster_identifier
  redshift_database              = module.redshift.database_name
  redshift_master_username       = var.redshift_master_username
  validated_bucket_name          = module.s3.validated_bucket_name
  redshift_iam_role_arn          = module.redshift.redshift_role_arn
  sns_topic_arn                 = module.sns.topic_arn
  map_state_concurrency         = var.map_state_concurrency
  orchestration_bucket_name     = module.s3.orchestration_bucket_name
  extraction_failures_queue_url = module.sqs.extraction_failures_queue_url
  statemachine_dir              = "${path.module}/../statemachine"
  tags                          = local.common_tags
}

# --- EventBridge (6h schedule) ---
module "eventbridge" {
  source = "./modules/eventbridge"

  name_prefix                    = local.name_prefix
  parent_state_machine_arn       = module.step_function.parent_state_machine_arn
  eventbridge_role_arn           = module.iam.eventbridge_role_arn
  eventbridge_failures_queue_arn = module.sqs.eventbridge_failures_queue_arn
  tags                           = local.common_tags
  schedule_expression= var.sechedule_expression_for_eventbridge
}

# --- CloudWatch (log groups + alarms) ---
module "cloudwatch" {
  source = "./modules/cloudwatch"

  name_prefix                       = local.name_prefix
  retention_days                    = var.log_retention_days
  config_loader_function_name       = module.lambda.config_loader_function_name
  map_state_processor_function_name = module.lambda.map_state_processor_function_name
  transform_glue_job_name           = module.glue.transform_job_name
  validation_glue_job_name          = module.glue.validation_job_name
  parent_state_machine_arn          = module.step_function.parent_state_machine_arn
  child1_state_machine_arn          = module.step_function.child1_state_machine_arn
  child2_state_machine_arn          = module.step_function.child2_state_machine_arn
  child3_state_machine_arn          = module.step_function.child3_state_machine_arn
  child4_state_machine_arn          = module.step_function.child4_state_machine_arn
  extraction_failures_queue_name    = "${local.name_prefix}-extraction-failures"
  sns_topic_arn                     = module.sns.topic_arn
  tags                              = local.common_tags
}
