bucket         = "data-horizon-terraform-state-staging-23"
key            = "data-horizon-pipeline/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "terraform-state-lock-staging"
encrypt        = true
