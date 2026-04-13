bucket         = "data-horizon-terraform-state-prod"
key            = "data-horizon-pipeline/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "terraform-state-lock-prod"
encrypt        = true
