bucket         = "data-horizon-terraform-state-dev-15"
key            = "data-horizon-pipeline/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "terraform-state-lock-dev"
encrypt        = true
