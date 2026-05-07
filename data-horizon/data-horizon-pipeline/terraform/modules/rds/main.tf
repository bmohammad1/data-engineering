resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds-pg"
  description = "Allow Postgres access from Lambda and Glue within the VPC"
  vpc_id      = var.vpc_id

  ingress {
    description = "Postgres from within VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-pg"
  })
}

resource "aws_db_subnet_group" "rds" {
  name       = "${var.name_prefix}-rds-subnet-group"
  subnet_ids = var.subnet_ids

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-subnet-group"
  })
}

resource "aws_db_instance" "pipeline_audit" {
  identifier        = "${var.name_prefix}-pipeline-audit"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.instance_class
  allocated_storage = var.allocated_storage

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  publicly_accessible     = false
  multi_az                = var.multi_az
  storage_encrypted       = true
  deletion_protection     = var.deletion_protection
  skip_final_snapshot     = var.skip_final_snapshot
  backup_retention_period = 7

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-pipeline-audit"
  })
}

resource "aws_ssm_parameter" "pg_connection_string" {
  name  = "/data-horizon/${var.environment}/postgres-connection-string"
  type  = "SecureString"
  value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pipeline_audit.endpoint}/${var.db_name}?sslmode=require"

  tags = var.tags
}
