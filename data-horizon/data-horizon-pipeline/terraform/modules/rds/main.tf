resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds-postgres"
  description = "Allow PostgreSQL access from Lambda and Glue within the VPC"
  vpc_id      = var.vpc_id

  ingress {
    description = "PostgreSQL from within VPC"
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
    Name = "${var.name_prefix}-rds-postgres"
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
  identifier             = "${var.name_prefix}-pipeline-audit"
  engine                 = "postgres"
  engine_version         = "16.13"
  instance_class         = var.instance_class
  allocated_storage      = 20
  max_allocated_storage  = 100
  storage_type           = "gp2"
  storage_encrypted      = true

  db_name                = var.db_name
  username               = var.db_username
  password               = var.db_password
  port                   = 5432

  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = var.multi_az

  backup_retention_period   = 7
  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = var.skip_final_snapshot
  final_snapshot_identifier = "${var.name_prefix}-pipeline-audit-final-snapshot"

  iam_database_authentication_enabled = true
  performance_insights_enabled        = true
  auto_minor_version_upgrade          = true

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

resource "aws_ssm_parameter" "rds_instance_arn" {
  name  = "/data-horizon/${var.environment}/rds-instance-arn"
  type  = "String"
  value = aws_db_instance.pipeline_audit.arn

  tags = var.tags
}
