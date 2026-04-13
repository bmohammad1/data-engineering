resource "aws_redshift_subnet_group" "this" {
  name       = "${var.name_prefix}-subnet-group"
  subnet_ids = [var.subnet_id]

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-subnet-group"
  })
}

resource "aws_redshift_cluster" "this" {
  cluster_identifier = "${var.name_prefix}-cluster"

  node_type       = var.node_type
  number_of_nodes = var.number_of_nodes
  cluster_type    = var.number_of_nodes > 1 ? "multi-node" : "single-node"
  database_name   = var.database_name
  master_username = var.master_username
  master_password = var.master_password

  cluster_subnet_group_name = aws_redshift_subnet_group.this.name
  vpc_security_group_ids    = [var.security_group_id]

  # Enhanced VPC routing keeps COPY/UNLOAD traffic on the VPC network.
  enhanced_vpc_routing = true

  # IAM role for S3 COPY access.
  iam_roles = [var.redshift_role_arn]

  # Skip final snapshot in dev/staging to ease destroy; override in prod if needed.
  skip_final_snapshot = true
  publicly_accessible = false
  encrypted           = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-cluster"
  })
}
