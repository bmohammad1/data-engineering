resource "aws_iam_role" "redshift" {
  name = "${var.name_prefix}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "redshift_s3_copy" {
  name = "${var.name_prefix}-redshift-s3-copy"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
      ]
      Resource = [
        var.s3_validated_bucket_arn,
        "${var.s3_validated_bucket_arn}/*",
      ]
    }]
  })
}

# =============================================================================
# Redshift Cluster
# =============================================================================

resource "aws_redshift_subnet_group" "this" {
  name       = "${var.name_prefix}-subnet-group"
  subnet_ids = var.subnet_ids

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
  iam_roles = [aws_iam_role.redshift.arn]

  # Skip final snapshot in dev/staging to ease destroy; override in prod if needed.
  skip_final_snapshot = true # true for dev/stg only
  publicly_accessible = false
  encrypted           = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-cluster"
  })
}
