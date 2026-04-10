locals {
  buckets = {
    raw           = "${var.name_prefix}-raw"
    cleaned       = "${var.name_prefix}-cleaned"
    parquet       = "${var.name_prefix}-parquet"
    bad           = "${var.name_prefix}-bad"
    scripts       = "${var.name_prefix}-scripts"
    orchestration = "${var.name_prefix}-orchestration"
    config        = "${var.name_prefix}-config"
  }
}

resource "aws_s3_bucket" "raw" {
  bucket = local.buckets.raw
  tags   = merge(var.tags, { Name = local.buckets.raw })
}

resource "aws_s3_bucket" "cleaned" {
  bucket = local.buckets.cleaned
  tags   = merge(var.tags, { Name = local.buckets.cleaned })
}

resource "aws_s3_bucket" "parquet" {
  bucket = local.buckets.parquet
  tags   = merge(var.tags, { Name = local.buckets.parquet })
}

resource "aws_s3_bucket" "bad" {
  bucket = local.buckets.bad
  tags   = merge(var.tags, { Name = local.buckets.bad })
}

resource "aws_s3_bucket" "scripts" {
  bucket = local.buckets.scripts
  tags   = merge(var.tags, { Name = local.buckets.scripts })
}

resource "aws_s3_bucket" "orchestration" {
  bucket = local.buckets.orchestration
  tags   = merge(var.tags, { Name = local.buckets.orchestration })
}

resource "aws_s3_bucket" "config" {
  bucket = local.buckets.config
  tags   = merge(var.tags, { Name = local.buckets.config })
}

# --- Server-side encryption (AES256) for all buckets ---

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = {
    raw           = aws_s3_bucket.raw.id
    cleaned       = aws_s3_bucket.cleaned.id
    parquet       = aws_s3_bucket.parquet.id
    bad           = aws_s3_bucket.bad.id
    scripts       = aws_s3_bucket.scripts.id
    orchestration = aws_s3_bucket.orchestration.id
    config        = aws_s3_bucket.config.id
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# --- Block public access on all buckets ---

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw           = aws_s3_bucket.raw.id
    cleaned       = aws_s3_bucket.cleaned.id
    parquet       = aws_s3_bucket.parquet.id
    bad           = aws_s3_bucket.bad.id
    scripts       = aws_s3_bucket.scripts.id
    orchestration = aws_s3_bucket.orchestration.id
    config        = aws_s3_bucket.config.id
  }

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Versioning on config bucket ---

resource "aws_s3_bucket_versioning" "config" {
  bucket = aws_s3_bucket.config.id

  versioning_configuration {
    status = "Enabled"
  }
}
