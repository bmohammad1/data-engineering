data "aws_caller_identity" "current" {}

locals {
  # Use the last 8 digits of the account ID as a suffix so bucket names are
  # unique across sandbox accounts and never collide on re-creation.
  account_suffix = substr(data.aws_caller_identity.current.account_id, -8, -1)

  buckets = {
    raw           = "${var.name_prefix}-raw-${local.account_suffix}"
    cleaned       = "${var.name_prefix}-cleaned-${local.account_suffix}"
    validated     = "${var.name_prefix}-validated-${local.account_suffix}"
    bad           = "${var.name_prefix}-bad-${local.account_suffix}"
    scripts       = "${var.name_prefix}-scripts-${local.account_suffix}"
    orchestration = "${var.name_prefix}-orchestration-${local.account_suffix}"
    config        = "${var.name_prefix}-config-${local.account_suffix}"
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

resource "aws_s3_bucket" "validated" {
  bucket = local.buckets.validated
  tags   = merge(var.tags, { Name = local.buckets.validated })
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
    validated     = aws_s3_bucket.validated.id
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

  # Wait for all buckets to finish creating before applying sub-resources.
  # S3 DNS propagation can lag behind resource creation, causing "no such host" errors.
  depends_on = [
    aws_s3_bucket.raw, aws_s3_bucket.cleaned, aws_s3_bucket.validated,
    aws_s3_bucket.bad, aws_s3_bucket.scripts, aws_s3_bucket.orchestration,
    aws_s3_bucket.config,
  ]
}

# --- Block public access on all buckets ---

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw           = aws_s3_bucket.raw.id
    cleaned       = aws_s3_bucket.cleaned.id
    validated     = aws_s3_bucket.validated.id
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

  depends_on = [
    aws_s3_bucket.raw, aws_s3_bucket.cleaned, aws_s3_bucket.validated,
    aws_s3_bucket.bad, aws_s3_bucket.scripts, aws_s3_bucket.orchestration,
    aws_s3_bucket.config,
  ]
}

# --- source_config/ folder placeholder in config bucket ---

resource "aws_s3_object" "config_source_config_folder" {
  bucket  = aws_s3_bucket.config.id
  key     = "source_config/"
  content = ""

  depends_on = [aws_s3_bucket.config]
}

# --- Versioning on config bucket ---

resource "aws_s3_bucket_versioning" "config" {
  bucket = aws_s3_bucket.config.id

  versioning_configuration {
    status = "Enabled"
  }
}
