# Glacier Deep Archive after 30 days for raw and cleaned buckets.
# Orchestration bucket: expire map state JSON files after 30 days — generated every 6 hours, never read after run completes.

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-raw-to-glacier"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "DEEP_ARCHIVE"
    }
    filter {} 
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cleaned" {
  bucket = aws_s3_bucket.cleaned.id

  rule {
    id     = "archive-cleaned-to-glacier"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "orchestration" {
  bucket = aws_s3_bucket.orchestration.id

  rule {
    id     = "expire-orchestration-files"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }
  }
}
