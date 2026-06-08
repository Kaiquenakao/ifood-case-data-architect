# ─── S3 Bucket — Data Lake ──────────────────────────────────────────────────
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = var.common_tags
}

# ─── Bloqueia acesso público ─────────────────────────────────────────────────
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─── Versionamento ───────────────────────────────────────────────────────────
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ─── Estrutura de pastas ─────────────────────────────────────────────────────
resource "aws_s3_object" "bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "bronze/"
}

resource "aws_s3_object" "silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/"
}

resource "aws_s3_object" "gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "gold/"
}

resource "aws_s3_object" "athena_results" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "athena-results/"
}
