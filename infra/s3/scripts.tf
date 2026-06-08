# ─── Upload dos scripts ETL para o S3 ───────────────────────────────────────
resource "aws_s3_object" "script_bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/etl_bronze.py"
  source = "${path.root}/../src/etl_bronze.py"
  etag   = filemd5("${path.root}/../src/etl_bronze.py")
}

resource "aws_s3_object" "script_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/etl_silver.py"
  source = "${path.root}/../src/etl_silver.py"
  etag   = filemd5("${path.root}/../src/etl_silver.py")
}

resource "aws_s3_object" "script_gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/etl_gold.py"
  source = "${path.root}/../src/etl_gold.py"
  etag   = filemd5("${path.root}/../src/etl_gold.py")
}
