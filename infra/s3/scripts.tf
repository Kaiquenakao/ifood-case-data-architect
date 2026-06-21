# ─── Upload dos scripts ETL para o S3 ───────────────────────────────────────
# Os scripts são organizados por camada (bronze/silver/gold) espelhando
# a estrutura de src/ do repositório. O path no S3 é usado pelos Glue Jobs
# definidos em infra/glue/jobs/{camada}/job_etl_*.tf

# ─── Bronze ──────────────────────────────────────────────────────────────────
resource "aws_s3_object" "script_etl_yellow_taxi_bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/bronze/etl_yellow_taxi_bronze.py"
  source = "${path.root}/../src/bronze/etl_yellow_taxi_bronze.py"
  etag   = filemd5("${path.root}/../src/bronze/etl_yellow_taxi_bronze.py")
}

resource "aws_s3_object" "script_etl_green_taxi_bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/bronze/etl_green_taxi_bronze.py"
  source = "${path.root}/../src/bronze/etl_green_taxi_bronze.py"
  etag   = filemd5("${path.root}/../src/bronze/etl_green_taxi_bronze.py")
}

# ─── Silver ──────────────────────────────────────────────────────────────────
resource "aws_s3_object" "script_etl_yellow_taxi_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/silver/etl_yellow_taxi_silver.py"
  source = "${path.root}/../src/silver/etl_yellow_taxi_silver.py"
  etag   = filemd5("${path.root}/../src/silver/etl_yellow_taxi_silver.py")
}

resource "aws_s3_object" "script_etl_green_taxi_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/silver/etl_green_taxi_silver.py"
  source = "${path.root}/../src/silver/etl_green_taxi_silver.py"
  etag   = filemd5("${path.root}/../src/silver/etl_green_taxi_silver.py")
}

# ─── Gold ─────────────────────────────────────────────────────────────────────
resource "aws_s3_object" "script_etl_all_taxi_gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/gold/etl_all_taxi_gold.py"
  source = "${path.root}/../src/gold/etl_all_taxi_gold.py"
  etag   = filemd5("${path.root}/../src/gold/etl_all_taxi_gold.py")
}

resource "aws_s3_object" "script_etl_avg_total_amount_gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/gold/etl_avg_total_amount_gold.py"
  source = "${path.root}/../src/gold/etl_avg_total_amount_gold.py"
  etag   = filemd5("${path.root}/../src/gold/etl_avg_total_amount_gold.py")
}

resource "aws_s3_object" "script_etl_avg_passengers_gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/gold/etl_avg_passengers_gold.py"
  source = "${path.root}/../src/gold/etl_avg_passengers_gold.py"
  etag   = filemd5("${path.root}/../src/gold/etl_avg_passengers_gold.py")
}
