# ─── Glue Job — ETL Yellow Taxi Bronze ──────────────────────────────────────
# Responsável pela ingestão dos dados brutos do Yellow Taxi na camada Bronze.
# Realiza download dos arquivos Parquet mensais da CDN do NYC TLC (Jan-Mai 2023),
# aplica padronização de colunas e tipagem explícita via schema, e persiste os
# dados particionados no S3 em bronze/table_yellow_taxi_bronze/.
#
# Script: src/bronze/etl_yellow_taxi_bronze.py
# Destino S3: s3://{bucket}/bronze/table_yellow_taxi_bronze/
# Catalog: Registrado via crawler catalog/bronze/crawler_yellow_taxi_bronze.tf
resource "aws_glue_job" "etl_yellow_taxi_bronze" {
  name         = "${var.prefix}-etl-yellow-taxi-bronze"
  role_arn     = var.crawler_role_arn
  description  = "ETL Bronze — Ingestão dos dados brutos Yellow Taxi NYC TLC Jan-Mai 2023"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/bronze/etl_yellow_taxi_bronze.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.bucket_name}/temp/"
    "--BUCKET_NAME"                      = var.bucket_name
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.common_tags
}
