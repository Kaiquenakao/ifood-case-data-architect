# ─── Glue Job — ETL Yellow Taxi Silver ──────────────────────────────────────
# Responsável pela transformação dos dados do Yellow Taxi da camada Bronze para Silver.
# Lê a tabela table_yellow_taxi_bronze do Glue Catalog, aplica seleção e
# padronização de colunas, tipagem explícita e remoção de nulos nas colunas
# obrigatórias, e persiste os dados particionados em silver/table_yellow_taxi_silver/.
#
# Script: src/silver/etl_yellow_taxi_silver.py
# Origem:  ifood_case_bronze.table_yellow_taxi_bronze
# Destino S3: s3://{bucket}/silver/table_yellow_taxi_silver/
# Catalog: Modelado em catalog/silver/table_yellow_taxi_silver.tf
resource "aws_glue_job" "etl_yellow_taxi_silver" {
  name         = "${var.prefix}-etl-yellow-taxi-silver"
  role_arn     = var.crawler_role_arn
  description  = "ETL Silver — Transformacao e padronizacao dos dados Yellow Taxi"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/silver/etl_yellow_taxi_silver.py"
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
