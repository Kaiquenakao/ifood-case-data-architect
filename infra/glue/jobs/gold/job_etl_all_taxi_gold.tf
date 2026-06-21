# ─── Glue Job — ETL All Taxi Gold ───────────────────────────────────────────
# Responsável pela consolidação dos dados de Yellow e Green Taxi na camada Gold.
# Lê as tabelas Silver do Glue Catalog, realiza UNION ALL, aplica seleção de
# colunas, tipagem explícita e filtros de qualidade (total_amount > 0,
# passenger_count 1-6, Jan-Mai 2023), e persiste em gold/table_all_taxi_gold/.
#
# Esta tabela é a base para as demais tabelas Gold derivadas:
#   - table_avg_total_amount_gold (job_etl_avg_total_amount_gold.tf)
#   - table_avg_passengers_gold   (job_etl_avg_passengers_gold.tf)
#
# Script: src/gold/etl_all_taxi_gold.py
# Origem:  ifood_case_silver.table_yellow_taxi_silver
#          ifood_case_silver.table_green_taxi_silver
# Destino S3: s3://{bucket}/gold/table_all_taxi_gold/
# Catalog: Modelado em catalog/gold/table_all_taxi_gold.tf
resource "aws_glue_job" "etl_all_taxi_gold" {
  name         = "${var.prefix}-etl-all-taxi-gold"
  role_arn     = var.crawler_role_arn
  description  = "ETL Gold — Consolidacao Yellow e Green Taxi com filtros de qualidade"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/gold/etl_all_taxi_gold.py"
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
