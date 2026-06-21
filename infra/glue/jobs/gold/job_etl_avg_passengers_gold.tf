# ─── Glue Job — ETL Avg Passengers Gold ─────────────────────────────────────
# Responsável pelo cálculo da média de passageiros por hora em maio de 2023.
# Deriva da tabela table_all_taxi_gold (já consolidada e filtrada), filtra
# apenas corridas de maio, extrai a hora do pickup, agrupa por hora e calcula
# a média de passenger_count para todos os táxis (Yellow e Green).
#
# Script: src/gold/etl_avg_passengers_gold.py
# Origem:  s3://{bucket}/gold/table_all_taxi_gold/
# Destino S3: s3://{bucket}/gold/table_avg_passengers_gold/
# Catalog: Modelado em catalog/gold/table_avg_passengers_gold.tf
resource "aws_glue_job" "etl_avg_passengers_gold" {
  name         = "${var.prefix}-etl-avg-passengers-gold"
  role_arn     = var.crawler_role_arn
  description  = "ETL Gold — Media de passageiros por hora — Todos os Taxis Maio 2023"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/gold/etl_avg_passengers_gold.py"
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
