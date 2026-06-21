# ─── Glue Job — ETL Avg Total Amount Gold ───────────────────────────────────
# Responsável pelo cálculo da média do valor total por mês do Yellow Taxi.
# Deriva da tabela table_all_taxi_gold (já consolidada e filtrada), filtra
# apenas corridas Yellow, agrupa por mês e calcula a média de total_amount,
# persistindo o resultado em gold/table_avg_total_amount_gold/.
#
# Script: src/gold/etl_avg_total_amount_gold.py
# Origem:  s3://{bucket}/gold/table_all_taxi_gold/
# Destino S3: s3://{bucket}/gold/table_avg_total_amount_gold/
# Catalog: Modelado em catalog/gold/table_avg_total_amount_gold.tf
resource "aws_glue_job" "etl_avg_total_amount_gold" {
  name         = "${var.prefix}-etl-avg-total-amount-gold"
  role_arn     = var.crawler_role_arn
  description  = "ETL Gold — Media do valor total por mes — Yellow Taxi Jan-Mai 2023"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/gold/etl_avg_total_amount_gold.py"
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
