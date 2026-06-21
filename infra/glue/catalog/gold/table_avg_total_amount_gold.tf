# ─── Modelagem — table_avg_total_amount_gold ────────────────────────────────
# Tabela Gold pré-agregada com a média do valor total por mês do Yellow Taxi.
# Responde: "Qual a média de total_amount por mês — Yellow Taxi Jan-Mai 2023?"
#
# Sem particionamento — tabela pequena de resultado analítico.
# Job: jobs/gold/job_etl_avg_total_amount_gold.tf
resource "aws_glue_catalog_table" "gold_avg_total_amount" {
  name          = "table_avg_total_amount_gold"
  database_name = var.database_gold
  description   = "Media de total_amount por mes — Yellow Taxi Jan a Mai 2023"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/gold/table_avg_total_amount_gold/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = { "serialization.format" = "1" }
    }

    columns {
      name    = "mes"
      type    = "int"
      comment = "Mes da corrida"
    }

    columns {
      name    = "avg_total_amount"
      type    = "double"
      comment = "Media do valor total das corridas por mes"
    }
  }
}
