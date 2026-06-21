# ─── Modelagem — table_avg_passengers_gold ──────────────────────────────────
# Tabela Gold pré-agregada com a média de passageiros por hora em maio de 2023.
# Responde: "Qual a média de passageiros por hora — Todos os Taxis Maio 2023?"
#
# Sem particionamento — tabela pequena de resultado analítico.
# Job: jobs/gold/job_etl_avg_passengers_gold.tf
resource "aws_glue_catalog_table" "gold_avg_passengers" {
  name          = "table_avg_passengers_gold"
  database_name = var.database_gold
  description   = "Media de passenger_count por hora — Todos os Taxis Maio 2023"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/gold/table_avg_passengers_gold/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = { "serialization.format" = "1" }
    }

    columns {
      name    = "hora"
      type    = "int"
      comment = "Hora do dia 0-23"
    }

    columns {
      name    = "avg_passenger_count"
      type    = "double"
      comment = "Media de passageiros por hora em maio"
    }
  }
}
