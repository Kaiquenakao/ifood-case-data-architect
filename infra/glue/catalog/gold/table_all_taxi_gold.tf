# ─── Modelagem — table_all_taxi_gold ────────────────────────────────────────
# Tabela Gold consolidada com dados de Yellow e Green Taxi filtrados.
# Base para as tabelas de agregação:
#   - table_avg_total_amount_gold
#   - table_avg_passengers_gold
#
# Filtros de qualidade: total_amount > 0, passenger_count 1-6, Jan-Mai 2023
# Job: jobs/gold/job_etl_all_taxi_gold.tf
resource "aws_glue_catalog_table" "gold_all_taxi" {
  name          = "table_all_taxi_gold"
  database_name = var.database_gold
  description   = "Dados consolidados Yellow e Green Taxi — Jan a Mai 2023"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/gold/table_all_taxi_gold/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = { "serialization.format" = "1" }
    }

    columns {
      name    = "VendorID"
      type    = "int"
      comment = "ID do fornecedor do taxi"
    }

    columns {
      name    = "passenger_count"
      type    = "int"
      comment = "Numero de passageiros — entre 1 e 6"
    }

    columns {
      name    = "total_amount"
      type    = "double"
      comment = "Valor total em USD — maior que zero"
    }

    columns {
      name    = "tpep_pickup_datetime"
      type    = "timestamp"
      comment = "Data e hora de inicio da corrida"
    }

    columns {
      name    = "tpep_dropoff_datetime"
      type    = "timestamp"
      comment = "Data e hora de fim da corrida"
    }

    columns {
      name    = "taxi_type"
      type    = "string"
      comment = "Tipo do taxi — yellow ou green"
    }
  }

  partition_keys {
    name    = "partition_year"
    type    = "int"
    comment = "Ano da corrida"
  }

  partition_keys {
    name    = "partition_month"
    type    = "int"
    comment = "Mes da corrida"
  }
}
