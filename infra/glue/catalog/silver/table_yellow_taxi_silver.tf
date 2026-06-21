# ─── Modelagem — table_yellow_taxi_silver ───────────────────────────────────
# Tabela da camada Silver com os dados transformados e padronizados do Yellow Taxi.
#
# Schema aplicado pelo ETL Silver (src/silver/etl_yellow_taxi_silver.py):
#   - vendor_id       : int       — ID do fornecedor (renomeado de vendorid)
#   - passenger_count : int       — Numero de passageiros (nulos removidos)
#   - total_amount    : double    — Valor total da corrida em USD (nulos removidos)
#   - pickup_datetime : timestamp — Inicio da corrida
#   - dropoff_datetime: timestamp — Fim da corrida
#   - taxi_type       : string    — Literal 'yellow'
#
# Particionamento: partition_year / partition_month
# Job: jobs/silver/job_etl_yellow_taxi_silver.tf
resource "aws_glue_catalog_table" "silver_yellow_taxi" {
  name          = "table_yellow_taxi_silver"
  database_name = var.database_silver
  description   = "Dados transformados Yellow Taxi — Jan a Mai 2023"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/silver/table_yellow_taxi_silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = { "serialization.format" = "1" }
    }

    columns {
      name    = "vendor_id"
      type    = "int"
      comment = "ID do fornecedor do taxi"
    }

    columns {
      name    = "passenger_count"
      type    = "int"
      comment = "Numero de passageiros na corrida"
    }

    columns {
      name    = "total_amount"
      type    = "double"
      comment = "Valor total da corrida em USD"
    }

    columns {
      name    = "pickup_datetime"
      type    = "timestamp"
      comment = "Data e hora de inicio da corrida"
    }

    columns {
      name    = "dropoff_datetime"
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
