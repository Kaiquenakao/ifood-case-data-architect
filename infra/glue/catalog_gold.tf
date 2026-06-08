# ─── Catalog Table Gold — All Taxi ──────────────────────────────────────────
resource "aws_glue_catalog_table" "gold_all_taxi" {
  name          = "table_all_taxi_gold"
  database_name = aws_glue_catalog_database.gold.name
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
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name    = "VendorID"
      type    = "int"
      comment = "ID do fornecedor do taxi"
    }

    columns {
      name    = "passenger_count"
      type    = "int"
      comment = "Numero de passageiros na corrida — entre 1 e 6"
    }

    columns {
      name    = "total_amount"
      type    = "double"
      comment = "Valor total da corrida em USD — maior que zero"
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

# ─── Catalog Table Gold — Query 1 ───────────────────────────────────────────
resource "aws_glue_catalog_table" "gold_avg_total_amount" {
  name          = "table_avg_total_amount_gold"
  database_name = aws_glue_catalog_database.gold.name
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
      parameters = {
        "serialization.format" = "1"
      }
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

# ─── Catalog Table Gold — Query 2 ───────────────────────────────────────────
resource "aws_glue_catalog_table" "gold_avg_passengers" {
  name          = "table_avg_passengers_gold"
  database_name = aws_glue_catalog_database.gold.name
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
      parameters = {
        "serialization.format" = "1"
      }
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

# ─── Registra partições do table_all_taxi_gold ──────────────────────────────
resource "null_resource" "repair_gold_all_taxi" {
  depends_on = [aws_glue_catalog_table.gold_all_taxi]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = "aws athena start-query-execution --query-string \"MSCK REPAIR TABLE ifood_case_gold.table_all_taxi_gold\" --result-configuration OutputLocation=s3://${var.bucket_name}/athena-results/"
  }
}
