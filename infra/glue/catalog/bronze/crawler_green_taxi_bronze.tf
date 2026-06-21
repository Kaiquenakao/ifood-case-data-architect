# ─── Crawler Bronze — Green Taxi ────────────────────────────────────────────
# Crawler responsável por registrar e atualizar o schema da tabela
# table_green_taxi_bronze no Glue Data Catalog após a execução do ETL Bronze.
#
# Tabela gerenciada: ifood_case_bronze.table_green_taxi_bronze
# Path S3: s3://{bucket}/bronze/table_green_taxi_bronze/
# Job que gera os dados: jobs/bronze/job_etl_green_taxi_bronze.tf
resource "aws_glue_crawler" "bronze_green_taxi" {
  name          = "${var.prefix}-crawler-bronze-green-taxi"
  role          = var.crawler_role_arn
  database_name = var.database_bronze
  description   = "Crawler da camada Bronze — Green Taxi NYC TLC"

  s3_target {
    path = "s3://${var.bucket_name}/bronze/table_green_taxi_bronze/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })

  tags = var.common_tags
}

resource "null_resource" "run_crawler_green" {
  depends_on = [aws_glue_crawler.bronze_green_taxi]

  provisioner "local-exec" {
    command = "aws glue start-crawler --name ${aws_glue_crawler.bronze_green_taxi.name} --region ${var.aws_region}"
  }
}
