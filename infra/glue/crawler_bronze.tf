# ─── Crawler Bronze — Yellow Taxi ───────────────────────────────────────────
resource "aws_glue_crawler" "bronze_yellow_taxi" {
  name          = "${local.prefix}-crawler-bronze-yellow-taxi"
  role          = var.crawler_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawler da camada Bronze — Yellow Taxi"

  s3_target {
    path = "s3://${var.bucket_name}/bronze/table_yellow_taxi_bronze/"
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

# ─── Crawler Bronze — Green Taxi ────────────────────────────────────────────
resource "aws_glue_crawler" "bronze_green_taxi" {
  name          = "${local.prefix}-crawler-bronze-green-taxi"
  role          = var.crawler_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawler da camada Bronze — Green Taxi"

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

# ─── Execução automática dos Crawlers ───────────────────────────────────────
resource "null_resource" "run_crawler_yellow" {
  depends_on = [aws_glue_crawler.bronze_yellow_taxi]

  provisioner "local-exec" {
    command = "aws glue start-crawler --name ${aws_glue_crawler.bronze_yellow_taxi.name} --region ${var.aws_region}"
  }
}

resource "null_resource" "run_crawler_green" {
  depends_on = [aws_glue_crawler.bronze_green_taxi]

  provisioner "local-exec" {
    command = "aws glue start-crawler --name ${aws_glue_crawler.bronze_green_taxi.name} --region ${var.aws_region}"
  }
}
