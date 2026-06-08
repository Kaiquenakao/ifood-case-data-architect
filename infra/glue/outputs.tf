output "database_bronze_name" {
  description = "Nome do database Bronze"
  value       = aws_glue_catalog_database.bronze.name
}

output "database_silver_name" {
  description = "Nome do database Silver"
  value       = aws_glue_catalog_database.silver.name
}

output "database_gold_name" {
  description = "Nome do database Gold"
  value       = aws_glue_catalog_database.gold.name
}

output "crawler_bronze_yellow_name" {
  description = "Nome do Crawler Bronze Yellow Taxi"
  value       = aws_glue_crawler.bronze_yellow_taxi.name
}

output "crawler_bronze_green_name" {
  description = "Nome do Crawler Bronze Green Taxi"
  value       = aws_glue_crawler.bronze_green_taxi.name
}

output "job_bronze_name" {
  description = "Nome do Glue Job ETL Bronze"
  value       = aws_glue_job.etl_bronze.name
}

output "job_silver_name" {
  description = "Nome do Glue Job ETL Silver"
  value       = aws_glue_job.etl_silver.name
}

output "job_gold_name" {
  description = "Nome do Glue Job ETL Gold"
  value       = aws_glue_job.etl_gold.name
}
