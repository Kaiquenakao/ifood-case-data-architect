output "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  value       = module.s3.bucket_name
}

output "bucket_arn" {
  description = "ARN do bucket S3 do Data Lake"
  value       = module.s3.bucket_arn
}

output "glue_database_bronze" {
  description = "Nome do database Bronze no Glue Data Catalog"
  value       = module.glue.database_bronze_name
}

output "glue_database_silver" {
  description = "Nome do database Silver no Glue Data Catalog"
  value       = module.glue.database_silver_name
}

output "glue_database_gold" {
  description = "Nome do database Gold no Glue Data Catalog"
  value       = module.glue.database_gold_name
}

output "crawler_bronze_yellow_name" {
  description = "Nome do Crawler Bronze Yellow Taxi"
  value       = module.glue.crawler_bronze_yellow_name
}

output "crawler_bronze_green_name" {
  description = "Nome do Crawler Bronze Green Taxi"
  value       = module.glue.crawler_bronze_green_name
}

output "glue_crawler_role_arn" {
  description = "ARN da Role do Glue Crawler"
  value       = module.iam.glue_crawler_role_arn
}
