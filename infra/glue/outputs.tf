# ─── Databases ──────────────────────────────────────────────────────────────
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
