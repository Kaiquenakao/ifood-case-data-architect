# ─── Módulos de Jobs ─────────────────────────────────────────────────────────
module "jobs_bronze" {
  source           = "./jobs/bronze"
  prefix           = local.prefix
  bucket_name      = var.bucket_name
  crawler_role_arn = var.crawler_role_arn
  common_tags      = var.common_tags
}

module "jobs_silver" {
  source           = "./jobs/silver"
  prefix           = local.prefix
  bucket_name      = var.bucket_name
  crawler_role_arn = var.crawler_role_arn
  common_tags      = var.common_tags
}

module "jobs_gold" {
  source           = "./jobs/gold"
  prefix           = local.prefix
  bucket_name      = var.bucket_name
  crawler_role_arn = var.crawler_role_arn
  common_tags      = var.common_tags
}

# ─── Módulos de Catalog ───────────────────────────────────────────────────────
module "catalog_bronze" {
  source           = "./catalog/bronze"
  prefix           = local.prefix
  bucket_name      = var.bucket_name
  crawler_role_arn = var.crawler_role_arn
  database_bronze  = aws_glue_catalog_database.bronze.name
  aws_region       = var.aws_region
  common_tags      = var.common_tags
}

module "catalog_silver" {
  source          = "./catalog/silver"
  bucket_name     = var.bucket_name
  database_silver = aws_glue_catalog_database.silver.name
}

module "catalog_gold" {
  source        = "./catalog/gold"
  bucket_name   = var.bucket_name
  database_gold = aws_glue_catalog_database.gold.name
}
