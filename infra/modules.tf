module "s3" {
  source       = "./s3"
  project_name = var.project_name
  environment  = var.environment
  bucket_name  = var.bucket_name
  common_tags  = local.common_tags
}

module "iam" {
  source       = "./iam"
  project_name = var.project_name
  environment  = var.environment
  bucket_name  = var.bucket_name
  common_tags  = local.common_tags
}

module "glue" {
  source               = "./glue"
  project_name         = var.project_name
  environment          = var.environment
  bucket_name          = var.bucket_name
  aws_region           = var.aws_region
  glue_database_bronze = var.glue_database_bronze
  glue_database_silver = var.glue_database_silver
  glue_database_gold   = var.glue_database_gold
  crawler_role_arn     = module.iam.glue_crawler_role_arn
  common_tags          = local.common_tags

  depends_on = [module.s3]
}

module "stepfunction" {
  source        = "./stepfunction"
  project_name  = var.project_name
  environment   = var.environment
  aws_region    = var.aws_region
  bucket_name   = var.bucket_name
  glue_role_arn = module.iam.glue_crawler_role_arn
  common_tags   = local.common_tags

  depends_on = [module.glue]
}
