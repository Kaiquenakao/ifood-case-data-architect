locals {
  # ─── Valores fixos do projeto — não alterar ──────────────────────────────
  project_name = "ifood-case"
  environment  = "prod"

  glue_database_bronze = "ifood_case_bronze"
  glue_database_silver = "ifood_case_silver"
  glue_database_gold   = "ifood_case_gold"

  prefix = "${local.project_name}-${local.environment}"

  common_tags = {
    Project     = local.project_name
    Environment = local.environment
    ManagedBy   = "terraform"
  }
}
