# ─── Glue Database Bronze ───────────────────────────────────────────────────
resource "aws_glue_catalog_database" "bronze" {
  name        = var.glue_database_bronze
  description = "Camada Bronze — Dados brutos NYC TLC"

  tags = var.common_tags
}

# ─── Glue Database Silver ───────────────────────────────────────────────────
resource "aws_glue_catalog_database" "silver" {
  name        = var.glue_database_silver
  description = "Camada Silver — Dados transformados e padronizados"

  tags = var.common_tags
}

# ─── Glue Database Gold ─────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_gold
  description = "Camada Gold — Dados agregados para consumo"

  tags = var.common_tags
}
