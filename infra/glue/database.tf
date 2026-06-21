# ─── Glue Database Bronze ───────────────────────────────────────────────────
# Armazena os metadados das tabelas de dados brutos NYC TLC.
# Populado automaticamente pelos crawlers após execução do ETL Bronze.
resource "aws_glue_catalog_database" "bronze" {
  name        = var.glue_database_bronze
  description = "Camada Bronze — Dados brutos NYC TLC"

  tags = var.common_tags
}

# ─── Glue Database Silver ───────────────────────────────────────────────────
# Armazena os metadados das tabelas de dados transformados e padronizados.
# Populado via catalog/silver após execução do ETL Silver.
resource "aws_glue_catalog_database" "silver" {
  name        = var.glue_database_silver
  description = "Camada Silver — Dados transformados e padronizados"

  tags = var.common_tags
}

# ─── Glue Database Gold ─────────────────────────────────────────────────────
# Armazena os metadados das tabelas agregadas e prontas para consumo analítico.
# Populado via catalog/gold após execução do ETL Gold.
resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_gold
  description = "Camada Gold — Dados agregados para consumo"

  tags = var.common_tags
}
