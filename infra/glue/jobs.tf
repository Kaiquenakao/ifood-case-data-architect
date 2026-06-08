# ─── IAM Role para os Glue Jobs ─────────────────────────────────────────────
# Reutiliza a role do Crawler adicionando permissões de escrita no S3

# ─── Glue Job — ETL Bronze ──────────────────────────────────────────────────
resource "aws_glue_job" "etl_bronze" {
  name         = "${local.prefix}-etl-bronze"
  role_arn     = var.crawler_role_arn
  description  = "ETL Bronze — Ingestão dos dados NYC TLC no Data Lake"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/etl_bronze.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.bucket_name}/temp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.common_tags
}

# ─── Glue Job — ETL Silver ──────────────────────────────────────────────────
resource "aws_glue_job" "etl_silver" {
  name         = "${local.prefix}-etl-silver"
  role_arn     = var.crawler_role_arn
  description  = "ETL Silver — Transformacao e padronizacao dos dados Bronze"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/etl_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.bucket_name}/temp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.common_tags
}

# ─── Glue Job — ETL Gold ────────────────────────────────────────────────────
resource "aws_glue_job" "etl_gold" {
  name         = "${local.prefix}-etl-gold"
  role_arn     = var.crawler_role_arn
  description  = "ETL Gold — Consolidacao e agregacao para camada de consumo"
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/etl_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.bucket_name}/temp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.common_tags
}
