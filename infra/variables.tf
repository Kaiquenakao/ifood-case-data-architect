variable "aws_region" {
  description = "Regiao AWS onde os recursos serao provisionados"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto — usado como prefixo nos recursos"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "glue_database_bronze" {
  description = "Nome do database Bronze no Glue Data Catalog"
  type        = string
}

variable "glue_database_silver" {
  description = "Nome do database Silver no Glue Data Catalog"
  type        = string
}

variable "glue_database_gold" {
  description = "Nome do database Gold no Glue Data Catalog"
  type        = string
}

variable "environment" {
  description = "Ambiente de execucao"
  type        = string
}
