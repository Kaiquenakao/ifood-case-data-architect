variable "project_name" {
  description = "Nome do projeto"
  type        = string
}

variable "environment" {
  description = "Ambiente de execucao"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "aws_region" {
  description = "Regiao AWS"
  type        = string
}

variable "glue_database_bronze" {
  description = "Nome do database Bronze"
  type        = string
}

variable "glue_database_silver" {
  description = "Nome do database Silver"
  type        = string
}

variable "glue_database_gold" {
  description = "Nome do database Gold"
  type        = string
}

variable "crawler_role_arn" {
  description = "ARN da Role do Glue Crawler e Jobs"
  type        = string
}

variable "common_tags" {
  description = "Tags comuns a todos os recursos"
  type        = map(string)
}
