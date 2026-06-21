variable "project_name" {
  description = "Nome do projeto"
  type        = string
}

variable "environment" {
  description = "Ambiente de execucao"
  type        = string
}

variable "aws_region" {
  description = "Regiao AWS"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "glue_role_arn" {
  description = "ARN da Role do Glue"
  type        = string
}

variable "common_tags" {
  description = "Tags comuns a todos os recursos"
  type        = map(string)
}
