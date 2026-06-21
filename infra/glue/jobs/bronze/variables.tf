variable "prefix" {
  description = "Prefixo dos recursos — {project_name}-{environment}"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "crawler_role_arn" {
  description = "ARN da Role do Glue"
  type        = string
}

variable "common_tags" {
  description = "Tags comuns a todos os recursos"
  type        = map(string)
}
