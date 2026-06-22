variable "aws_region" {
  description = "Regiao AWS onde os recursos serao provisionados"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake — deve ser unico globalmente na AWS"
  type        = string
}
