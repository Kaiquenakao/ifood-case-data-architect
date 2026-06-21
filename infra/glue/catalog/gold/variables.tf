variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "database_gold" {
  description = "Nome do database Gold no Glue Catalog"
  type        = string
}
