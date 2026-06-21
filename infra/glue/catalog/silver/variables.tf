variable "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  type        = string
}

variable "database_silver" {
  description = "Nome do database Silver no Glue Catalog"
  type        = string
}
