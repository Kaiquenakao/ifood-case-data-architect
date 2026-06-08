output "bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  value       = aws_s3_bucket.data_lake.id
}

output "bucket_arn" {
  description = "ARN do bucket S3 do Data Lake"
  value       = aws_s3_bucket.data_lake.arn
}
