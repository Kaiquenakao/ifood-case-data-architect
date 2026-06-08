output "glue_crawler_role_arn" {
  description = "ARN da Role do Glue Crawler"
  value       = aws_iam_role.glue_crawler.arn
}
