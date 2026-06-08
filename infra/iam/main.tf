# ─── Role do Glue ───────────────────────────────────────────────────────────
resource "aws_iam_role" "glue_crawler" {
  name        = "${local.prefix}-glue-crawler-role"
  description = "Role para os Glue Jobs e Crawlers acessarem o S3 e o Glue Catalog"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.common_tags
}

# ─── Policy — acesso ao S3 (leitura e escrita) ──────────────────────────────
resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.prefix}-glue-s3-policy"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*",
        ]
      }
    ]
  })
}

# ─── Policy — Glue Service ──────────────────────────────────────────────────
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
