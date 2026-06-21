# ─── Policy — Athena (MSCK REPAIR TABLE) ────────────────────────────────────
# Permite que os Glue Jobs executem queries no Athena para registrar
# partições via MSCK REPAIR TABLE após a escrita dos dados no S3.
resource "aws_iam_role_policy" "glue_athena" {
  name = "${local.prefix}-glue-athena-policy"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
        ]
        Resource = "*"
      }
    ]
  })
}
