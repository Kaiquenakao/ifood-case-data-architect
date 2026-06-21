# ─── Step Function — Pipeline NYC TLC ───────────────────────────────────────
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.prefix}-pipeline-nyc-tlc"
  role_arn = aws_iam_role.stepfunction.arn

  definition = jsonencode({
    Comment = "Pipeline de ingestão e transformação dos dados NYC TLC — Bronze → Silver → Gold"
    StartAt = "ETL Bronze"

    States = {

      # ─── Bronze — paralelo ────────────────────────────────────────────────
      "ETL Bronze" = {
        Type    = "Parallel"
        Comment = "Ingestão paralela dos dados brutos Yellow e Green Taxi"
        Branches = [
          {
            StartAt = "ETL Yellow Taxi Bronze"
            States = {
              "ETL Yellow Taxi Bronze" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-yellow-taxi-bronze"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          },
          {
            StartAt = "ETL Green Taxi Bronze"
            States = {
              "ETL Green Taxi Bronze" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-green-taxi-bronze"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          }
        ]
        Next = "Crawler Bronze"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Falha Bronze", ResultPath = "$.error" }]
      }

      "Falha Bronze" = {
        Type  = "Fail"
        Error = "BronzeJobFailed"
        Cause = "Um ou mais jobs da camada Bronze falharam após todas as tentativas de retry"
      }

      # ─── Crawler Bronze — paralelo ────────────────────────────────────────
      "Crawler Bronze" = {
        Type    = "Parallel"
        Comment = "Executa crawlers para registrar partições Bronze no Glue Catalog"
        Branches = [
          {
            StartAt = "Crawler Yellow Taxi Bronze"
            States = {
              "Crawler Yellow Taxi Bronze" = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = "${local.prefix}-crawler-bronze-yellow-taxi" }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 15, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          },
          {
            StartAt = "Crawler Green Taxi Bronze"
            States = {
              "Crawler Green Taxi Bronze" = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = "${local.prefix}-crawler-bronze-green-taxi" }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 15, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          }
        ]
        Next  = "ETL Silver"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Falha Crawler Bronze", ResultPath = "$.error" }]
      }

      "Falha Crawler Bronze" = {
        Type  = "Fail"
        Error = "CrawlerBronzeFailed"
        Cause = "Um ou mais crawlers Bronze falharam após todas as tentativas de retry"
      }

      # ─── Silver — paralelo ────────────────────────────────────────────────
      "ETL Silver" = {
        Type    = "Parallel"
        Comment = "Transformação paralela dos dados Yellow e Green Taxi para camada Silver"
        Branches = [
          {
            StartAt = "ETL Yellow Taxi Silver"
            States = {
              "ETL Yellow Taxi Silver" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-yellow-taxi-silver"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          },
          {
            StartAt = "ETL Green Taxi Silver"
            States = {
              "ETL Green Taxi Silver" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-green-taxi-silver"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          }
        ]
        Next  = "ETL All Taxi Gold"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Falha Silver", ResultPath = "$.error" }]
      }

      "Falha Silver" = {
        Type  = "Fail"
        Error = "SilverJobFailed"
        Cause = "Um ou mais jobs da camada Silver falharam após todas as tentativas de retry"
      }

      # ─── Gold All Taxi ────────────────────────────────────────────────────
      "ETL All Taxi Gold" = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Comment  = "Consolida Yellow e Green Taxi com filtros de qualidade na Gold"
        Parameters = {
          JobName = "${local.prefix}-etl-all-taxi-gold"
          Arguments = { "--BUCKET_NAME" = var.bucket_name }
        }
        Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Falha Gold", ResultPath = "$.error" }]
        Next  = "ETL Gold Agregacoes"
      }

      # ─── Gold Agregações — paralelo ───────────────────────────────────────
      "ETL Gold Agregacoes" = {
        Type    = "Parallel"
        Comment = "Calcula agregações Gold em paralelo a partir da table_all_taxi_gold"
        Branches = [
          {
            StartAt = "ETL Avg Total Amount Gold"
            States = {
              "ETL Avg Total Amount Gold" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-avg-total-amount-gold"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          },
          {
            StartAt = "ETL Avg Passengers Gold"
            States = {
              "ETL Avg Passengers Gold" = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "${local.prefix}-etl-avg-passengers-gold"
                  Arguments = { "--BUCKET_NAME" = var.bucket_name }
                }
                Retry = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 30, MaxAttempts = 3, BackoffRate = 2.0 }]
                End = true
              }
            }
          }
        ]
        Next  = "Pipeline Concluido"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Falha Gold", ResultPath = "$.error" }]
      }

      "Falha Gold" = {
        Type  = "Fail"
        Error = "GoldJobFailed"
        Cause = "Um ou mais jobs da camada Gold falharam após todas as tentativas de retry"
      }

      # ─── Sucesso ──────────────────────────────────────────────────────────
      "Pipeline Concluido" = {
        Type    = "Succeed"
        Comment = "Pipeline NYC TLC finalizado com sucesso — Bronze → Silver → Gold"
      }
    }
  })

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.stepfunction.arn}:*"
  }

  tags = var.common_tags
}

# ─── CloudWatch Log Group ─────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "stepfunction" {
  name              = "/aws/states/${local.prefix}-pipeline-nyc-tlc"
  retention_in_days = 30
  tags              = var.common_tags
}
