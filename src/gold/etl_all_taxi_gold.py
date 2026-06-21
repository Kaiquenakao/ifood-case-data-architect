"""Script responsável pela consolidação dos dados de Yellow e Green Taxi na camada Gold do Data Lake.

Este script lê as tabelas table_yellow_taxi_silver e table_green_taxi_silver do Glue
Data Catalog, realiza a união dos dois DataFrames via unionByName, aplica seleção e
padronização de colunas, tipagem explícita e filtros de qualidade, e persiste os dados
particionados no S3 na camada Gold.

Esta tabela serve como base consolidada para as demais tabelas Gold derivadas
(table_avg_total_amount_gold e table_avg_passengers_gold), centralizando os filtros
de qualidade em um único lugar e garantindo consistência entre as análises.

Uso:
    Executado como AWS Glue Job com os argumentos:
        --JOB_NAME    : Nome do job no Glue.
        --BUCKET_NAME : Nome do bucket S3 de destino.

Argumentos Glue:
    JOB_NAME    : Injetado automaticamente pelo Glue.
    BUCKET_NAME : Nome do bucket S3 onde os dados serão persistidos.
"""

import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger("etl_all_taxi_gold")

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = None
DATABASE_SILVER = "ifood_case_silver"
TABLE_YELLOW_SILVER = "table_yellow_taxi_silver"
TABLE_GREEN_SILVER = "table_green_taxi_silver"
TABLE_GOLD = "table_all_taxi_gold"
ANO = 2023
MES_INICIO = 1
MES_FIM = 5


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(glue_context, tabela: str):
    """Lê uma tabela da camada Silver do Glue Data Catalog e retorna um DataFrame Spark.

    Args:
        glue_context: Contexto do Glue inicializado no run().
        tabela      : Nome da tabela no Glue Data Catalog a ser lida.

    Returns:
        DataFrame Spark com os dados da tabela Silver.
    """
    logger.info("Iniciando extract | tabela=%s | database=%s", tabela, DATABASE_SILVER)

    df = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE_SILVER,
        table_name=tabela,
        transformation_ctx=tabela,
    ).toDF()

    logger.info("Extract concluido | tabela=%s", tabela)
    return df


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(df_yellow, df_green):
    """Une Yellow e Green Taxi, padroniza colunas e aplica filtros de qualidade.

    Aplica as seguintes transformações:
        - UNION ALL entre Yellow e Green Taxi via unionByName.
        - Seleção e renomeação de colunas para o schema Gold padronizado.
        - Cast explícito de tipos para garantir consistência.
        - Filtro de ano (2023) e meses (janeiro a maio).
        - Filtro de qualidade: total_amount > 0 e passenger_count entre 1 e 6.

    Args:
        df_yellow: DataFrame Spark com os dados do Yellow Taxi lidos da Silver.
        df_green : DataFrame Spark com os dados do Green Taxi lidos da Silver.

    Returns:
        DataFrame Spark consolidado, filtrado e pronto para carga na camada Gold.
    """
    logger.info("Iniciando transform | tabela=%s", TABLE_GOLD)

    df = df_yellow.unionByName(df_green)
    logger.info("UNION ALL concluido | tabela=%s", TABLE_GOLD)

    df = df.select(
        F.col("vendor_id").cast(IntegerType()).alias("VendorID"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("pickup_datetime").alias("tpep_pickup_datetime"),
        F.col("dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.col("taxi_type").cast(StringType()).alias("taxi_type"),
        F.col("partition_year").cast(IntegerType()).alias("partition_year"),
        F.col("partition_month").cast(IntegerType()).alias("partition_month"),
    )
    logger.info("Colunas renomeadas | vendor_id->VendorID | pickup/dropoff_datetime->tpep_*")

    df = (
        df.filter(F.year(F.col("tpep_pickup_datetime")) == ANO)
        .filter(F.month(F.col("tpep_pickup_datetime")).between(MES_INICIO, MES_FIM))
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("passenger_count") <= 6)
    )
    logger.info(
        "Filtros aplicados | ano=%s | meses=%s a %s | total_amount>0 | passenger_count 1-6",
        ANO, MES_INICIO, MES_FIM,
    )

    total = df.count()
    logger.info("Transform concluido | tabela=%s | registros=%s", TABLE_GOLD, f"{total:,}")
    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df, spark):
    """Persiste o DataFrame consolidado no S3 particionado por ano e mês.

    Args:
        df: DataFrame Spark transformado pelo transform.
        spark: Sessão Spark.
    """
    s3_path = f"s3://{BUCKET_NAME}/gold/{TABLE_GOLD}/"
    logger.info("Iniciando load | destino=%s", s3_path)

    df.coalesce(1).write.mode("overwrite").partitionBy(
        "partition_year", "partition_month"
    ).parquet(s3_path)

    spark.sql(f"MSCK REPAIR TABLE ifood_case_gold.{TABLE_GOLD}")
    logger.info("Partições registradas | tabela=%s", TABLE_GOLD)

    logger.info("Load concluido | destino=%s", s3_path)


# ─── Pipeline ───────────────────────────────────────────────────────────────
def run():
    """Executa o pipeline completo: extract → transform → load."""
    global BUCKET_NAME

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    BUCKET_NAME = args["BUCKET_NAME"]

    logger.info("Iniciando pipeline | tabela=%s", TABLE_GOLD)
    try:
        df_yellow = extract(glueContext, TABLE_YELLOW_SILVER)
        df_green = extract(glueContext, TABLE_GREEN_SILVER)
        df = transform(df_yellow, df_green)
        load(df, spark)
        logger.info("Pipeline finalizado com sucesso | tabela=%s", TABLE_GOLD)
    except (ValueError, OSError) as e:
        logger.error("Falha no pipeline | tabela=%s | erro=%s", TABLE_GOLD, e, exc_info=True)
        raise

    job.commit()


if __name__ == "__main__":
    run()
