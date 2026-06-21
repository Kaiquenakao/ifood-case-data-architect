"""Script responsável pela transformação e persistência dos dados do Yellow Taxi na camada Silver do Data Lake.

Este script lê os dados brutos da tabela table_yellow_taxi_bronze no Glue Data Catalog,
aplica seleção e padronização de colunas, tipagem explícita, remoção de nulos nas colunas
obrigatórias e persiste os dados particionados no S3 na camada Silver.

Os dados são extraídos via Glue Data Catalog (camada Bronze), transformados com PySpark
para garantir consistência de schema, e carregados no S3 com particionamento por ano e mês.

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

import boto3

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
logger = logging.getLogger("etl_yellow_taxi_silver")

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = None
DATABASE_BRONZE = "ifood_case_bronze"
TABLE_BRONZE = "table_yellow_taxi_bronze"
TABLE_SILVER = "table_yellow_taxi_silver"
PICKUP_COL = "tpep_pickup_datetime"
DROPOFF_COL = "tpep_dropoff_datetime"
TAXI_TYPE = "yellow"

COLUNAS_OBRIGATORIAS = [
    "vendor_id",
    "passenger_count",
    "total_amount",
    "pickup_datetime",
    "dropoff_datetime",
]


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(glue_context):
    """Lê a tabela table_yellow_taxi_bronze do Glue Data Catalog e retorna um DataFrame Spark.

    Args:
        glue_context: Contexto do Glue inicializado no run().

    Returns:
        DataFrame Spark com os dados brutos do Yellow Taxi.
    """
    logger.info(
        "Iniciando extract | tabela=%s | database=%s", TABLE_BRONZE, DATABASE_BRONZE
    )

    df = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE_BRONZE,
        table_name=TABLE_BRONZE,
        transformation_ctx=TABLE_BRONZE,
    ).toDF()

    logger.info("Extract concluido | tabela=%s", TABLE_BRONZE)
    return df


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(df):
    """Seleciona, renomeia, tipifica e remove nulos do DataFrame do Yellow Taxi.

    Aplica as seguintes transformações:
        - Seleção e renomeação das colunas para o schema Silver padronizado.
        - Cast explícito de tipos para garantir consistência.
        - Adição da coluna taxi_type com valor literal 'yellow'.
        - Remoção de registros com nulos nas colunas obrigatórias.

    Args:
        df: DataFrame Spark com os dados brutos lidos do Glue Catalog (Bronze).

    Returns:
        DataFrame Spark transformado e pronto para carga na camada Silver.
    """
    logger.info("Iniciando transform | tabela=%s", TABLE_SILVER)

    df = df.select(
        F.col("vendorid").cast(IntegerType()).alias("vendor_id"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col(PICKUP_COL).cast("timestamp").alias("pickup_datetime"),
        F.col(DROPOFF_COL).cast("timestamp").alias("dropoff_datetime"),
        F.lit(TAXI_TYPE).cast(StringType()).alias("taxi_type"),
        F.col("partition_year").cast(IntegerType()).alias("partition_year"),
        F.col("partition_month").cast(IntegerType()).alias("partition_month"),
    )
    logger.info("Colunas selecionadas, renomeadas e tipadas | tabela=%s", TABLE_SILVER)

    df = df.dropna(subset=COLUNAS_OBRIGATORIAS)
    logger.info("Nulos removidos | tabela=%s", TABLE_SILVER)

    total = df.count()
    logger.info(
        "Transform concluido | tabela=%s | registros=%s", TABLE_SILVER, f"{total:,}"
    )

    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df):
    """Persiste o DataFrame transformado no S3 particionado por ano e mês.

    Args:
        df: DataFrame Spark transformado pelo transform.
    """
    s3_path = f"s3://{BUCKET_NAME}/silver/{TABLE_SILVER}/"
    logger.info("Iniciando load | destino=%s", s3_path)

    df.coalesce(1).write.mode("overwrite").partitionBy(
        "partition_year", "partition_month"
    ).parquet(s3_path)

    logger.info("Load concluido | destino=%s", s3_path)

    # ─── Registra partições no Glue Catalog via Athena ──────────────────────
    athena = boto3.client("athena")
    athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE ifood_case_silver.{TABLE_SILVER}",
        ResultConfiguration={
            "OutputLocation": f"s3://{BUCKET_NAME}/athena-results/"
        }
    )
    logger.info("MSCK REPAIR TABLE executado | tabela=%s", TABLE_SILVER)


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

    logger.info("Iniciando pipeline | tabela=%s", TABLE_SILVER)
    try:
        df = extract(glueContext)
        df = transform(df)
        load(df)
        logger.info("Pipeline finalizado com sucesso | tabela=%s", TABLE_SILVER)
    except (ValueError, OSError) as e:
        logger.error(
            "Falha no pipeline | tabela=%s | erro=%s", TABLE_SILVER, e, exc_info=True
        )
        raise

    job.commit()


if __name__ == "__main__":
    run()
