"""Script responsável pelo cálculo e persistência da média do valor total por mês
do Yellow Taxi na tabela table_avg_total_amount_gold na camada Gold do Data Lake.

Este script deriva da tabela table_all_taxi_gold, que já contém os dados consolidados
e filtrados de Yellow e Green Taxi. A partir dela, filtra apenas corridas do Yellow Taxi,
agrupa por mês e calcula a média do valor total das corridas, persistindo o resultado
no S3 como uma tabela pré-agregada de alta performance para consumo analítico.

A derivação a partir de table_all_taxi_gold garante consistência com os filtros de
qualidade já aplicados (total_amount > 0, passenger_count 1-6, jan a mai 2023).

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

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger("etl_avg_total_amount_gold")

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = None
TABLE_SOURCE = "table_all_taxi_gold"
TABLE_GOLD = "table_avg_total_amount_gold"


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(spark):
    """Lê a tabela table_all_taxi_gold do S3 e retorna um DataFrame Spark.

    Args:
        spark: SparkSession inicializada no run().

    Returns:
        DataFrame Spark com os dados consolidados e filtrados da camada Gold.
    """
    s3_path = f"s3://{BUCKET_NAME}/gold/{TABLE_SOURCE}/"
    logger.info("Iniciando extract | origem=%s", s3_path)

    df = spark.read.parquet(s3_path)

    logger.info("Extract concluido | origem=%s", s3_path)
    return df


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(df):
    """Filtra Yellow Taxi, agrupa por mês e calcula a média do valor total das corridas.

    Aplica as seguintes transformações:
        - Filtro por taxi_type == 'yellow'.
        - Agrupamento por mês de pickup.
        - Cálculo da média de total_amount arredondada em 2 casas decimais.
        - Ordenação crescente por mês.

    Args:
        df: DataFrame Spark com os dados consolidados lidos do extract.

    Returns:
        DataFrame Spark agregado com colunas mes e avg_total_amount.
    """
    logger.info("Iniciando transform | tabela=%s", TABLE_GOLD)

    df = (
        df.filter(F.col("taxi_type") == "yellow")
        .groupBy(F.month(F.col("tpep_pickup_datetime")).alias("mes"))
        .agg(F.round(F.avg("total_amount"), 2).alias("avg_total_amount"))
        .orderBy("mes")
    )

    logger.info("Transform concluido | tabela=%s", TABLE_GOLD)
    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df):
    """Persiste o DataFrame agregado no S3 sem particionamento.

    Tabela de resultado analítico — sem particionamento por ser pequena
    e de leitura direta pelo usuário final.

    Args:
        df: DataFrame Spark com o resultado da agregação por mês.
    """
    s3_path = f"s3://{BUCKET_NAME}/gold/{TABLE_GOLD}/"
    logger.info("Iniciando load | destino=%s", s3_path)

    df.coalesce(1).write.mode("overwrite").parquet(s3_path)

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
        df = extract(spark)
        df = transform(df)
        load(df)
        logger.info("Pipeline finalizado com sucesso | tabela=%s", TABLE_GOLD)
    except (ValueError, OSError) as e:
        logger.error("Falha no pipeline | tabela=%s | erro=%s", TABLE_GOLD, e, exc_info=True)
        raise

    job.commit()


if __name__ == "__main__":
    run()
