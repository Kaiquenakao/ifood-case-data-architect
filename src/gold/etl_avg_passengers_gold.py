"""Script responsável pelo cálculo e persistência da média de passageiros por hora
de todos os táxis em maio de 2023 na tabela table_avg_passengers_gold na camada Gold do Data Lake.

Este script deriva da tabela table_all_taxi_gold, que já contém os dados consolidados
e filtrados de Yellow e Green Taxi. A partir dela, filtra apenas corridas de maio,
extrai a hora do pickup, agrupa por hora e calcula a média de passageiros, persistindo
o resultado no S3 como uma tabela pré-agregada de alta performance para consumo analítico.

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
logger = logging.getLogger("etl_avg_passengers_gold")

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = None
TABLE_SOURCE = "table_all_taxi_gold"
TABLE_GOLD = "table_avg_passengers_gold"
MES_MAIO = 5


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
    """Filtra maio, extrai hora do pickup, agrupa por hora e calcula média de passageiros.

    Aplica as seguintes transformações:
        - Filtro por mês de pickup == maio (5).
        - Extração da hora do pickup como nova coluna 'hora'.
        - Agrupamento por hora do dia (0-23).
        - Cálculo da média de passenger_count arredondada em 2 casas decimais.
        - Ordenação crescente por hora.

    Args:
        df: DataFrame Spark com os dados consolidados lidos do extract.

    Returns:
        DataFrame Spark agregado com colunas hora e avg_passenger_count.
    """
    logger.info("Iniciando transform | tabela=%s", TABLE_GOLD)

    df = (
        df.filter(F.month(F.col("tpep_pickup_datetime")) == MES_MAIO)
        .withColumn("hora", F.hour(F.col("tpep_pickup_datetime")))
        .groupBy("hora")
        .agg(F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"))
        .orderBy("hora")
    )

    logger.info("Transform concluido | tabela=%s", TABLE_GOLD)
    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df):
    """Persiste o DataFrame agregado no S3 sem particionamento.

    Tabela de resultado analítico — sem particionamento por ser pequena
    e de leitura direta pelo usuário final.

    Args:
        df: DataFrame Spark com o resultado da agregação por hora.
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
