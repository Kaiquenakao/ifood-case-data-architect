import sys
import logging
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger("etl_gold")

# ─── Inicializa contexto Glue e Spark ───────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = "ifood-case-data-lake-kaique"
DATABASE_SILVER = "ifood_case_silver"
ANO = 2023
MES_INICIO = 1
MES_FIM = 5
MES_MAIO = 5


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(tabela: str):
    logger.info("Iniciando extract | tabela=%s | database=%s", tabela, DATABASE_SILVER)

    df = glueContext.create_dynamic_frame.from_catalog(
        database=DATABASE_SILVER,
        table_name=tabela,
        transformation_ctx=tabela,
    ).toDF()

    logger.info("Extract concluido | tabela=%s", tabela)

    return df


# ─── Transform — All Taxi ───────────────────────────────────────────────────
def transform_all_taxi(df_yellow, df_green):
    logger.info("Iniciando transform | tabela=table_all_taxi_gold")

    # UNION ALL Yellow + Green
    df_union = df_yellow.unionByName(df_green)
    logger.info("UNION ALL concluido")

    # Renomeia colunas para atender o enunciado
    df = df_union.select(
        F.col("vendor_id").cast(IntegerType()).alias("VendorID"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("pickup_datetime").alias("tpep_pickup_datetime"),
        F.col("dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.col("taxi_type").cast(StringType()).alias("taxi_type"),
        F.col("partition_year").cast(IntegerType()).alias("partition_year"),
        F.col("partition_month").cast(IntegerType()).alias("partition_month"),
    )
    logger.info(
        "Colunas renomeadas | vendor_id->VendorID | pickup/dropoff_datetime->tpep_*"
    )

    # Filtros de negócio
    df = (
        df.filter(F.year(F.col("tpep_pickup_datetime")) == ANO)
        .filter(F.month(F.col("tpep_pickup_datetime")).between(MES_INICIO, MES_FIM))
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("passenger_count") <= 6)
    )

    logger.info(
        "Filtros aplicados | ano=%s | meses=%s a %s | total_amount>0 | passenger_count 1-6",
        ANO,
        MES_INICIO,
        MES_FIM,
    )

    total = df.count()
    logger.info(
        "Transform concluido | tabela=table_all_taxi_gold | registros=%s", f"{total:,}"
    )

    return df


# ─── Transform — Query 1 ────────────────────────────────────────────────────
def transform_query1(df_gold):
    logger.info("Iniciando transform | tabela=table_avg_total_amount_gold")

    # Query 1 — média de total_amount por mês considerando apenas yellow taxi
    df = (
        df_gold.filter(F.col("taxi_type") == "yellow")
        .groupBy(F.month(F.col("tpep_pickup_datetime")).alias("mes"))
        .agg(F.round(F.avg("total_amount"), 2).alias("avg_total_amount"))
        .orderBy("mes")
    )

    logger.info("Transform concluido | tabela=table_avg_total_amount_gold")

    return df


# ─── Transform — Query 2 ────────────────────────────────────────────────────
def transform_query2(df_gold):
    logger.info("Iniciando transform | tabela=table_avg_passengers_gold")

    # Query 2 — média de passenger_count por hora em maio (yellow + green)
    df = (
        df_gold.filter(F.month(F.col("tpep_pickup_datetime")) == MES_MAIO)
        .withColumn("hora", F.hour(F.col("tpep_pickup_datetime")))
        .groupBy("hora")
        .agg(F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"))
        .orderBy("hora")
    )

    logger.info("Transform concluido | tabela=table_avg_passengers_gold")

    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df, destino: str, partition: bool = True):
    s3_path = f"s3://{BUCKET_NAME}/gold/{destino}/"
    logger.info("Iniciando load | destino=%s", s3_path)

    writer = df.coalesce(1).write.mode("overwrite")

    if partition:
        writer.partitionBy("partition_year", "partition_month").parquet(s3_path)
    else:
        writer.parquet(s3_path)

    logger.info("Load concluido | destino=%s", s3_path)


# ─── Pipeline ───────────────────────────────────────────────────────────────
def run():
    logger.info("Iniciando pipeline Gold")

    try:
        # 1. Extract do Silver
        df_yellow = extract("table_yellow_taxi_silver")
        df_green = extract("table_green_taxi_silver")

        # 2. Transform + Load — table_all_taxi_gold
        logger.info("─" * 60)
        df_gold = transform_all_taxi(df_yellow, df_green)
        df_gold.cache()
        load(df_gold, "table_all_taxi_gold", partition=True)

        # 3. Transform + Load — Query 1
        logger.info("─" * 60)
        df_query1 = transform_query1(df_gold)
        load(df_query1, "table_avg_total_amount_gold", partition=False)

        # 4. Transform + Load — Query 2
        logger.info("─" * 60)
        df_query2 = transform_query2(df_gold)
        load(df_query2, "table_avg_passengers_gold", partition=False)

        df_gold.unpersist()
        logger.info("─" * 60)
        logger.info("Pipeline Gold finalizado com sucesso")

    except (ValueError, OSError) as e:
        logger.error("Falha no pipeline Gold | erro=%s", e, exc_info=True)
        raise


run()
job.commit()
