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
logger = logging.getLogger("etl_silver")

# ─── Inicializa contexto Glue e Spark ───────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = args["BUCKET_NAME"]
DATABASE_BRONZE = "ifood_case_bronze"

TABELAS = {
    "table_yellow_taxi_bronze": {
        "destino": "table_yellow_taxi_silver",
        "pickup": "tpep_pickup_datetime",
        "dropoff": "tpep_dropoff_datetime",
        "taxi_type": "yellow",
    },
    "table_green_taxi_bronze": {
        "destino": "table_green_taxi_silver",
        "pickup": "lpep_pickup_datetime",
        "dropoff": "lpep_dropoff_datetime",
        "taxi_type": "green",
    },
}

COLUNAS_OBRIGATORIAS = [
    "vendor_id",
    "passenger_count",
    "total_amount",
    "pickup_datetime",
    "dropoff_datetime",
]


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(tabela: str):
    logger.info("Iniciando extract | tabela=%s | database=%s", tabela, DATABASE_BRONZE)

    df = glueContext.create_dynamic_frame.from_catalog(
        database=DATABASE_BRONZE,
        table_name=tabela,
        transformation_ctx=tabela,
    ).toDF()

    logger.info("Extract concluido | tabela=%s", tabela)

    return df


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(df, tabela: str, pickup: str, dropoff: str, taxi_type: str):
    logger.info("Iniciando transform | tabela=%s", tabela)

    df = df.select(
        F.col("vendorid").cast(IntegerType()).alias("vendor_id"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col(pickup).cast("timestamp").alias("pickup_datetime"),
        F.col(dropoff).cast("timestamp").alias("dropoff_datetime"),
        F.lit(taxi_type).cast(StringType()).alias("taxi_type"),
        F.col("partition_year").cast(IntegerType()).alias("partition_year"),
        F.col("partition_month").cast(IntegerType()).alias("partition_month"),
    )
    logger.info("Colunas selecionadas, renomeadas e tipadas | tabela=%s", tabela)

    df = df.dropna(subset=COLUNAS_OBRIGATORIAS)
    logger.info("Nulos removidos | tabela=%s", tabela)

    total = df.count()
    logger.info(
        "Transform concluido | tabela=%s | registros=%s",
        tabela,
        f"{total:,}",
    )

    return df


# ─── Load ───────────────────────────────────────────────────────────────────
def load(df, destino: str):
    s3_path = f"s3://{BUCKET_NAME}/silver/{destino}/"
    logger.info("Iniciando load | destino=%s", s3_path)

    df.coalesce(1).write.mode("overwrite").partitionBy(
        "partition_year", "partition_month"
    ).parquet(s3_path)

    logger.info("Load concluido | destino=%s", s3_path)


# ─── Pipeline ───────────────────────────────────────────────────────────────
def run():
    logger.info("Iniciando pipeline Silver")
    sucessos, falhas = 0, 0

    for tabela, config in TABELAS.items():
        logger.info("─" * 60)
        logger.info("Processando | tabela=%s", tabela)
        try:
            df = extract(tabela)
            df = transform(
                df,
                tabela,
                config["pickup"],
                config["dropoff"],
                config["taxi_type"],
            )
            load(df, config["destino"])
            sucessos += 1
        except (ValueError, OSError) as e:
            logger.error(
                "Falha ao processar | tabela=%s | erro=%s",
                tabela,
                e,
                exc_info=True,
            )
            falhas += 1

    logger.info("─" * 60)
    logger.info(
        "Pipeline Silver finalizado | sucessos=%s | falhas=%s",
        sucessos,
        falhas,
    )


run()
job.commit()
