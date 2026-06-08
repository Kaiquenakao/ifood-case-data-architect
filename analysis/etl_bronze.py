import sys
import logging
from io import BytesIO

import boto3
import pandas as pd
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger("etl_bronze")

# ─── Inicializa contexto Glue e Spark ───────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ─── Configurações ──────────────────────────────────────────────────────────
BUCKET_NAME = "ifood-case-data-lake-kaique"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
REQUEST_TIMEOUT = 120
MESES = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]
TABELAS = {
    "table_yellow_taxi": "yellow_tripdata",
    "table_green_taxi": "green_tripdata",
}

s3_client = boto3.client("s3")

# ─── Schemas explícitos por tabela ──────────────────────────────────────────
SCHEMA_YELLOW = {
    "vendorid": "Int64",
    "tpep_pickup_datetime": "datetime64[us]",
    "tpep_dropoff_datetime": "datetime64[us]",
    "passenger_count": "float64",
    "trip_distance": "float64",
    "ratecodeid": "float64",
    "store_and_fwd_flag": "object",
    "pulocationid": "Int64",
    "dolocationid": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "airport_fee": "float64",
}

SCHEMA_GREEN = {
    "vendorid": "Int64",
    "lpep_pickup_datetime": "datetime64[us]",
    "lpep_dropoff_datetime": "datetime64[us]",
    "store_and_fwd_flag": "object",
    "ratecodeid": "float64",
    "pulocationid": "Int64",
    "dolocationid": "Int64",
    "passenger_count": "float64",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "payment_type": "float64",
    "trip_type": "float64",
    "congestion_surcharge": "float64",
}

SCHEMAS = {
    "table_yellow_taxi": SCHEMA_YELLOW,
    "table_green_taxi": SCHEMA_GREEN,
}


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(prefixo: str, mes: str) -> BytesIO:
    url = f"{BASE_URL}/{prefixo}_{mes}.parquet"
    logger.info("Iniciando download | url=%s", url)

    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    dados = BytesIO(response.content)
    tamanho_mb = round(len(response.content) / 1024 / 1024, 2)
    logger.info("Download concluido | tamanho=%sMB", tamanho_mb)

    return dados


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(dados: BytesIO, tabela: str, mes: str) -> BytesIO:
    logger.info("Iniciando transform | tabela=%s | mes=%s", tabela, mes)

    if dados.getbuffer().nbytes == 0:
        logger.error("Arquivo vazio | tabela=%s | mes=%s", tabela, mes)
        raise ValueError(f"Arquivo vazio | tabela={tabela} | mes={mes}")

    df = pd.read_parquet(dados)

    # 1. Padroniza nomes de colunas para lowercase
    colunas_originais = df.columns.tolist()
    df.columns = [col.lower() for col in df.columns]

    colunas_renomeadas = [
        f"{orig} -> {novo}"
        for orig, novo in zip(colunas_originais, df.columns)
        if orig != novo
    ]

    if colunas_renomeadas:
        logger.warning(
            "Colunas padronizadas para lowercase | tabela=%s | mes=%s | colunas=%s",
            tabela,
            mes,
            colunas_renomeadas,
        )
    else:
        logger.info(
            "Nenhuma coluna precisou ser renomeada | tabela=%s | mes=%s",
            tabela,
            mes,
        )

    # 2. Aplica schema explícito — garante tipagem consistente entre meses
    schema = SCHEMAS[tabela]
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Falha ao converter coluna | tabela=%s | mes=%s | "
                    "coluna=%s | dtype=%s | erro=%s",
                    tabela,
                    mes,
                    col,
                    dtype,
                    e,
                )

    logger.info(
        "Transform concluido | tabela=%s | mes=%s | colunas=%s | linhas=%s",
        tabela,
        mes,
        len(df.columns),
        f"{len(df):,}",
    )

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer


# ─── Load ───────────────────────────────────────────────────────────────────
def load(dados: BytesIO, prefixo: str, tabela: str, mes: str):
    ano, month = mes.split("-")
    s3_key = (
        f"bronze/{tabela}_bronze/"
        f"partition_year={int(ano)}/partition_month={int(month)}/"
        f"{prefixo}_{mes}.parquet"
    )

    logger.info("Iniciando carga | destino=s3://%s/%s", BUCKET_NAME, s3_key)
    s3_client.upload_fileobj(dados, BUCKET_NAME, s3_key)
    logger.info("Carga concluida | destino=s3://%s/%s", BUCKET_NAME, s3_key)


# ─── Pipeline ───────────────────────────────────────────────────────────────
def run():
    logger.info("Iniciando pipeline Bronze")
    sucessos, falhas = 0, 0

    for tabela, prefixo in TABELAS.items():
        for mes in MESES:
            logger.info("─" * 60)
            logger.info("Processando | tabela=%s | mes=%s", tabela, mes)
            try:
                dados = extract(prefixo, mes)
                dados = transform(dados, tabela, mes)
                load(dados, prefixo, tabela, mes)
                sucessos += 1
            except (ValueError, requests.RequestException, OSError) as e:
                logger.error(
                    "Falha ao processar | tabela=%s | mes=%s | erro=%s",
                    tabela,
                    mes,
                    e,
                    exc_info=True,
                )
                falhas += 1

    logger.info("─" * 60)
    logger.info(
        "Pipeline Bronze finalizado | sucessos=%s | falhas=%s",
        sucessos,
        falhas,
    )


run()
job.commit()
