"""Script responsável pela ingestão dos dados brutos do Green Taxi na camada Bronze do Data Lake.

Este script realiza o download dos arquivos Parquet mensais do NYC TLC (Taxi & Limousine
Commission), aplica padronização de colunas e tipagem explícita via schema, e persiste
os dados particionados no S3 na camada Bronze.

Os dados são extraídos diretamente da CDN pública do NYC TLC, transformados com pandas
para garantir consistência de schema entre os meses, e carregados no S3 com particionamento
por ano e mês.

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
logger = logging.getLogger("etl_green_taxi_bronze")

# ─── Configurações ──────────────────────────────────────────────────────────
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
REQUEST_TIMEOUT = 120
PREFIXO = "green_tripdata"
TABLE_NAME = "table_green_taxi_bronze"
MESES = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]

# ─── Schema explícito ────────────────────────────────────────────────────────
SCHEMA = {
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


# ─── Extract ────────────────────────────────────────────────────────────────
def extract(mes: str) -> BytesIO:
    """Realiza o download do arquivo Parquet mensal do Green Taxi a partir da CDN do NYC TLC.

    Args:
        mes: Mês de referência no formato 'AAAA-MM'.

    Returns:
        Buffer em memória com o conteúdo do arquivo Parquet baixado.

    Raises:
        requests.RequestException: Se o download falhar ou retornar status de erro.
    """
    url = f"{BASE_URL}/{PREFIXO}_{mes}.parquet"
    logger.info("Iniciando download | url=%s", url)

    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    dados = BytesIO(response.content)
    tamanho_mb = round(len(response.content) / 1024 / 1024, 2)
    logger.info("Download concluido | tamanho=%sMB", tamanho_mb)

    return dados


# ─── Transform ──────────────────────────────────────────────────────────────
def transform(dados: BytesIO, mes: str) -> BytesIO:
    """Padroniza colunas para lowercase e aplica tipagem explícita via schema do Green Taxi.

    Args:
        dados: Buffer em memória com o arquivo Parquet bruto baixado pelo extract.
        mes  : Mês de referência no formato 'AAAA-MM', usado nos logs.

    Returns:
        Buffer em memória com o Parquet transformado e pronto para carga no S3.

    Raises:
        ValueError: Se o arquivo estiver vazio.
    """
    logger.info("Iniciando transform | tabela=%s | mes=%s", TABLE_NAME, mes)

    if dados.getbuffer().nbytes == 0:
        logger.error("Arquivo vazio | tabela=%s | mes=%s", TABLE_NAME, mes)
        raise ValueError(f"Arquivo vazio | tabela={TABLE_NAME} | mes={mes}")

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
            TABLE_NAME,
            mes,
            colunas_renomeadas,
        )
    else:
        logger.info(
            "Nenhuma coluna precisou ser renomeada | tabela=%s | mes=%s",
            TABLE_NAME,
            mes,
        )

    # 2. Aplica schema explícito — garante tipagem consistente entre meses
    for col, dtype in SCHEMA.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Falha converter | tabela=%s | mes=%s | col=%s | dtype=%s | erro=%s",
                    TABLE_NAME,
                    mes,
                    col,
                    dtype,
                    e,
                )

    logger.info(
        "Transform concluido | tabela=%s | mes=%s | colunas=%s | linhas=%s",
        TABLE_NAME,
        mes,
        len(df.columns),
        f"{len(df):,}",
    )

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer


# ─── Load ───────────────────────────────────────────────────────────────────
def load(dados: BytesIO, mes: str, bucket_name: str, s3_client):
    """Persiste o arquivo Parquet transformado no S3 particionado por ano e mês.

    Args:
        dados      : Buffer em memória com o Parquet transformado.
        mes        : Mês de referência no formato 'AAAA-MM'.
        bucket_name: Nome do bucket S3 de destino.
        s3_client  : Cliente boto3 S3.
    """
    ano, month = mes.split("-")
    s3_key = (
        f"bronze/{TABLE_NAME}/"
        f"partition_year={int(ano)}/partition_month={int(month)}/"
        f"{PREFIXO}_{mes}.parquet"
    )

    logger.info("Iniciando carga | destino=s3://%s/%s", bucket_name, s3_key)
    s3_client.upload_fileobj(dados, bucket_name, s3_key)
    logger.info("Carga concluida | destino=s3://%s/%s", bucket_name, s3_key)


# ─── Pipeline ───────────────────────────────────────────────────────────────
def run():
    """Executa o pipeline completo para todos os meses: extract → transform → load."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket_name = args["BUCKET_NAME"]
    s3_client = boto3.client("s3")

    logger.info("Iniciando pipeline | tabela=%s", TABLE_NAME)
    sucessos, falhas = 0, 0

    for mes in MESES:
        logger.info("─" * 60)
        logger.info("Processando | tabela=%s | mes=%s", TABLE_NAME, mes)
        try:
            dados = extract(mes)
            dados = transform(dados, mes)
            load(dados, mes, bucket_name, s3_client)
            sucessos += 1
        except (ValueError, requests.RequestException, OSError) as e:
            logger.error(
                "Falha ao processar | tabela=%s | mes=%s | erro=%s",
                TABLE_NAME,
                mes,
                e,
                exc_info=True,
            )
            falhas += 1

    logger.info("─" * 60)
    logger.info(
        "Pipeline finalizado | tabela=%s | sucessos=%s | falhas=%s",
        TABLE_NAME,
        sucessos,
        falhas,
    )
    job.commit()


if __name__ == "__main__":
    run()
