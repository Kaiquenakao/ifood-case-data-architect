"""Testes unitários para o etl_green_taxi_bronze.

Cobre extract, transform e load do pipeline Bronze do Green Taxi.
Utiliza moto para mockar o S3 e responses para mockar o download HTTP.
"""

import sys
import os

# ─── Adiciona o mock de awsglue e pyspark ao path antes de qualquer import ──
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import boto3
from io import BytesIO

import pandas as pd
import responses as responses_mock

from moto import mock_aws

from src.bronze.etl_green_taxi_bronze import (
    extract,
    transform,
    load,
    TABLE_NAME,
    SCHEMA,
    PREFIXO,
    BASE_URL,
)

BUCKET = "test-bucket"


# ─── Helpers ────────────────────────────────────────────────────────────────
def make_parquet_buffer(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def fake_green_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "VendorID": [1, 2],
            "lpep_pickup_datetime": pd.to_datetime(
                ["2023-01-01 07:00", "2023-01-02 08:00"]
            ),
            "lpep_dropoff_datetime": pd.to_datetime(
                ["2023-01-01 07:30", "2023-01-02 08:45"]
            ),
            "store_and_fwd_flag": ["N", "N"],
            "RatecodeID": [1.0, 1.0],
            "PULocationID": [50, 60],
            "DOLocationID": [70, 80],
            "passenger_count": [1.0, 2.0],
            "trip_distance": [3.0, 4.5],
            "fare_amount": [12.0, 18.0],
            "extra": [0.5, 0.5],
            "mta_tax": [0.5, 0.5],
            "tip_amount": [1.5, 2.0],
            "tolls_amount": [0.0, 0.0],
            "ehail_fee": [0.0, 0.0],
            "improvement_surcharge": [0.3, 0.3],
            "total_amount": [14.8, 21.3],
            "payment_type": [1.0, 2.0],
            "trip_type": [1.0, 1.0],
            "congestion_surcharge": [2.5, 2.5],
        }
    )


# ─── Extract ────────────────────────────────────────────────────────────────
class TestExtract:
    @responses_mock.activate
    def test_returns_bytesio_with_content(self):
        """Valida que o extract retorna um BytesIO com conteúdo."""
        url = f"{BASE_URL}/{PREFIXO}_2023-01.parquet"
        responses_mock.add(
            responses_mock.GET,
            url,
            body=make_parquet_buffer(fake_green_df()),
            status=200,
        )

        result = extract("2023-01")

        assert isinstance(result, BytesIO)
        assert result.getbuffer().nbytes > 0

    @responses_mock.activate
    def test_http_404_raises_exception(self):
        """Valida que HTTP 404 levanta exceção."""
        url = f"{BASE_URL}/{PREFIXO}_2023-01.parquet"
        responses_mock.add(responses_mock.GET, url, status=404)

        with pytest.raises(Exception):
            extract("2023-01")


# ─── Transform ──────────────────────────────────────────────────────────────
class TestTransform:
    def test_columns_normalized_to_lowercase(self, raw_green_parquet):
        """Valida que colunas em maiúsculo são normalizadas para lowercase."""
        result = transform(raw_green_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        for col in df.columns:
            assert col == col.lower(), f"Coluna '{col}' não está em lowercase"

    def test_schema_types_applied_correctly(self, raw_green_parquet):
        """Valida que os tipos do SCHEMA são aplicados corretamente."""
        result = transform(raw_green_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        assert df["vendorid"].dtype == "Int64"
        assert df["total_amount"].dtype == "float64"
        assert df["passenger_count"].dtype == "float64"
        assert str(df["lpep_pickup_datetime"].dtype) == "datetime64[us]"
        assert str(df["lpep_dropoff_datetime"].dtype) == "datetime64[us]"

    def test_returns_bytesio(self, raw_green_parquet):
        """Valida que o transform retorna um BytesIO."""
        result = transform(raw_green_parquet, mes="2023-01")
        assert isinstance(result, BytesIO)

    def test_buffer_not_empty(self, raw_green_parquet):
        """Valida que o buffer retornado contém dados."""
        result = transform(raw_green_parquet, mes="2023-01")
        assert result.getbuffer().nbytes > 0

    def test_empty_file_raises_value_error(self, empty_parquet):
        """Valida que arquivo vazio levanta ValueError."""
        with pytest.raises(ValueError, match="Arquivo vazio"):
            transform(empty_parquet, mes="2023-01")

    def test_row_count_preserved(self, raw_green_parquet):
        """Valida que o transform não remove nem duplica linhas."""
        df_original = pd.read_parquet(raw_green_parquet)
        raw_green_parquet.seek(0)
        result = transform(raw_green_parquet, mes="2023-01")
        df_result = pd.read_parquet(result)
        assert len(df_result) == len(df_original)

    def test_all_schema_columns_present(self, raw_green_parquet):
        """Valida que todas as colunas do SCHEMA estão presentes."""
        result = transform(raw_green_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        for col in SCHEMA.keys():
            assert col in df.columns, f"Coluna obrigatória ausente: {col}"

    def test_green_exclusive_column_present(self, raw_green_parquet):
        """Valida que ehail_fee — exclusiva do Green — está presente."""
        result = transform(raw_green_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        assert "ehail_fee" in df.columns


# ─── Load ───────────────────────────────────────────────────────────────────
class TestLoad:
    def test_file_saved_to_correct_s3_path(self, raw_green_parquet):
        """Valida que o load persiste o arquivo no caminho correto do S3."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)

            dados = transform(raw_green_parquet, mes="2023-01")
            load(dados, mes="2023-01", bucket_name=BUCKET, s3_client=s3)

            key = f"bronze/{TABLE_NAME}/partition_year=2023/partition_month=1/{PREFIXO}_2023-01.parquet"
            response = s3.get_object(Bucket=BUCKET, Key=key)
            assert response["ContentLength"] > 0

    def test_saved_file_is_readable_parquet(self, raw_green_parquet):
        """Valida que o arquivo salvo no S3 é um Parquet válido e legível."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)

            dados = transform(raw_green_parquet, mes="2023-01")
            load(dados, mes="2023-01", bucket_name=BUCKET, s3_client=s3)

            key = f"bronze/{TABLE_NAME}/partition_year=2023/partition_month=1/{PREFIXO}_2023-01.parquet"
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))

            assert len(df) == 2
            assert "vendorid" in df.columns
