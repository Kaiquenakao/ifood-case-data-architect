"""Testes unitários para o etl_yellow_taxi_bronze.

Cobre extract, transform e load do pipeline Bronze do Yellow Taxi.
Utiliza moto para mockar o S3 e responses para mockar o download HTTP.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import boto3
from io import BytesIO
import pandas as pd
import responses as responses_mock
from moto import mock_aws

import src.bronze.etl_yellow_taxi_bronze as mod

BUCKET = "mock-bucket"


# ─── Helpers ────────────────────────────────────────────────────────────────
def make_parquet_buffer(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def fake_yellow_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": pd.to_datetime(
                ["2023-01-01 08:00", "2023-01-02 09:00"]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                ["2023-01-01 08:30", "2023-01-02 09:45"]
            ),
            "passenger_count": [1.0, 2.0],
            "trip_distance": [2.5, 5.0],
            "RatecodeID": [1.0, 1.0],
            "store_and_fwd_flag": ["N", "Y"],
            "PULocationID": [100, 200],
            "DOLocationID": [150, 250],
            "payment_type": [1, 2],
            "fare_amount": [10.0, 20.0],
            "extra": [0.5, 1.0],
            "mta_tax": [0.5, 0.5],
            "tip_amount": [2.0, 3.0],
            "tolls_amount": [0.0, 0.0],
            "improvement_surcharge": [0.3, 0.3],
            "total_amount": [13.3, 24.8],
            "congestion_surcharge": [2.5, 2.5],
            "airport_fee": [0.0, 0.0],
        }
    )


# ─── Extract ────────────────────────────────────────────────────────────────
class TestExtract:
    @responses_mock.activate
    def test_returns_bytesio_with_content(self):
        """Valida que o extract retorna um BytesIO com conteúdo."""
        url = f"{mod.BASE_URL}/{mod.PREFIXO}_2023-01.parquet"
        responses_mock.add(
            responses_mock.GET,
            url,
            body=make_parquet_buffer(fake_yellow_df()),
            status=200,
        )
        result = mod.extract("2023-01")
        assert isinstance(result, BytesIO)
        assert result.getbuffer().nbytes > 0

    @responses_mock.activate
    def test_http_404_raises_exception(self):
        """Valida que HTTP 404 levanta exceção."""
        url = f"{mod.BASE_URL}/{mod.PREFIXO}_2023-01.parquet"
        responses_mock.add(responses_mock.GET, url, status=404)
        with pytest.raises(Exception):
            mod.extract("2023-01")


# ─── Transform ──────────────────────────────────────────────────────────────
class TestTransform:
    def test_columns_normalized_to_lowercase(self, raw_yellow_parquet):
        """Valida que colunas em maiúsculo são normalizadas para lowercase."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        for col in df.columns:
            assert col == col.lower(), f"Coluna '{col}' não está em lowercase"

    def test_schema_types_applied_correctly(self, raw_yellow_parquet):
        """Valida que os tipos do SCHEMA são aplicados corretamente."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        assert df["vendorid"].dtype == "Int64"
        assert df["total_amount"].dtype == "float64"
        assert df["passenger_count"].dtype == "float64"
        assert str(df["tpep_pickup_datetime"].dtype) == "datetime64[us]"
        assert str(df["tpep_dropoff_datetime"].dtype) == "datetime64[us]"

    def test_returns_bytesio(self, raw_yellow_parquet):
        """Valida que o transform retorna um BytesIO."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        assert isinstance(result, BytesIO)

    def test_buffer_not_empty(self, raw_yellow_parquet):
        """Valida que o buffer retornado contém dados."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        assert result.getbuffer().nbytes > 0

    def test_empty_file_raises_value_error(self, empty_parquet):
        """Valida que arquivo vazio levanta ValueError."""
        with pytest.raises(ValueError, match="Arquivo vazio"):
            mod.transform(empty_parquet, mes="2023-01")

    def test_row_count_preserved(self, raw_yellow_parquet):
        """Valida que o transform não remove nem duplica linhas."""
        df_original = pd.read_parquet(raw_yellow_parquet)
        raw_yellow_parquet.seek(0)
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        df_result = pd.read_parquet(result)
        assert len(df_result) == len(df_original)

    def test_all_schema_columns_present(self, raw_yellow_parquet):
        """Valida que todas as colunas do SCHEMA estão presentes."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        for col in mod.SCHEMA.keys():
            assert col in df.columns, f"Coluna obrigatória ausente: {col}"

    def test_yellow_exclusive_column_present(self, raw_yellow_parquet):
        """Valida que airport_fee — exclusiva do Yellow — está presente."""
        result = mod.transform(raw_yellow_parquet, mes="2023-01")
        df = pd.read_parquet(result)
        assert "airport_fee" in df.columns


# ─── Load ───────────────────────────────────────────────────────────────────
class TestLoad:
    def test_file_saved_to_correct_s3_path(self, raw_yellow_parquet):
        """Valida que o load persiste o arquivo no caminho correto do S3."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)
            mod.BUCKET_NAME = BUCKET
            mod.s3_client = s3
            dados = mod.transform(raw_yellow_parquet, mes="2023-01")
            mod.load(dados, mes="2023-01")
            key = f"bronze/{mod.TABLE_NAME}/partition_year=2023/partition_month=1/{mod.PREFIXO}_2023-01.parquet"
            response = s3.get_object(Bucket=BUCKET, Key=key)
            assert response["ContentLength"] > 0

    def test_saved_file_is_readable_parquet(self, raw_yellow_parquet):
        """Valida que o arquivo salvo no S3 é um Parquet válido e legível."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)
            mod.BUCKET_NAME = BUCKET
            mod.s3_client = s3
            raw_yellow_parquet.seek(0)
            dados = mod.transform(raw_yellow_parquet, mes="2023-02")
            mod.load(dados, mes="2023-02")
            key = f"bronze/{mod.TABLE_NAME}/partition_year=2023/partition_month=2/{mod.PREFIXO}_2023-02.parquet"
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            assert len(df) == 2
            assert "vendorid" in df.columns
