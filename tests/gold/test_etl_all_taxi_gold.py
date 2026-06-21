"""Testes unitários para o etl_all_taxi_gold.

Cobre transform e load do pipeline Gold consolidado.
Utiliza PySpark local para testar as transformações e mock para o load no S3.

O extract não é testado pois depende do Glue Data Catalog.
O load é mockado via unittest.mock pois escreve diretamente no S3 via PySpark.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import src.gold.etl_all_taxi_gold as mod


@pytest.fixture(scope="module")
def spark():
    """Cria e retorna uma SparkSession local para os testes.

    Returns:
        SparkSession configurada em modo local.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-all-taxi-gold")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def df_yellow_silver(spark):
    """Retorna um DataFrame Spark simulando a tabela table_yellow_taxi_silver.

    Args:
        spark: SparkSession fixture.

    Returns:
        DataFrame Spark com schema Silver do Yellow Taxi.
    """
    data = [
        (1, 2, 15.0, "2023-01-01 08:00:00", "2023-01-01 08:30:00", "yellow", 2023, 1),
        (2, 1, 25.0, "2023-02-15 09:00:00", "2023-02-15 09:45:00", "yellow", 2023, 2),
        (1, 3, 30.0, "2023-05-10 14:00:00", "2023-05-10 14:30:00", "yellow", 2023, 5),
        (2, 7, 50.0, "2023-05-10 14:00:00", "2023-05-10 15:00:00", "yellow", 2023, 5),  # passenger_count > 6
        (1, 2, -5.0, "2023-05-10 14:00:00", "2023-05-10 14:30:00", "yellow", 2023, 5),  # total_amount < 0
    ]
    return spark.createDataFrame(
        data,
        ["vendor_id", "passenger_count", "total_amount",
         "pickup_datetime", "dropoff_datetime", "taxi_type",
         "partition_year", "partition_month"],
    )


@pytest.fixture
def df_green_silver(spark):
    """Retorna um DataFrame Spark simulando a tabela table_green_taxi_silver.

    Args:
        spark: SparkSession fixture.

    Returns:
        DataFrame Spark com schema Silver do Green Taxi.
    """
    data = [
        (1, 2, 14.0, "2023-01-10 07:00:00", "2023-01-10 07:30:00", "green", 2023, 1),
        (2, 4, 20.0, "2023-05-15 14:00:00", "2023-05-15 14:45:00", "green", 2023, 5),
    ]
    return spark.createDataFrame(
        data,
        ["vendor_id", "passenger_count", "total_amount",
         "pickup_datetime", "dropoff_datetime", "taxi_type",
         "partition_year", "partition_month"],
    )


# ─── Transform ──────────────────────────────────────────────────────────────
class TestTransform:

    def test_union_yellow_e_green(self, df_yellow_silver, df_green_silver):
        """Valida que o union retorna registros de ambos os táxis."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        tipos = [r["taxi_type"] for r in result.select("taxi_type").collect()]
        assert "yellow" in tipos
        assert "green" in tipos

    def test_gold_schema_columns_present(self, df_yellow_silver, df_green_silver):
        """Valida que todas as colunas do schema Gold estão presentes."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        colunas_esperadas = {
            "VendorID", "passenger_count", "total_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "taxi_type", "partition_year", "partition_month",
        }
        assert colunas_esperadas == set(result.columns)

    def test_vendor_id_renamed_to_VendorID(self, df_yellow_silver, df_green_silver):
        """Valida que vendor_id é renomeado para VendorID."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        assert "VendorID" in result.columns
        assert "vendor_id" not in result.columns

    def test_pickup_renamed_to_tpep(self, df_yellow_silver, df_green_silver):
        """Valida que pickup_datetime é renomeado para tpep_pickup_datetime."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        assert "tpep_pickup_datetime" in result.columns
        assert "pickup_datetime" not in result.columns

    def test_filtro_ano(self, df_yellow_silver, df_green_silver):
        """Valida que apenas registros do ano 2023 são mantidos."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        invalidos = result.filter(F.year("tpep_pickup_datetime") != mod.ANO).count()
        assert invalidos == 0

    def test_filtro_meses(self, df_yellow_silver, df_green_silver):
        """Valida que apenas registros entre janeiro e maio são mantidos."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        invalidos = result.filter(
            ~F.month("tpep_pickup_datetime").between(mod.MES_INICIO, mod.MES_FIM)
        ).count()
        assert invalidos == 0

    def test_filtro_total_amount_maior_zero(self, df_yellow_silver, df_green_silver):
        """Valida que registros com total_amount <= 0 são removidos."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        assert result.filter(result["total_amount"] <= 0).count() == 0

    def test_filtro_passenger_count_maximo_6(self, df_yellow_silver, df_green_silver):
        """Valida que registros com passenger_count > 6 são removidos."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        assert result.filter(result["passenger_count"] > 6).count() == 0

    def test_resultado_nao_vazio(self, df_yellow_silver, df_green_silver):
        """Valida que o resultado possui ao menos um registro."""
        result = mod.transform(df_yellow_silver, df_green_silver)
        assert result.count() > 0


# ─── Load ───────────────────────────────────────────────────────────────────
class TestLoad:

    def test_load_calls_parquet_write(self, spark, df_yellow_silver, df_green_silver):
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_yellow_silver, df_green_silver)

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch("boto3.client"), \
             patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)):
            mod.load(df)

        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.partitionBy.assert_called_once_with("partition_year", "partition_month")

    def test_load_s3_path_correct(self, spark, df_yellow_silver, df_green_silver):
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_yellow_silver, df_green_silver)
        captured_path = {}

        def fake_parquet(path):
            captured_path["path"] = path

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.parquet.side_effect = fake_parquet

        with patch("boto3.client"), \
             patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)):
            mod.load(df)

        assert "table_all_taxi_gold" in captured_path.get("path", "")
        assert "gold" in captured_path.get("path", "")