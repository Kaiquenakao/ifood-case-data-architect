"""Testes unitários para o etl_green_taxi_silver.

Cobre transform e load do pipeline Silver do Green Taxi.
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

import src.silver.etl_green_taxi_silver as mod


@pytest.fixture(scope="module")
def spark():
    """Cria e retorna uma SparkSession local para os testes.

    Returns:
        SparkSession configurada em modo local.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-green-silver")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def df_bronze_green(spark):
    """Retorna um DataFrame Spark simulando a tabela table_green_taxi_bronze.

    Contém registros válidos e com nulos nas colunas obrigatórias
    para validar a remoção de nulos no transform.

    Args:
        spark: SparkSession fixture.

    Returns:
        DataFrame Spark com schema Bronze do Green Taxi.
    """
    data = [
        (1, 2, 14.0, "2023-01-10 07:00:00", "2023-01-10 07:30:00", 2023, 1),
        (2, 1, 22.0, "2023-02-20 08:00:00", "2023-02-20 08:45:00", 2023, 2),
        (1, None, 18.0, "2023-03-05 09:00:00", "2023-03-05 09:30:00", 2023, 3),
        (2, 3, None, "2023-04-15 10:00:00", "2023-04-15 10:20:00", 2023, 4),
    ]
    return spark.createDataFrame(
        data,
        [
            "vendorid",
            "passenger_count",
            "total_amount",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "partition_year",
            "partition_month",
        ],
    )


# ─── Transform ──────────────────────────────────────────────────────────────
class TestTransform:
    def test_silver_schema_columns_present(self, df_bronze_green):
        """Valida que todas as colunas do schema Silver estão presentes no resultado."""
        result = mod.transform(df_bronze_green)
        colunas_esperadas = {
            "vendor_id",
            "passenger_count",
            "total_amount",
            "pickup_datetime",
            "dropoff_datetime",
            "taxi_type",
            "partition_year",
            "partition_month",
        }
        assert colunas_esperadas == set(result.columns)

    def test_vendorid_renamed_to_vendor_id(self, df_bronze_green):
        """Valida que vendorid é renomeado para vendor_id."""
        result = mod.transform(df_bronze_green)
        assert "vendor_id" in result.columns
        assert "vendorid" not in result.columns

    def test_pickup_renamed_to_pickup_datetime(self, df_bronze_green):
        """Valida que lpep_pickup_datetime é renomeado para pickup_datetime."""
        result = mod.transform(df_bronze_green)
        assert "pickup_datetime" in result.columns
        assert "lpep_pickup_datetime" not in result.columns

    def test_dropoff_renamed_to_dropoff_datetime(self, df_bronze_green):
        """Valida que lpep_dropoff_datetime é renomeado para dropoff_datetime."""
        result = mod.transform(df_bronze_green)
        assert "dropoff_datetime" in result.columns
        assert "lpep_dropoff_datetime" not in result.columns

    def test_taxi_type_literal_green(self, df_bronze_green):
        """Valida que taxi_type é adicionado com valor literal 'green'."""
        result = mod.transform(df_bronze_green)
        valores = [r["taxi_type"] for r in result.select("taxi_type").collect()]
        assert all(v == "green" for v in valores)

    def test_nulls_removed_from_mandatory_columns(self, df_bronze_green):
        """Valida que registros com nulos nas colunas obrigatórias são removidos."""
        result = mod.transform(df_bronze_green)
        for col in mod.COLUNAS_OBRIGATORIAS:
            nulos = result.filter(result[col].isNull()).count()
            assert nulos == 0, f"Coluna '{col}' ainda possui nulos após transform"

    def test_valid_rows_preserved(self, df_bronze_green):
        """Valida que apenas os registros sem nulos obrigatórios são preservados."""
        result = mod.transform(df_bronze_green)
        assert result.count() == 2

    def test_vendor_id_type_is_integer(self, df_bronze_green):
        """Valida que vendor_id é do tipo IntegerType."""
        result = mod.transform(df_bronze_green)
        assert dict(result.dtypes)["vendor_id"] == "int"

    def test_total_amount_type_is_double(self, df_bronze_green):
        """Valida que total_amount é do tipo DoubleType."""
        result = mod.transform(df_bronze_green)
        assert dict(result.dtypes)["total_amount"] == "double"

    def test_pickup_datetime_type_is_timestamp(self, df_bronze_green):
        """Valida que pickup_datetime é do tipo timestamp."""
        result = mod.transform(df_bronze_green)
        assert dict(result.dtypes)["pickup_datetime"] == "timestamp"


# ─── Load ───────────────────────────────────────────────────────────────────
class TestLoad:

    def test_load_calls_parquet_write(self, spark, df_bronze_green):
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_bronze_green)

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)), \
             patch.object(spark, "sql"):
            mod.load(df, spark)

        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.partitionBy.assert_called_once_with("partition_year", "partition_month")

    def test_load_s3_path_correct(self, spark, df_bronze_green):
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_bronze_green)
        captured_path = {}

        def fake_parquet(path):
            captured_path["path"] = path

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.parquet.side_effect = fake_parquet

        with patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)), \
             patch.object(spark, "sql"):
            mod.load(df, spark)

        assert "table_green_taxi_silver" in captured_path.get("path", "")
        assert "silver" in captured_path.get("path", "")