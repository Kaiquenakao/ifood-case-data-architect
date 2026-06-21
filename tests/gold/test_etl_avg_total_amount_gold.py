"""Testes unitários para o etl_avg_total_amount_gold.

Cobre transform e load do pipeline Gold de média do valor total por mês.
Utiliza PySpark local para testar as transformações e mock para o load no S3.

O extract não é testado pois lê diretamente do S3 em produção.
O load é mockado via unittest.mock pois escreve diretamente no S3 via PySpark.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

import src.gold.etl_avg_total_amount_gold as mod


@pytest.fixture(scope="module")
def spark():
    """Cria e retorna uma SparkSession local para os testes.

    Returns:
        SparkSession configurada em modo local.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-avg-total-amount-gold")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def df_all_taxi_gold(spark):
    """Retorna um DataFrame Spark simulando a tabela table_all_taxi_gold.

    Args:
        spark: SparkSession fixture.

    Returns:
        DataFrame Spark com schema Gold consolidado.
    """
    data = [
        (1, 2, 15.0, "2023-01-01 08:00:00", "2023-01-01 08:30:00", "yellow", 2023, 1),
        (2, 1, 25.0, "2023-02-15 09:00:00", "2023-02-15 09:45:00", "yellow", 2023, 2),
        (1, 3, 35.0, "2023-03-10 10:00:00", "2023-03-10 10:30:00", "yellow", 2023, 3),
        (2, 2, 20.0, "2023-04-05 11:00:00", "2023-04-05 11:30:00", "yellow", 2023, 4),
        (1, 4, 40.0, "2023-05-10 14:00:00", "2023-05-10 14:30:00", "yellow", 2023, 5),
        (2, 2, 14.0, "2023-01-10 07:00:00", "2023-01-10 07:30:00", "green",  2023, 1),
        (1, 3, 22.0, "2023-05-15 08:00:00", "2023-05-15 08:45:00", "green",  2023, 5),
    ]
    return spark.createDataFrame(
        data,
        ["VendorID", "passenger_count", "total_amount",
         "tpep_pickup_datetime", "tpep_dropoff_datetime",
         "taxi_type", "partition_year", "partition_month"],
    )


# ─── Transform ──────────────────────────────────────────────────────────────
class TestTransform:

    def test_colunas_esperadas_presentes(self, df_all_taxi_gold):
        """Valida que o resultado contém apenas as colunas mes e avg_total_amount."""
        result = mod.transform(df_all_taxi_gold)
        assert set(result.columns) == {"mes", "avg_total_amount"}

    def test_filtra_apenas_yellow(self, df_all_taxi_gold):
        """Valida que apenas corridas do Yellow Taxi são consideradas na média."""
        result = mod.transform(df_all_taxi_gold)
        assert result.count() == 5

    def test_agrupa_por_mes(self, df_all_taxi_gold):
        """Valida que o resultado tem um registro por mês."""
        result = mod.transform(df_all_taxi_gold)
        meses = [r["mes"] for r in result.select("mes").collect()]
        assert len(meses) == len(set(meses))

    def test_ordenado_por_mes(self, df_all_taxi_gold):
        """Valida que o resultado está ordenado crescentemente por mês."""
        result = mod.transform(df_all_taxi_gold)
        meses = [r["mes"] for r in result.select("mes").collect()]
        assert meses == sorted(meses)

    def test_media_calculada_corretamente(self, df_all_taxi_gold):
        """Valida que a média de total_amount do mês 1 (yellow) está correta."""
        result = mod.transform(df_all_taxi_gold)
        mes1 = result.filter(result["mes"] == 1).collect()[0]
        assert mes1["avg_total_amount"] == 15.0

    def test_media_arredondada_2_casas(self, df_all_taxi_gold):
        """Valida que avg_total_amount tem no máximo 2 casas decimais."""
        result = mod.transform(df_all_taxi_gold)
        for row in result.collect():
            valor = row["avg_total_amount"]
            assert round(valor, 2) == valor

    def test_resultado_nao_vazio(self, df_all_taxi_gold):
        """Valida que o resultado possui ao menos um registro."""
        result = mod.transform(df_all_taxi_gold)
        assert result.count() > 0


# ─── Load ───────────────────────────────────────────────────────────────────
class TestLoad:

    def test_load_calls_parquet_write(self, df_all_taxi_gold):
        """Valida que o load chama o writer Parquet sem particionamento."""
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_all_taxi_gold)

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer

        with patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)):
            mod.load(df)

        mock_writer.mode.assert_called_once_with("overwrite")

    def test_load_s3_path_correct(self, df_all_taxi_gold):
        """Valida que o path S3 do load contém a tabela Gold correta."""
        mod.BUCKET_NAME = "mock-bucket"
        df = mod.transform(df_all_taxi_gold)
        captured_path = {}

        def fake_parquet(path):
            captured_path["path"] = path

        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.parquet.side_effect = fake_parquet

        with patch.object(df.__class__, "coalesce", return_value=MagicMock(write=mock_writer)):
            mod.load(df)

        assert "table_avg_total_amount_gold" in captured_path.get("path", "")
        assert "gold" in captured_path.get("path", "")
