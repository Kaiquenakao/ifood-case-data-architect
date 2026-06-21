"""Testes unitários para o etl_avg_passengers_gold.

Cobre transform e load do pipeline Gold de média de passageiros por hora.
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

import src.gold.etl_avg_passengers_gold as mod


@pytest.fixture(scope="module")
def spark():
    """Cria e retorna uma SparkSession local para os testes.

    Returns:
        SparkSession configurada em modo local.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-avg-passengers-gold")
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
        (1, 4, 40.0, "2023-05-10 14:00:00", "2023-05-10 14:30:00", "yellow", 2023, 5),
        (2, 2, 14.0, "2023-05-10 07:00:00", "2023-05-10 07:30:00", "green",  2023, 5),  # hora 7
        (1, 3, 22.0, "2023-05-15 08:00:00", "2023-05-15 08:45:00", "green",  2023, 5),  # hora 8
        (2, 5, 30.0, "2023-05-20 14:00:00", "2023-05-20 14:30:00", "green",  2023, 5),  # hora 14
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
        """Valida que o resultado contém apenas as colunas hora e avg_passenger_count."""
        result = mod.transform(df_all_taxi_gold)
        assert set(result.columns) == {"hora", "avg_passenger_count"}

    def test_filtra_apenas_maio(self, df_all_taxi_gold):
        """Valida que apenas corridas de maio são consideradas."""
        result = mod.transform(df_all_taxi_gold)
        horas = sorted([r["hora"] for r in result.select("hora").collect()])
        assert horas == [7, 8, 14]

    def test_agrupa_por_hora(self, df_all_taxi_gold):
        """Valida que o resultado tem um registro por hora do dia."""
        result = mod.transform(df_all_taxi_gold)
        horas = [r["hora"] for r in result.select("hora").collect()]
        assert len(horas) == len(set(horas))

    def test_ordenado_por_hora(self, df_all_taxi_gold):
        """Valida que o resultado está ordenado crescentemente por hora."""
        result = mod.transform(df_all_taxi_gold)
        horas = [r["hora"] for r in result.select("hora").collect()]
        assert horas == sorted(horas)

    def test_media_calculada_corretamente(self, df_all_taxi_gold):
        """Valida que a média de passageiros às 14h de maio está correta."""
        result = mod.transform(df_all_taxi_gold)
        hora14 = result.filter(result["hora"] == 14).collect()[0]
        # maio hora 14: yellow(4) + green(5) → avg = 4.5
        assert hora14["avg_passenger_count"] == 4.5

    def test_media_arredondada_2_casas(self, df_all_taxi_gold):
        """Valida que avg_passenger_count tem no máximo 2 casas decimais."""
        result = mod.transform(df_all_taxi_gold)
        for row in result.collect():
            valor = row["avg_passenger_count"]
            assert round(valor, 2) == valor

    def test_hora_valida_entre_0_e_23(self, df_all_taxi_gold):
        """Valida que todas as horas no resultado estão no intervalo 0-23."""
        result = mod.transform(df_all_taxi_gold)
        horas = [r["hora"] for r in result.select("hora").collect()]
        assert all(0 <= h <= 23 for h in horas)

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

        assert "table_avg_passengers_gold" in captured_path.get("path", "")
        assert "gold" in captured_path.get("path", "")
