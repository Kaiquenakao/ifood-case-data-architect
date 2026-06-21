"""Fixtures compartilhadas entre todos os testes do projeto.

Este módulo centraliza os dados fake utilizados nos testes de transform
das camadas Bronze, Silver e Gold.
"""

import pytest
from io import BytesIO

import pandas as pd


# ─── Fixtures Bronze ─────────────────────────────────────────────────────────
@pytest.fixture
def raw_yellow_parquet():
    """Retorna um buffer BytesIO com dados fake do Yellow Taxi em formato Parquet.

    Simula o arquivo baixado da CDN do NYC TLC pelo extract do Bronze.
    Contém colunas com nomes em maiúsculo para validar a normalização para lowercase.

    Returns:
        BytesIO com o conteúdo Parquet dos dados fake do Yellow Taxi.
    """
    df = pd.DataFrame(
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
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


@pytest.fixture
def raw_green_parquet():
    """Retorna um buffer BytesIO com dados fake do Green Taxi em formato Parquet.

    Simula o arquivo baixado da CDN do NYC TLC pelo extract do Bronze.

    Returns:
        BytesIO com o conteúdo Parquet dos dados fake do Green Taxi.
    """
    df = pd.DataFrame(
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
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


@pytest.fixture
def empty_parquet():
    """Retorna um buffer BytesIO vazio para testar o caso de arquivo vazio no Bronze.

    Returns:
        BytesIO vazio simulando falha no download.
    """
    return BytesIO(b"")
