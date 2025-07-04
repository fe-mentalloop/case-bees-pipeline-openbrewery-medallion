from pathlib import Path
import pandas as pd
import pytest
from src.gold import main as gold_main

BASE_DIR   = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "datalake" / "silver"
GOLD_DIR   = BASE_DIR / "datalake" / "gold"

def test_gold_aggregation():
    # prepara a camada Silver manualmente
    # limpa e recria pastas state=NY e state=CA
    (SILVER_DIR / "state=NY").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / "state=CA").mkdir(parents=True, exist_ok=True)
    # garante que gold/ exista (será preenchido pelo gold_main)
    (GOLD_DIR).mkdir(parents=True, exist_ok=True)

    # cria dois registros em NY e um em CA
    # inclui a coluna 'state', mas o gold_main irá descartá-la e reinjetar
    df_ny = pd.DataFrame([
        {"id": 1, "state": "NY", "brewery_type": "micro"},
        {"id": 2, "state": "NY", "brewery_type": "micro"},
    ])
    df_ca = pd.DataFrame([
        {"id": 3, "state": "CA", "brewery_type": "brewpub"},
    ])
    # grava como data.parquet em cada partição
    df_ny.to_parquet(SILVER_DIR / "state=NY" / "data.parquet", index=False)
    df_ca.to_parquet(SILVER_DIR / "state=CA" / "data.parquet", index=False)

    # executa a agregação
    gold_main()

    # arquivo de saída
    gold_file = GOLD_DIR / "breweries_agg.parquet"
    # deve existir e ser um arquivo
    assert gold_file.exists()
    assert gold_file.is_file()

    # lê o parquet de gold e verifica os counts
    df_out = pd.read_parquet(gold_file)
    results = {
        (row.state, row.brewery_type, row.brewery_count)
        for _, row in df_out.iterrows()
    }
    assert ("NY", "micro", 2) in results
    assert ("CA", "brewpub", 1) in results