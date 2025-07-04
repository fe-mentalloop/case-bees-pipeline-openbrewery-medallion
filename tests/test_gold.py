from pathlib import Path
import pandas as pd
import pytest
from src.gold import main as gold_main

BASE_DIR = Path(__file__).resolve().parent.parent 
GOLD_DIR = BASE_DIR / "datalake" / "gold"
SILVER_DIR = BASE_DIR / "datalake" / "silver"

def test_gold_aggregation(tmp_path):
    # Prepara as pastas de partição na camada Silver
    #    garantindo que existam state=NY e state=CA
    (SILVER_DIR/"state=NY").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR/"state=CA").mkdir(parents=True, exist_ok=True)

    # Monta DataFrames de exemplo:
    #    - Dois registros para NY, ambos brewery_type="micro"
    #    - Um registro para CA, brewery_type="brewpub"
    df_ny = pd.DataFrame([
        {"id": 1, "state": "NY", "brewery_type": "micro"},
        {"id": 2, "state": "NY", "brewery_type": "micro"},
    ])
    df_ca = pd.DataFrame([
        {"id": 3, "state": "CA", "brewery_type": "brewpub"},
    ])

    # Grava cada DataFrame como data.parquet dentro da partição correspondente
    df_ny.to_parquet(SILVER_DIR/"state=NY"/"data.parquet", index=False)
    df_ca.to_parquet(SILVER_DIR/"state=CA"/"data.parquet", index=False)

    # Executa a função principal do Gold lay
    gold_main()

    # Verifica se o arquivo de saída foi criado
    gold_file = GOLD_DIR / "breweries_agg.parquet"
    assert gold_file.exists()

    # Lê o parquet gerado e coleta resultados em um set de tuplas
    df_out = pd.read_parquet(gold_file)
    vals = {
        (row.state, row.brewery_type, row.brewery_count)
        for _, row in df_out.iterrows()
    }

    # Checa que as contagens estão corretas
    assert ("NY", "micro", 2) in vals
    assert ("CA", "brewpub", 1) in vals