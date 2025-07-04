import json
from pathlib import Path
import pandas as pd
import pytest
from src.silver import main as silver_main

BASE_DIR   = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "datalake" / "bronze"
SILVER_DIR = BASE_DIR / "datalake" / "silver"

def test_silver_partitions_and_parquet(tmp_path):
    # cria raw JSON com 3 registros (um sem state)
    BRONZE_DIR.mkdir(parents=True)
    data = [
        {"id":1, "name":"A", "brewery_type":"micro",  "city":"X", "state":"CA", "country":"US"},
        {"id":2, "name":"B", "brewery_type":"brewpub","city":"Y", "state":"NY", "country":"US"},
        {"id":3, "name":"C", "brewery_type":"micro",  "city":"Z", "state":None,"country":"US"},
    ]
    f = BRONZE_DIR / "sample.json"
    with f.open("w", encoding="utf-8") as fp:
        for rec in data:
            fp.write(json.dumps(rec) + "\n")

    # roda transformação
    silver_main()

    # espera dirs partitionados
    assert (SILVER_DIR/"state=CA").is_dir()
    assert (SILVER_DIR/"state=NY").is_dir()

    # lê tudo via pandas (aponta para o dir root)
    df = pd.read_parquet(SILVER_DIR).astype(str)
    print(df)
    assert len(df)==2
    assert set(df["state"])=={"CA","NY"}