import os
import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent 
RAW_PATH = BASE_DIR / "datalake" / "bronze"

print(RAW_PATH)

def fetch_all_breweries(page_size=50):
    url = "https://api.openbrewerydb.org/v1/breweries"
    page = 1
    all_data = []
    while True:
        resp = requests.get(url, params={"page": page, "per_page": page_size}, timeout=(3, 10))
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_data.extend(batch)
        page += 1
    return all_data

def main():
    # Garante que /bronze exista
    RAW_PATH.mkdir(parents=True, exist_ok=True)

    # Busca informações
    data = fetch_all_breweries()

    # Prepara DataFrame e marca timestamp
    df = pd.DataFrame(data)
    df["ingest_timestamp"] = datetime.now().isoformat()

    # Define arquivo de saída
    filename = f"breweries_{datetime.now():%Y%m%d_%H%M%S}.json"
    out = os.path.join(RAW_PATH, filename)

    # Grava JSON
    df.to_json(out, orient="records", lines=True, date_format="iso")
    print("Bronze escrito em", out)

if __name__ == "__main__":
    main()