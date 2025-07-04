from pathlib import Path
import pandas as pd
import glob

BASE_DIR   = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "datalake" / "silver"
GOLD_DIR   = BASE_DIR / "datalake" / "gold"

def main():
    print("Repo root:", BASE_DIR)
    print("Coletando Parquet em:", SILVER_DIR)

    # Garante pasta gold
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    files = glob.glob(str(SILVER_DIR / "state=*" / "data.parquet"))
    if not files:
        print("Nenhum Parquet real encontrado em", SILVER_DIR)
        return
    
    print(files)

    # Concatena todos em um único DataFrame
    df = pd.concat((pd.read_parquet(p) for p in files), ignore_index=True)

    # Agrupa e conta
    agg = (
        df
        .groupby(["state", "brewery_type"], as_index=False)
        .size()
        .rename(columns={"size": "brewery_count"})
    )

    # Grava resultado
    out_path = GOLD_DIR / "breweries_agg.parquet"
    agg.to_parquet(out_path, index=False)

    print(f"Gold escrito em {out_path} ({len(agg)} linhas)")

if __name__ == "__main__":
    main()