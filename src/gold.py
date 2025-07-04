from pathlib import Path
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "datalake" / "silver"
GOLD_DIR   = BASE_DIR / "datalake" / "gold"

def main():
    # Garante que gold/ exista
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Encontra só os arquivos data.parquet de cada partição
    parts = list(SILVER_DIR.glob("state=*/data.parquet"))
    if not parts:
        print(f"Nenhuma partição em {SILVER_DIR}")
        return

    # Lê cada partição e injeta 'state' pelo nome da pasta, para evitar conflitos.
    dfs = []
    for p in parts:
        df = pd.read_parquet(p)
        state = p.parent.name.split("=", 1)[1] 
        df["state"] = state                   
        dfs.append(df)

    # Concatena tudo num único DataFrame
    df_all = pd.concat(dfs, ignore_index=True)

    # Agrupa por state e brewery_type
    agg = (
        df_all
        .groupby(["state", "brewery_type"], as_index=False)
        .size()
        .rename(columns={"size": "brewery_count"})
    )

    # Grava o Parquet agregado
    out = GOLD_DIR / "breweries_agg.parquet"
    agg.to_parquet(out, index=False)
    print(f"Gold escrito em {out} ({len(agg)} linhas)")

if __name__ == "__main__":
    main()