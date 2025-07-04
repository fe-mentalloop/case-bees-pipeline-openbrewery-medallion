import glob
from pathlib import Path
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "datalake" / "bronze"
SILVER_DIR = BASE_DIR / "datalake" / "silver"

def main():
    # 1) Garante a pasta silver
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # 2) Acha todos os JSON Lines na camada Bronze
    files = glob.glob(str(BRONZE_DIR / "*.json"))
    if not files:
        print("Nenhum JSON encontrado em", BRONZE_DIR)
        return

    # 3) Carrega tudo num DataFrame e limpa
    df = pd.concat((pd.read_json(f, lines=True) for f in files), ignore_index=True)
    df = df.dropna(subset=["state"])
    if df.empty:
        print("Nenhum registro com state válido; abortando")
        return

    # 4) Para cada state, cria a subpasta e escreve um único data.parquet
    for state, group in df.groupby("state"):
        part_dir = SILVER_DIR / f"state={state}"
        part_dir.mkdir(parents=True, exist_ok=True)
        data = group.drop(columns=["state"])
        out = part_dir / "data.parquet"
        data.to_parquet(out, index=False)
        print(f"Wrote {len(data)} rows to {out}")
        
    print("Silver layer completa.")

if __name__ == "__main__":
    main()