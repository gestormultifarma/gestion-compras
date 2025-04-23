# analysis/transformer/transformer_bodega.py

import pandas as pd

def transformar_bodega(path):
    df = pd.read_excel(path, sheet_name=0)

    if "Codigo" not in df.columns:
        raise ValueError("❌ La columna 'Codigo' no se encuentra en el archivo.")

    df = df[["Codigo"]].dropna()
    df["Codigo"] = df["Codigo"].astype(str).str.strip()

    return df
