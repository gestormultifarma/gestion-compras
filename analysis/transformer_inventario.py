# analysis\transformer_inventario.py

import pandas as pd

columnas_estandarizadas = {
    "Grupo": "Grupo",
    "Cod Producto": "Codigo",
    "Nombre": "Nombre",
    "Inventario Caja": "Inventario-Caja",
    "Inventario Blister": "Inventario-Blister",
    "Costo Unidad": "Inventario-Unidad",
    "Costo Blister": "Costo-Blister",
    "Costo Caja": "Costo-Caja",
    "% Iva": "%Iva",
    "Inventario Unidad": "Inventario-Unidad",
    "Costo Total": "Costo-Total",
    "Costo Total Iva": "Costo-Total-Iva",
    "Venta Total": "Venta-Total",
    "Ibua": "Ibua",
    "Icui": "Icui"
}

def transformar_inventario(path):
    df = pd.read_excel(path, sheet_name=0, skiprows=0)
    columnas_actuales = df.columns
    columnas_renombrar = {col: columnas_estandarizadas[col] for col in columnas_actuales if col in columnas_estandarizadas}
    df.rename(columns=columnas_renombrar, inplace=True)

    for col in columnas_estandarizadas.values():
        if col not in df.columns:
            df[col] = pd.NA

    return df
