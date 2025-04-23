# analysis/transformer/transformer_inventario.py

import pandas as pd

# Diccionario para estandarizar nombres de columnas
columnas_estandarizadas = {
    "Grupo": "Grupo",
    "Cod Producto": "Codigo",
    "Nombre": "Nombre",
    "Inventario Caja": "Inventario-Caja",
    "Inventario Blister": "Inventario-Blister",
    "Costo Unidad": "Costo-Unidad",
    "Costo Blister": "Costo-Blister",
    "Costo Caja": "Costo-Caja",
    "% Iva": "%-Iva",
    "Inventario Unidad": "Inventario-Unidad",
    "Costo Total": "Costo-Total",
    "Costo Total Iva": "Costo-Total-Iva",
    "Venta Total": "Venta-Total",
    "Ibua": "Ibua",
    "Icui": "Icui"
}

# Lista de columnas que deben estar presentes después de renombrar
columnas_esperadas = list(columnas_estandarizadas.values())

def validar_columnas_inventario(df, archivo):
    actuales = df.columns.tolist()
    faltantes = [col for col in columnas_esperadas if col not in actuales]
    extras = [col for col in actuales if col not in columnas_esperadas]

    if faltantes:
        raise ValueError(f"❌ Faltan columnas en {archivo}: {faltantes}")
    if extras:
        print(f"ℹ️ Columnas adicionales no mapeadas en {archivo}: {extras}")

def transformar_inventario(path):
    df = pd.read_excel(path, sheet_name=0, skiprows=0)

    # Renombrar las columnas según el diccionario
    columnas_actuales = df.columns
    columnas_renombrar = {col: columnas_estandarizadas[col] for col in columnas_actuales if col in columnas_estandarizadas}
    df.rename(columns=columnas_renombrar, inplace=True)

    # Asegurar que las columnas esperadas existan (si faltan, crear con NA)
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = pd.NA

    # Validar estructura final
    validar_columnas_inventario(df, path)

    return df
