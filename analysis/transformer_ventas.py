# analysis/transformer_ventas.py

import pandas as pd

columnas_esperadas = [
    "Codigo", "Nombre", "Sugerido", "Laboratorio",
    "Contenido-Caja", "Contenido-Blister", "Contenido-Unidad",
    "Inventario-Caja", "Inventario-Blister", "Inventario-Unidad",
    "UltimaCompra", "Venta-Caja", "Venta-Blister", "Venta-Unidad",
    "Costo-Unitario", "Precio-Venta-Contado"
]

def transformar_archivo(path):
    try:
        df = pd.read_excel(path, skiprows=1, engine='openpyxl')
        columnas_originales = df.columns.tolist()

        if len(columnas_originales) != len(columnas_esperadas):
            raise ValueError(f"Columnas inesperadas en archivo: {path}")

        renombradas = {col: nuevo for col, nuevo in zip(columnas_originales, columnas_esperadas)}
        df.rename(columns=renombradas, inplace=True)
        return df

    except Exception as e:
        raise RuntimeError(f"Error al transformar {path}: {str(e)}")
