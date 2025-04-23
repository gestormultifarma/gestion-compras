# analysis/transformer/transformer_ventas.py

import pandas as pd

# Diccionario de estandarización
columnas_estandarizadas = {
    "Codigo": "Codigo",
    "Nombre": "Nombre",
    "Sugerido": "Sugerido",
    "Laboratorio": "Laboratorio",
    "Caja": "Contenido-Caja",
    "Blister": "Contenido-Blister",
    "Unidad": "Contenido-Unidad",
    "Caja.1": "Inventario-Caja",
    "Blister.1": "Inventario-Blister",
    "Unidad.1": "Inventario-Unidad",
    "UltimaCompra": "Ultima-Compra",
    "Caja.2": "Venta-Caja",
    "Blister.2": "Venta-Blister",
    "Unidad.2": "Venta-Unidad",
    "Costo Unitario": "Costo-Unitario",
    "Precio Venta Contado": "Precio-Venta-Contado"
}

# Columnas esperadas tras el renombramiento
columnas_esperadas = list(columnas_estandarizadas.values())

def validar_columnas_ventas(df, archivo):
    actuales = df.columns.tolist()
    faltantes = [col for col in columnas_esperadas if col not in actuales]
    extras = [col for col in actuales if col not in columnas_esperadas]

    if faltantes:
        raise ValueError(f"❌ Faltan columnas en {archivo}: {faltantes}")
    if extras:
        print(f"ℹ️ Columnas adicionales no mapeadas en {archivo}: {extras}")

def transformar_archivo(path):
    try:
        # Leer el archivo sin encabezado
        df_raw = pd.read_excel(path, header=None, engine='openpyxl')

        # Tomar la segunda fila como encabezado
        new_header = df_raw.iloc[1].tolist()
        df = df_raw[2:].copy()  # Eliminar la fila 0 (descripción) y fila 1 (encabezado real)
        df.columns = new_header  # Asignar el encabezado correcto

        # Renombrar columnas con nombres estandarizados
        df.rename(columns=columnas_estandarizadas, inplace=True)

        # Validar estructura resultante
        validar_columnas_ventas(df, path)

        return df

    except Exception as e:
        raise RuntimeError(f"Error al transformar {path}: {str(e)}")
