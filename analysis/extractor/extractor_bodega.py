# analysis/extractor/extractor_bodega.py

import os

def extraer_archivo_bodega(directorio):
    archivo_objetivo = "productos-solo-bodega.xlsx"
    path = os.path.join(directorio, archivo_objetivo)
    if os.path.exists(path):
        return path
    else:
        raise FileNotFoundError(f"⚠️ Archivo no encontrado en: {path}")
