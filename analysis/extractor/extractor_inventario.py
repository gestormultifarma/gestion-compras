# analysis\extractor\extractor_inventario.py

import os

def extraer_archivos_inventario(directorio_raiz):
    archivos = []

    for archivo in os.listdir(directorio_raiz):
        if archivo.lower().endswith('.xlsx') and 'costeados' in archivo.lower():
            ruta_completa = os.path.join(directorio_raiz, archivo)
            archivos.append(ruta_completa)

    return archivos
