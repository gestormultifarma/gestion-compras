# analysis/extractor_ventas.py

import os

def extraer_archivos(directorio_raiz):
    archivos = []

    for subcarpeta in os.listdir(directorio_raiz):
        ruta_subcarpeta = os.path.join(directorio_raiz, subcarpeta)
        if os.path.isdir(ruta_subcarpeta):
            for archivo in os.listdir(ruta_subcarpeta):
                # Ignorar archivos temporales que empiezan con '~$'
                if (archivo.endswith('.xlsx') or archivo.endswith('.xls')) and not archivo.startswith('~$'):
                    ruta_completa = os.path.join(ruta_subcarpeta, archivo)
                    archivos.append(ruta_completa)

    return archivos
