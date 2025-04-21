# gestionCompras/analysis/actualizador.py

import os

def procesar_archivos_excel(directorio_raiz: str):
    archivos_procesados = []

    for carpeta, _, archivos in os.walk(directorio_raiz):
        for archivo in archivos:
            if archivo.endswith('.xlsx') or archivo.endswith('.xls'):
                ruta_archivo = os.path.join(carpeta, archivo)
                
                # Aquí puedes incluir la lógica de transformación en el futuro
                archivos_procesados.append(ruta_archivo)

    return archivos_procesados
