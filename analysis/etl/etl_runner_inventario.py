# analysis\etl\etl_runner_inventario.py

import os
import warnings
from analysis.extractor.extractor_inventario import extraer_archivos_inventario
from analysis.transformer.transformer_inventario import transformar_inventario

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

def run_etl_inventario():
    directorio_raiz = r"E:\desarrollo\gestionCompras\data\input\inventarios"
    dataframes = {}

    print(f"Iniciando ETL Inventarios desde: {directorio_raiz}")
    archivos = extraer_archivos_inventario(directorio_raiz)
    print(f"{len(archivos)} archivos encontrados para procesar.\n")

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')

        try:
            nombre_punto, codigo, *_ = nombre_archivo.rsplit(' ', 2)
            clave = f"{codigo}_{nombre_punto}"
        except ValueError:
            print(f"[❌ ERROR] Nombre de archivo inválido: {nombre_archivo}")
            continue

        try:
            df_limpio = transformar_inventario(archivo)
            dataframes[clave] = df_limpio
            print(f"[✅ OK] {clave} procesado")
        except Exception as e:
            print(f"[⚠️ ERROR] Falló la transformación de {archivo}: {e}")

    print("\n🎯 ETL Inventarios Finalizado.")
    print(f"✅ Total de archivos procesados exitosamente: {len(dataframes)}")
    print(f"📁 Claves disponibles: {list(dataframes.keys())}")

    return dataframes

# Permite ejecutar desde terminal con: python -m analysis.etl_runner_inventario
if __name__ == '__main__':
    run_etl_inventario()
