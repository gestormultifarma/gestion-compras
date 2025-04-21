import os
from analysis.extractor import extraer_archivos
from analysis.transformer import transformar_archivo

def run_etl():
    directorio_raiz = r"E:\desarrollo\gestionCompras\data\input\ventas"
    dataframes = {}

    print(f"Iniciando ETL desde: {directorio_raiz}")
    archivos = extraer_archivos(directorio_raiz)
    print(f"{len(archivos)} archivos encontrados para procesar.\n")

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
        
        try:
            nombre_punto, codigo, mes = nombre_archivo.rsplit(' ', 2)
            mes = mes.replace('-', '')  # elimina el guion y deja solo el número
            clave = f"{codigo}_{nombre_punto}_{mes}"  # sin 'mes' como prefijo
        except ValueError:
            print(f"[❌ ERROR] Nombre de archivo inválido: {nombre_archivo}")
            continue

        try:
            df_limpio = transformar_archivo(archivo)
            dataframes[clave] = df_limpio
            print(f"[✅ OK] {clave} procesado")
        except Exception as e:
            print(f"[⚠️ ERROR] Falló la transformación de {archivo}: {e}")

    print("\n🎯 ETL Finalizado.")
    print(f"✅ Total de archivos procesados exitosamente: {len(dataframes)}")
    print(f"📁 Claves disponibles: {list(dataframes.keys())}")

    return dataframes

# Permite ejecutar desde terminal con: python -m analysis.etl_runner
if __name__ == '__main__':
    run_etl()
