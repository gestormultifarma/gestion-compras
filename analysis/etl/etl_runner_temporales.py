import os
import warnings

from analysis.extractor.extractor_temporales import ExtractorTemporales
from analysis.transformer.transformer_temporales import TransformadorTemporales
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_temporales import LoaderTemporales  # ✅ Loader específico
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_temporales(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"


def transformador(path):
    return TransformadorTemporales(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\temporal"
    extractor = ExtractorTemporales(directorio)
    logger = LoggerETL("ETL Temporales")
    loader = LoaderTemporales(logger=logger)
    archivos = extractor.extraer()
    # print(f"🗂️ Archivos encontrados: {archivos}")

"""     for archivo in archivos:
        print(f"\n🔍 Procesando archivo: {archivo}")
        try:
            df = transformador(archivo)
            # print(f"📋 Columnas del DataFrame: {df.columns.tolist()}")
            # print(f"📏 Registros: {len(df)}")
            clave = generar_clave_temporales(archivo)
            # print(f"🗝️ Clave generada: {clave}")

            loader.cargar(df, clave)  # ✅ Ya no lanza error

        except Exception as e:
            # print(f"❌ Error procesando {archivo}: {e}") """

runner = BaseETLRunner(
    directorio_raiz=directorio,
    extractor_func=extractor.extraer,
    transformer_func=transformador,
    clave_func=generar_clave_temporales,
    nombre_etl="ETL Temporales",
    logger=logger,
    loader=loader
)
runner.run()
