import os
import warnings

from analysis.extractor.extractor_temporales import ExtractorTemporales
from analysis.transformer.transformer_temporales import TransformadorTemporales
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_temporales(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"{nombre_archivo}_temporales"


def transformador(path):
    return TransformadorTemporales(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\temporal"
    extractor = ExtractorTemporales(directorio)
    logger = LoggerETL("ETL Temporales")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_temporales,
        nombre_etl="ETL Temporales",
        logger=logger
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_temporales
