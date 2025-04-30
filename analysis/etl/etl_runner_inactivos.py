import os
import warnings

from analysis.extractor.extractor_inactivos import ExtractorInactivos
from analysis.transformer.transformer_inactivos import TransformadorInactivos
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_inactivos(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"{nombre_archivo}_inactivos"


def transformador(path):
    return TransformadorInactivos(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\estado-inactivo"
    extractor = ExtractorInactivos(directorio)
    logger = LoggerETL("ETL Inactivos")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_inactivos,
        nombre_etl="ETL Inactivos",
        logger=logger
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_inactivos