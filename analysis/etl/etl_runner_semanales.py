import os
import warnings

from analysis.extractor.extractor_semanales import ExtractorSemanales
from analysis.transformer.transformer_semanales import TransformadorSemanales
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_semanales(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"


def transformador(path):
    return TransformadorSemanales(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\semanales"
    extractor = ExtractorSemanales(directorio)
    logger = LoggerETL("ETL Semanales")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_semanales,
        nombre_etl="ETL Semanales",
        logger=logger,
        loader=loader
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_semanales
