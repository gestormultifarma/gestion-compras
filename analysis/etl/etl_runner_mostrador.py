# analysis/etl/etl_runner_mostrador.py

import os
import warnings

from analysis.extractor.extractor_mostrador import ExtractorMostrador
from analysis.transformer.transformer_mostrador import TransformadorMostrador
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_mostrador import LoaderMostrador
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_mostrador(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"


def transformador(path):
    return TransformadorMostrador(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\mostrador"
    extractor = ExtractorMostrador(directorio)
    logger = LoggerETL("ETL Mostrador")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_mostrador,
        nombre_etl="ETL Mostrador",
        logger=logger,
        loader = LoaderMostrador(logger=logger)
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_mostrador