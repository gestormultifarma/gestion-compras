# analysis/etl/etl_runner_merchandising.py

import os
import warnings

from analysis.extractor.extractor_merchandising import ExtractorMerchandising
from analysis.transformer.transformer_merchandising import TransformadorMerchandising
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_merchandising import LoaderMerchandising 
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_merchandising(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"


def transformador(path):
    return TransformadorMerchandising(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\merchandising"
    extractor = ExtractorMerchandising(directorio)
    logger = LoggerETL("ETL Merchandising")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_merchandising,
        nombre_etl="ETL Merchandising",
        logger=logger,
        loader=LoaderMerchandising(logger=logger)
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_merchandising
