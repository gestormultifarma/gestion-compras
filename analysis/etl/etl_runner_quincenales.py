# analysis/etl/etl_runner_quincenales.py

import os
import warnings

from analysis.extractor.extractor_quincenales import ExtractorQuincenales
from analysis.transformer.transformer_quincenales import TransformadorQuincenales
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_quincenales(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"
    

def transformador(path):
    return TransformadorQuincenales(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\quincenales"
    extractor = ExtractorQuincenales(directorio)
    logger = LoggerETL("ETL Quincenales")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_quincenales,
        nombre_etl="ETL Quincenales",
        logger=logger,
        loader=loader
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_quincenales