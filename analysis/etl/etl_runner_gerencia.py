# analysis/etl/etl_runner_gerencia.py

import os
import warnings

from analysis.extractor.extractor_gerencia import ExtractorGerencia
from analysis.transformer.transformer_gerencia import TransformadorGerencia
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_gerencia(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return nombre_archivo
    

def transformador(path):
    return TransformadorGerencia(path).transformar()


if __name__ == "__main__":
    directorio = r"E:\desarrollo\gestionCompras\data\input\gerencia"
    extractor = ExtractorGerencia(directorio)
    logger = LoggerETL("ETL Gerencia")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_gerencia,
        nombre_etl="ETL Gerencia",
        logger=logger
    )
    runner.run()

# prueba funcional: python -m analysis.etl.etl_runner_gerencia
