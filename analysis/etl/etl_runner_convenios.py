# analysis/etl/etl_runner_convenios.py

import os
import warnings

from analysis.extractor.extractor_convenios import ExtractorConvenios
from analysis.transformer.transformer_convenios import ConveniosTransformer
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_convenios import LoaderConvenios
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_convenio(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return f"stg_{nombre_archivo}"

def transformador(path):
    return ConveniosTransformer(path).transformar()


if __name__ == "__main__":
    directorio = r"E:\desarrollo\gestionCompras\data\input\convenios"
    extractor = ExtractorConvenios(directorio)
    logger = LoggerETL("ETL Convenios")
    loader = BaseLoader(db_name="gestion_compras")


    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_convenio,
        nombre_etl="ETL Convenios",
        logger=logger,
        loader=LoaderConvenios(logger=logger)

    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_convenios
