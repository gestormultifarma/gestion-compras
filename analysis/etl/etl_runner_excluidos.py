# analysis/etl/etl_runner_excluidos.py

import os
import warnings

from analysis.extractor.extractor_excluidos import ExtractorExcluidos
from analysis.transformer.transformer_excluidos import TransformadorExcluidos
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_excluidos import LoaderExcluidos
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_excluidos(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return f"stg_{nombre_archivo}"


def transformar(path):
    return TransformadorExcluidos(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\excluidos"
    extractor = ExtractorExcluidos(directorio)
    logger = LoggerETL("ETL Excluidos")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformar,
        clave_func=generar_clave_excluidos,
        nombre_etl="ETL Excluidos",
        logger=logger,
        loader = LoaderExcluidos(logger=logger)
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_excluidos