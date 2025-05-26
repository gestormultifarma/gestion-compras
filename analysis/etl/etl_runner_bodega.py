# analysis/etl/etl_runner_bodega.py

import os
import warnings

from analysis.extractor.extractor_bodega import ExtractorBodega
from analysis.transformer.transformer_bodega import BodegaTransformer
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_bodega import LoaderBodega
from utils.logger_etl import LoggerETL


warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_bodega(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return f"stg_{nombre_archivo}"


def transformador(path):
    return BodegaTransformer(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\bodega"
    extractor = ExtractorBodega(directorio)
    logger = LoggerETL("ETL Bodega")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_bodega,
        nombre_etl="ETL Bodega",
        logger=logger,
        loader=LoaderBodega(logger=logger)
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_bodega