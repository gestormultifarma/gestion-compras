# analysis/etl/etl_runner_ecommerce.py

import os
import warnings

from analysis.extractor.extractor_ecommerce import ExtractorEcommerce
from analysis.transformer.transformer_ecommerce import EcommerceTransformer
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_ecommerce(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return nombre_archivo


def transformador(path):
    return EcommerceTransformer(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\e-commerce"
    extractor = ExtractorEcommerce(directorio)
    logger = LoggerETL("ETL Ecommerce")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_ecommerce,
        nombre_etl="ETL Ecommerce",
        logger=logger
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_ecommerce