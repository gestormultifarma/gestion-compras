# analysis/etl/etl_runner_inventario.py

import os
import warnings

from analysis.extractor.extractor_inventario import ExtractorInventario
from analysis.transformer.transformer_inventario import TransformadorInventario
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_inventario(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_punto, codigo, *_ = nombre_archivo.rsplit(' ', 2)
    return f"{codigo}_{nombre_punto}"


def transformador(path):
    return TransformadorInventario(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\inventarios"
    extractor = ExtractorInventario(directorio)
    logger = LoggerETL("ETL Inventarios")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_inventario,
        nombre_etl="ETL Inventarios",
        logger=logger
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_inventario