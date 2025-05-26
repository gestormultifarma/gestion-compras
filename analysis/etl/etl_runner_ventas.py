# analysis/etl/etl_runner_ventas.py

import os
import warnings

from analysis.extractor.extractor_ventas import ExtractorVentas
from analysis.transformer.transformer_ventas import VentasTransformer
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_ventas import LoaderVentas      
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_ventas(archivo):
    nombre_archivo = os.path.basename(archivo) \
        .replace('.xlsx', '') \
        .replace('.xls', '')
    nombre_punto, codigo, mes = nombre_archivo.rsplit(' ', 2)
    mes = mes.replace('-', '')
    clave_utilizar = nombre_punto.strip().lower().replace(' ', '_')
    return f"stg_rotacion_de_{clave_utilizar}_{codigo}_{mes}"


def transformador(path):
    return VentasTransformer(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\ventas"
    extractor = ExtractorVentas(directorio)
    logger = LoggerETL("ETL Ventas")
    loader = LoaderVentas(logger=logger)   

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_ventas,
        nombre_etl="ETL Ventas",
        logger=logger,
        loader=loader
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_ventas

