# analysis/etl/etl_runner_oferta.py

import os

from analysis.extractor.extractor_oferta import ExtractorOferta
from analysis.transformer.transformer_oferta import TransformadorOferta
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_oferta import LoaderOferta
from utils.logger_etl import LoggerETL


def generar_clave_oferta(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_ofertas_{nombre_archivo}"


def transformador(path):
    return TransformadorOferta(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\oferta"
    extractor = ExtractorOferta(directorio)
    logger = LoggerETL("ETL Ofertas")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_oferta,
        nombre_etl="ETL Ofertas",
        logger=logger,
        loader = LoaderOferta(logger=logger)
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_oferta
