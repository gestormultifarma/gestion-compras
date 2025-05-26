import os
import warnings

from analysis.extractor.extractor_transferencias import ExtractorTransferencias
from analysis.transformer.transformer_transferencias import TransformadorTransferencias
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_transferencias import LoaderTransferencias
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_transferencias(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_{nombre_archivo}"


def transformador(path):
    return TransformadorTransferencias(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\transferencias"
    extractor = ExtractorTransferencias(directorio)
    logger = LoggerETL("ETL Transferencias")
    loader = LoaderTransferencias(logger=logger)

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_transferencias,
        nombre_etl="ETL Transferencias",
        logger=logger,
        loader=loader
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_transferencias
