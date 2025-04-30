import os
import warnings

from analysis.extractor.extractor_correos_laboratorios import ExtractorCorreosLaboratorios
from analysis.transformer.transformer_correos_laboratorios import TransformadorCorreosLaboratorios
from analysis.etl.etl_base import BaseETLRunner
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_correos_laboratorios(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"{nombre_archivo}_correos"


def transformador(path):
    return TransformadorCorreosLaboratorios(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\transferencistas"
    extractor = ExtractorCorreosLaboratorios(directorio)
    logger = LoggerETL("ETL Correos Laboratorios")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_correos_laboratorios,
        nombre_etl="ETL Correos Laboratorios",
        logger=logger
    )
    runner.run()

# Prueba funcional: python -m analysis.etl.etl_runner_correos_laboratorios
