# analysis\etl\etl_runner_maestra_inventario.py

import os
import warnings

from analysis.extractor.extractor_maestra_inventario import ExtractorMaestraInventario
from analysis.transformer.transformer_maestra_inventario import TransformadorMaestraInventario
from analysis.etl.etl_base import BaseETLRunner
from analysis.loader.loader_base import BaseLoader
from analysis.loader.loader_maestra_inventario import LoaderMaestraInventario
from utils.logger_etl import LoggerETL

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_maestra_inventario(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_archivo = nombre_archivo.replace(' ', '_').lower()
    return f"stg_maestra_pdv_{nombre_archivo}"


def transformador(path):
    return TransformadorMaestraInventario(path).transformar()


if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\maestras_inventarios"
    extractor = ExtractorMaestraInventario(directorio)
    logger = LoggerETL("ETL Maestras Inventarios")
    loader = BaseLoader(db_name="gestion_compras")

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_maestra_inventario,
        nombre_etl="ETL Maestras Inventarios",
        logger=logger,
        loader = LoaderMaestraInventario(logger=logger)
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_maestra_inventario