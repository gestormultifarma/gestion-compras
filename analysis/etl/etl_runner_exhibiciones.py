# analysis/etl/etl_runner_exhibiciones.py

import os
import warnings

from analysis.extractor.extractor_exhibiciones import ExtractorExhibiciones
from analysis.transformer.transformer_exhibiciones import ExhibicionesTransformer
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_exhibiciones(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return nombre_archivo

def transformar_exhibiciones(path):
    return ExhibicionesTransformer(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\exhibiciones"
    extractor = ExtractorExhibiciones(directorio)

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformar_exhibiciones,
        clave_func=generar_clave_exhibiciones,
        nombre_etl="ETL Exhibiciones"
    )
    runner.run()


#prueba funcional: python -m analysis.etl.etl_runner_exhibiciones
