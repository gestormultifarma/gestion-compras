# analysis/etl/etl_runner_excluidos.py

import os
import warnings

from analysis.extractor.extractor_excluidos import ExtractorExcluidos
from analysis.transformer.transformer_excluidos import TransformadorExcluidos
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_excluidos(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    return nombre_archivo

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\excluidos"
    extractor = ExtractorExcluidos(directorio)
    
    def transformar(path):
        return TransformadorExcluidos(path).transformar()

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformar,
        clave_func=generar_clave_excluidos,
        nombre_etl="ETL Excluidos"
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_excluidos