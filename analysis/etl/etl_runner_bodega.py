# analysis/etl/etl_runner_bodega.py

import warnings

from analysis.extractor.extractor_bodega import ExtractorBodega
from analysis.transformer.transformer_bodega import BodegaTransformer
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_bodega(archivo):
    return "solo_bodega"

def transformador(path):
    return BodegaTransformer(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\bodega"
    extractor = ExtractorBodega(directorio)

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_bodega,
        nombre_etl="ETL Bodega"
    )
    runner.run()

#prueba funcional: python -m analysis.etl.etl_runner_bodega