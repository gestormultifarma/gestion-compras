# analysis/etl/etl_runner_ecommerce.py

import warnings

from analysis.extractor.extractor_ecommerce import ExtractorEcommerce
from analysis.transformer.transformer_ecommerce import EcommerceTransformer
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_ecommerce(archivo):
    return "maestra_ecommerce"

def transformador(path):
    return EcommerceTransformer(path).transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\e-commerce"
    extractor = ExtractorEcommerce(directorio)  # ✅ corregido aquí

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_ecommerce,
        nombre_etl="ETL Ecommerce"
    )
    runner.run()
