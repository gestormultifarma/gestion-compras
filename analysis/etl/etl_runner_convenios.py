# analysis/etl/etl_runner_convenios.py

import warnings

from analysis.extractor.extractor_convenios import ExtractorConvenios
from analysis.transformer.transformer_convenios import ConveniosTransformer
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_convenio(archivo):
    return "convenios"

if __name__ == "__main__":
    directorio = r"E:\desarrollo\gestionCompras\data\input\convenios"
    extractor = ExtractorConvenios(directorio)
    
    def transformador(path):
        return ConveniosTransformer(path).transformar()

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_convenio,
        nombre_etl="ETL Convenios"
    )
    runner.run()


#prueba funcional: python -m analysis.etl.etl_runner_convenios