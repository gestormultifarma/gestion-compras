# analysis\etl\etl_runner_ventas.py

import os
import warnings

from analysis.extractor.extractor_ventas import ExtractorVentas
from analysis.transformer.transformer_ventas import VentasTransformer
from analysis.etl.etl_base import BaseETLRunner

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')


def generar_clave_ventas(archivo):
    nombre_archivo = os.path.basename(archivo).replace('.xlsx', '').replace('.xls', '')
    nombre_punto, codigo, mes = nombre_archivo.rsplit(' ', 2)
    mes = mes.replace('-', '')
    return f"{codigo}_{nombre_punto}_{mes}"

def transformador(path):
    instancia = VentasTransformer(path)
    return instancia.transformar()

if __name__ == '__main__':
    directorio = r"E:\desarrollo\gestionCompras\data\input\ventas"
    extractor = ExtractorVentas(directorio)

    runner = BaseETLRunner(
        directorio_raiz=directorio,
        extractor_func=extractor.extraer,
        transformer_func=transformador,
        clave_func=generar_clave_ventas,
        nombre_etl="ETL Ventas"
    )
    runner.run()


# prueba funcional: python -m analysis.etl.etl_runner_ventas