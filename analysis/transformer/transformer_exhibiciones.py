# analysis/transformer/transformer_exhibiciones.py

import pandas as pd
from analysis.transformer.transformer_base import BaseTransformer

class ExhibicionesTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "Codigo": "codigo",
        "Punto-de-Venta": "punto_de_venta",
        "Min-Exhibicion": "min_exhibicion",
        "Max-Exhibicion": "max_exhibicion"
    }
    columnas_esperadas = list(columnas_estandarizadas.values())

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="openpyxl")
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.validar_columnas(self.columnas_esperadas)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df
