# analysis/transformer/transformer_bodega.py

import pandas as pd
from analysis.transformer.transformer_base import BaseTransformer

class BodegaTransformer(BaseTransformer):
    columnas_esperadas = ["Codigo"]

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.validar_columnas(self.columnas_esperadas)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df
