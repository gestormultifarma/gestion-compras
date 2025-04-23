# analysis\transformer\transformer_convenios.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd

class ConveniosTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "Codigo": "Codigo",
        "Punto-de-Venta": "Punto-de-Venta",
        "Min-Convenio": "Min-Convenio",
        "Max-Convenio": "Max-Convenio"
    }

    columnas_esperadas = list(columnas_estandarizadas.values())

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.validar_columnas(self.columnas_esperadas)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df
