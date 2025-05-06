from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd
from datetime import datetime

class TransformadorTemporales(BaseTransformer):
    columnas_estandarizadas = {
        "Codigo": "codigo",
        "Punto-de-Venta": "punto_de_venta",
        "Min-temporales": "min_temporales",
        "Max-temporales": "max_temporales",
        "Fecha-Inicial": "fecha_inicial",
        "Fecha-Final": "fecha_final"
    }

    columnas_esperadas = list(columnas_estandarizadas.values())

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.agregar_columnas_faltantes(self.columnas_esperadas)
        self.validar_columnas(self.columnas_esperadas)

        # Calcular columna "Dias-Restantes"
        fecha_actual = datetime.now().date()
        self.df["dias_restantes"] = self.df["fecha_final"].apply(lambda fecha: (fecha.date() - fecha_actual).days if pd.notnull(fecha) else None)

        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df
