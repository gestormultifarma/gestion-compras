# analysis\transformer\transformer_inventario.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd

class TransformadorInventario(BaseTransformer):
    columnas_estandarizadas = {
        "Grupo": "Grupo",
        "Cod Producto": "Codigo",
        "Nombre": "Nombre",
        "Inventario Caja": "Inventario-Caja",
        "Inventario Blister": "Inventario-Blister",
        "Costo Unidad": "Costo-Unidad",
        "Costo Blister": "Costo-Blister",
        "Costo Caja": "Costo-Caja",
        "% Iva": "%-Iva",
        "Inventario Unidad": "Inventario-Unidad",
        "Costo Total": "Costo-Total",
        "Costo Total Iva": "Costo-Total-Iva",
        "Venta Total": "Venta-Total",
        "Ibua": "Ibua",
        "Icui": "Icui"
    }

    columnas_esperadas = list(columnas_estandarizadas.values())

    def transformar(self):
        self.df = pd.read_excel(self.path, sheet_name=0, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.agregar_columnas_faltantes(self.columnas_esperadas)
        self.validar_columnas(self.columnas_esperadas)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df