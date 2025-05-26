# analysis\transformer\transformer_inventario.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd

class TransformadorInventario(BaseTransformer):
    columnas_estandarizadas = {
        "Grupo": "grupo",
        "Cod Producto": "codigo",
        "Nombre": "nombre",
        "Inventario Caja": "inventario_caja",
        "Inventario Blister": "inventario_blister",
        "Costo Unidad": "costo_unidad",
        "Costo Blister": "costo_blister",
        "Costo Caja": "costo_caja",
        "% Iva": "porcentaje_iva",
        "Inventario Unidad": "inventario_unidad",
        "Costo Total": "costo_total",
        "Costo Total Iva": "costo_total_iva",
        "Venta Total": "venta_total",
        "Ibua": "ibua",
        "Icui": "icui"
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