# analysis\transformer\transformer_ventas.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd


class VentasTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "Codigo": "Codigo",
        "Nombre": "Nombre",
        "Sugerido": "Sugerido",
        "Laboratorio": "Laboratorio",
        "Caja": "Contenido-Caja",
        "Blister": "Contenido-Blister",
        "Unidad": "Contenido-Unidad",
        "Caja.1": "Inventario-Caja",
        "Blister.1": "Inventario-Blister",
        "Unidad.1": "Inventario-Unidad",
        "UltimaCompra": "Ultima-Compra",
        "Caja.2": "Venta-Caja",
        "Blister.2": "Venta-Blister",
        "Unidad.2": "Venta-Unidad",
        "Costo Unitario": "Costo-Unitario",
        "Precio Venta Contado": "Precio-Venta-Contado"
    }

    columnas_esperadas = list(columnas_estandarizadas.values())
    
    def transformar(self):
    # Leer el archivo usando la segunda fila como encabezado
        self.df = pd.read_excel(self.path, header=1, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.validar_columnas(self.columnas_esperadas)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        return self.df