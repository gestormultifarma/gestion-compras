# analysis\transformer\transformer_ventas.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd


class VentasTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "Codigo": "codigo",
        "Nombre": "nombre",
        "Sugerido": "sugerido",
        "Laboratorio": "laboratorio",
        "Caja": "contenido_caja",
        "Blister": "contenido_blister",
        "Unidad": "contenido_unidad",
        "Caja.1": "inventario_caja",
        "Blister.1": "inventario_blister",
        "Unidad.1": "inventario_unidad",
        "UltimaCompra": "ultima_compra",
        "Caja.2": "venta_caja",
        "Blister.2": "venta_blister",
        "Unidad.2": "venta_unidad",
        "Costo Unitario": "costo_unitario",
        "Precio Venta Contado": "precio_venta_contado"
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