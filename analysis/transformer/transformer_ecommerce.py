# analysis/transformer/transformer_ecommerce.py

import pandas as pd
from analysis.transformer.transformer_base import BaseTransformer

class EcommerceTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "cod": "Codigo",
        "SkuEcommerce": "SKU-Ecommerce",
        "CódigoProducto": "Nuevo-Codigo",
        "CódigoBarras": "Codigo-Barras",
        "NombreProducto": "Nombre",
        "CodigoIva": "%-Iva",
        "CantidadFracciones": "Cantidad-Fracciones",
        "VentaFraccionada": "Venta-Fraccionada",
        "Fabricante": "Laboratorio",
        "PrincipioActivo": "Principio-Activo",
        "Marca": "Marca",
        "CantidadPresentación": "Cantidad-Presentacion",
        "UnidadContenido": "Unidad-Contenido",
        "Departamento": "Departamento",
        "Categoría": "Categoria",
        "Subcategoría": "Subcategoria",
        "PrecioUnidad": "Precio-Unidad",
        "PrecioBlister": "Precio-Blister",
        "PUM": "PUM",
        "CantidadFraccionado": "Cantidad-Fraccionado",
        "EmpaqueFraccionado": "Empaque-Fraccionado",
        "ClasificaciónRotación": "Clasificacion-Rotacion",
        "Costo": "PVP",
        "TamanoEan": "Tamano-EAN",
        "ImagenFísica": "Imagen-Fisica",
        "EstaLR": "LR-ecommerce"
    }

    columnas_esperadas = list(columnas_estandarizadas.values())

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="openpyxl")
        self.df = self.limpiar_valores_invalidos(self.df)
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.validar_columnas(self.columnas_esperadas)
        print(f"\n📦 Vista previa final del archivo: {self.path}")
        print(self.df.tail(5))
        return self.df
