# analysis/transformer/transformer_ecommerce.py

import pandas as pd
from analysis.transformer.transformer_base import BaseTransformer

class EcommerceTransformer(BaseTransformer):
    columnas_estandarizadas = {
        "cod": "codigo",
        "SkuEcommerce": "sku_ecommerce",
        "CódigoProducto": "nuevo_codigo",
        "CódigoBarras": "codigo_barras",
        "NombreProducto": "nombre",
        "CodigoIva": "porcentaje_iva",
        "CantidadFracciones": "cantidad_fracciones",
        "VentaFraccionada": "venta_fraccionada",
        "Fabricante": "laboratorio",
        "PrincipioActivo": "principio_activo",
        "Marca": "marca",
        "CantidadPresentación": "cantidad_presentacion",
        "UnidadContenido": "unidad_contenido",
        "Departamento": "departamento",
        "Categoría": "categoria",
        "Subcategoría": "subcategoria",
        "PrecioUnidad": "precio_unidad",
        "PrecioBlister": "precio_blister",
        "PUM": "pum",
        "CantidadFraccionado": "cantidad_fraccionado",
        "EmpaqueFraccionado": "empaque_fraccionado",
        "ClasificaciónRotación": "clasificacion_rotacion",
        "Costo": "pvp",
        "TamanoEan": "tamano_ean",
        "ImagenFísica": "imagen_fisica",
        "EstaLR": "lr_ecommerce"
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
