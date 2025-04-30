# analysis/transformer/transformer_oferta.py

from analysis.transformer.transformer_base import BaseTransformer
import pandas as pd

class TransformadorOferta(BaseTransformer):
    columnas_estandarizadas = {
        "Centro": "centro",
        "Laboratorio": "laboratorio",
        "Material": "codigo",
        "Nombre": "nombre",
        "Cant": "cantidad",
        "Cant.Obs": "cantidad_obsequio",
        "Material.1": "codigo_obsequio",
        "Nombre.1": "nombre_obsequio",
        "$ Venta Real": "costo_caja_real"
    }

    columnas_esperadas = list(columnas_estandarizadas.values()) + ["Precio-Efectivo", "Descuento"]

    def transformar(self):
        self.df = pd.read_excel(self.path, engine="xlrd")
        self.estandarizar_columnas(self.columnas_estandarizadas)
        self.agregar_columnas_faltantes(self.columnas_esperadas)
        self.df['Costo-Caja-Real'] = (self.df['Costo-Caja-Real'] * 1000).round(0).astype(int)
        
        # Calcular 'Precio-Efectivo' y 'Descuento'
        try:
            self.df["Precio-Efectivo"] = (
                (self.df["Cantidad"] * self.df["Costo-Caja-Real"]) / (self.df["Cantidad"] + self.df["Cantidad-Obsequio"])
            )
            self.df["Descuento"] = (
                (self.df["Cantidad-Obsequio"] / (self.df["Cantidad"] + self.df["Cantidad-Obsequio"])) * 100
            )
        except Exception as e:
            raise ValueError(f"Error al calcular columnas de oferta: {e}")

        # Validar columnas finales
        self.validar_columnas(self.columnas_esperadas)
        self.df['Precio-Efectivo'] = (self.df['Precio-Efectivo']).round(0).astype(int)
        self.df['Descuento'] = (self.df['Descuento']/100).round(2).astype(float)
        # print(f"\n📦 Vista previa final del archivo: {self.path}")
        # print(self.df.tail(5))
        # print(self.df.dtypes)
        return self.df