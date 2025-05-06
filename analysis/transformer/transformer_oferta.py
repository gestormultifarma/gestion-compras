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

    columnas_esperadas = list(columnas_estandarizadas.values()) + ["precio_efectivo", "descuento"]

    def transformar(self):
        #print(f"📥 Leyendo archivo: {self.path}")

        try:
            self.df = pd.read_excel(self.path, engine="xlrd")
            #print("🧩 Columnas originales:", list(self.df.columns))

            self.estandarizar_columnas(self.columnas_estandarizadas)
            #print("✅ Columnas estandarizadas:", list(self.df.columns))

            self.agregar_columnas_faltantes(self.columnas_esperadas)
            #print("➕ Columnas faltantes agregadas (si aplicaba)")

            self.df['costo_caja_real'] = (self.df['costo_caja_real'] * 1000).round(0).astype(int)
            
            self.df["precio_efectivo"] = (
                (self.df["cantidad"] * self.df["costo_caja_real"]) / 
                (self.df["cantidad"] + self.df["cantidad_obsequio"])
            )
            self.df["descuento"] = (
                (self.df["cantidad_obsequio"] / 
                (self.df["cantidad"] + self.df["cantidad_obsequio"])) * 100
            )

            self.validar_columnas(self.columnas_esperadas)
            #print("columnas validadas")

            self.df['precio_efectivo'] = self.df['precio_efectivo'].round(0).astype(int)
            self.df['descuento'] = (self.df['descuento'] / 100).round(2).astype(float)

            #print("📦 Vista previa del DataFrame:")
            #print(self.df.head())
            #print("📏 Registros:", len(self.df))
            return self.df

        except Exception as e:
            #print(f"❌ Error en transformar(): {e}")
            raise ValueError(f"Error al calcular columnas de oferta: {e}")
            


