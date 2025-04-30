# analysis/loader/loader_bodega.py

from analysis.loader.loader_base import LoaderBase

class LoaderBodega(LoaderBase):
    def __init__(self):
        super().__init__(tabla_destino="bodega")

    def cargar_bodega(self, df):
        self.cargar(df)
