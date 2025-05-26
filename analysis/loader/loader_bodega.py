# analysis\loader\loader_bodega.py

from analysis.loader.loader_base import BaseLoader

class LoaderBodega(BaseLoader):
    def __init__(self, logger=None):
        super().__init__(db_name="gestion_compras", logger=logger)

    def cargar(self, df):
        nombre_tabla = "bodega"
        self.cargar_dataframe(df, nombre_tabla)
