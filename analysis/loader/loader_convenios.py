# analysis\loader\loader_convenios.py

from analysis.loader.loader_base import BaseLoader

class LoaderConvenios(BaseLoader):
    def __init__(self, logger=None):
        super().__init__(db_name="gestion_compras", logger=logger)

    def cargar(self, df):
        nombre_tabla = "convenios"
        self.cargar_dataframe(df, nombre_tabla)
