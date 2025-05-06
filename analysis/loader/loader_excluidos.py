# analysis/loader/loader_excluidos.py

from analysis.loader.loader_base import BaseLoader

class LoaderExcluidos(BaseLoader):
    def __init__(self, logger=None):
        super().__init__(db_name="gestion_compras", logger=logger)

    def cargar(self, df):
        self.cargar_dataframe(df, "excluidos")
