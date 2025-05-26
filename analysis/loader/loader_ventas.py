# analysis/loader/loader_ventas.py

from analysis.loader.loader_base import BaseLoader

class LoaderVentas(BaseLoader):
    def __init__(self, logger=None):
        super().__init__(db_name="gestion_compras", logger=logger)

    def cargar(self, df, nombre_tabla):
        # Nombre de tabla dinámico según clave (p. ej. "12345_bella_suiza_202504")
        self.cargar_dataframe(df, nombre_tabla)
