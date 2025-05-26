from analysis.loader.loader_base import BaseLoader

class LoaderOferta(BaseLoader):
    def __init__(self, logger=None):
        super().__init__(db_name="gestion_compras", logger=logger)

    def cargar(self, df):
        # print("🚀 Iniciando carga del DataFrame en la tabla 'oferta'...")
        self.cargar_dataframe(df, "oferta")
        # print("✅ Carga finalizada.")