#analysis\loader\loader_base.py

import pandas as pd

from sqlalchemy import create_engine
from utils.db_connection import get_mysql_url

class BaseLoader:
    def __init__(self, db_name="gestion_compras", logger=None):
        self.db_url = get_mysql_url(db_name)
        self.engine = create_engine(self.db_url)
        self.logger = logger

    def cargar_dataframe(self, df: pd.DataFrame, nombre_tabla: str):
        try:
            df.to_sql(nombre_tabla, con=self.engine, if_exists='replace', index=False)
            mensaje_exito = f"✅ Tabla '{nombre_tabla}' cargada con éxito. Registros: {len(df)}"
            print(mensaje_exito)
            if self.logger:
                self.logger.info(mensaje_exito)
            return True
        except Exception as e:
            mensaje_error = f"❌ Error al cargar tabla '{nombre_tabla}': {e}"
            print(mensaje_error)
            if self.logger:
                self.logger.error(mensaje_error)
            return False
