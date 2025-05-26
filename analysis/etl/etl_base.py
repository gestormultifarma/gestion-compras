# analysis\etl\etl_base.py

from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url  


class BaseETLRunner:
    def __init__(self, directorio_raiz, extractor_func, transformer_func, clave_func, nombre_etl, logger=None, loader=None):
        self.directorio_raiz = directorio_raiz
        self.extractor_func = extractor_func
        self.transformer_func = transformer_func
        self.clave_func = clave_func
        self.nombre_etl = nombre_etl
        self.logger = logger
        self.loader = loader

    def run(self):
        archivos = self.extractor_func()
        if self.logger:
            self.logger.info(f"🔍 Archivos encontrados: {len(archivos)}")

        for archivo in archivos:
            try:
                clave = self.clave_func(archivo)
                if self.logger:
                    self.logger.info(f"📄 Procesando archivo: {archivo} (clave: {clave})")

                df = self.transformer_func(archivo)

                if df is not None and not df.empty:
                    # --- Guardar histórico como CSV local ---
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    nombre_archivo_csv = f"{clave}_{timestamp}.csv"
                    ruta_historico = Path(f"E:/desarrollo/gestionCompras/historico/{self.nombre_etl}")
                    ruta_historico.mkdir(parents=True, exist_ok=True)
                    df.to_csv(ruta_historico / nombre_archivo_csv, index=False, encoding='utf-8-sig')

                    if self.logger:
                        self.logger.info(f"🗂️ Backup CSV guardado en: {ruta_historico / nombre_archivo_csv}")
                        self.logger.info(f"📊 DataFrame generado. Registros: {len(df)}")

                    # --- Cargar a MySQL reemplazando la tabla ---
                    if self.loader:
                        self.loader.cargar_dataframe(df, clave)

                        # validación de prueba
                        # print(f"[DEBUG] carga {clave}")

                        self.validar_carga(clave)  # ✅ Validación después de cargar
                        # revision de prueba
                        # print(f"[DEBUG] Validación {clave}")


                else:
                    mensaje = f"⚠️  DataFrame vacío o nulo para archivo: {archivo}"
                    if self.logger:
                        self.logger.error(mensaje)
                    print(mensaje)  

            except Exception as e:
                if self.logger:
                    self.logger.error(f"💥 Error al procesar archivo '{archivo}': {e}")

    def validar_carga(self, nombre_tabla):
        try:
            engine = create_engine(get_mysql_url("gestion_compras"))
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(*) FROM `{nombre_tabla}`")
                resultado = conn.execute(query).scalar()

                if self.logger:
                    self.logger.info(f"✅ Validación de carga: {resultado} registros encontrados en '{nombre_tabla}'. Carga confirmada en la base de datos.")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Error al validar la tabla '{nombre_tabla}': {e}")

