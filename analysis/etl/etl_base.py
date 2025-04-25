# analysis\etl\etl_base.py

import os

class BaseETLRunner:
    def __init__(self, directorio_raiz, extractor_func, transformer_func, clave_func=None, nombre_etl="ETL"):
        self.directorio_raiz = directorio_raiz
        self.extractor_func = extractor_func
        self.transformer_func = transformer_func
        self.clave_func = clave_func
        self.nombre_etl = nombre_etl
        self.dataframes = {}

    def run(self):
        print(f"Iniciando {self.nombre_etl} desde: {self.directorio_raiz}")
        archivos = self.extractor_func()
        print(f"{len(archivos)} archivos encontrados para procesar.\n")

        for archivo in archivos:
            try:
                clave = self.clave_func(archivo) if self.clave_func else os.path.basename(archivo)
                df = self.transformer_func(archivo)
                self.dataframes[clave] = df
                print(f"[✅ OK] {clave} procesado")
            except Exception as e:
                print(f"[⚠️ ERROR] Falló la transformación de {archivo}: {e}")

        print(f"\n🎯 {self.nombre_etl} Finalizado.")
        print(f"✅ Total de archivos procesados exitosamente: {len(self.dataframes)}")
        print(f"📁 Claves disponibles: {list(self.dataframes.keys())}")
        return self.dataframes