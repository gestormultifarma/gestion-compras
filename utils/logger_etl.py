# utils/logger_etl.py

import logging
import os
import time
from datetime import datetime

class LoggerETL:
    def __init__(self, nombre_etl):
        self.nombre_etl = nombre_etl
        self.logger = None
        self.setup_logger()

    def setup_logger(self):
        logs_dir = "E:\\desarrollo\\gestionCompras\\logs"
        os.makedirs(logs_dir, exist_ok=True)
        fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{self.nombre_etl.lower().replace(' ', '_')}_{fecha_hora}.log"
        log_path = os.path.join(logs_dir, log_filename)

        self.logger = logging.getLogger(self.nombre_etl)
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(log_path, encoding='utf-8')
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    def info(self, mensaje):
        if self.logger:
            self.logger.info(mensaje)

    def error(self, mensaje):
        if self.logger:
            self.logger.error(mensaje)

def limpiar_logs_antiguos(directorio_logs, dias=30):
    """Elimina logs *.log más antiguos que 'dias' días."""
    ahora = time.time()
    for archivo in os.listdir(directorio_logs):
        if archivo.endswith(".log"):
            ruta_archivo = os.path.join(directorio_logs, archivo)
            tiempo_modificacion = os.path.getmtime(ruta_archivo)
            if (ahora - tiempo_modificacion) > (dias * 86400):  # 86400 segundos = 1 día
                try:
                    os.remove(ruta_archivo)
                    print(f"🧹 Log eliminado: {archivo}")
                except Exception as e:
                    print(f"⚠️ Error eliminando {archivo}: {e}")
