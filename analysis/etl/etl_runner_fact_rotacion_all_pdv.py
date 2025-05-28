# analysis/etl/etl_runner_fact_rotacion_all_pdv.py

import os
import warnings
from datetime import datetime

# Usar el extractor para todos los PDVs
from analysis.extractor.extractor_fact_rotacion_all_pdv import ExtractorFactRotacion
from analysis.transformer.transformer_fact_rotacion import TransformadorFactRotacion
from analysis.loader.loader_fact_rotacion import LoaderFactRotacion
from utils.logger_etl import LoggerETL

# Suprimir advertencias de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

class FactRotacionETLRunner:
    """
    Runner ETL para la tabla fact_rotacion.
    Implementa un proceso ETL que extrae datos de las tablas stg_rotacion_*
    para TODOS los puntos de venta.
    """
    
    def __init__(self):
        """Inicializa el runner con sus componentes ETL y el logger"""
        self.logger = LoggerETL("ETL Fact Rotacion (Todos los PDVs)")
        self.extractor = ExtractorFactRotacion(logger=self.logger)
        self.loader = LoaderFactRotacion(logger=self.logger)
        
    def run(self):
        """Ejecuta el proceso ETL completo para fact_rotacion procesando todos los PDVs"""
        try:
            self.logger.info("🚀 Iniciando ETL para fact_rotacion (TODOS los PDVs)")
            
            # Fase de extracción
            self.logger.info("📥 Iniciando extracción de datos desde tablas stg_rotacion_*")
            datos_extraidos = self.extractor.extraer()
            
            if datos_extraidos.empty:
                self.logger.warning("⚠️ No se obtuvieron datos en la extracción")
                return False
                
            self.logger.info(f"📊 Datos extraídos: {len(datos_extraidos)} registros")
            
            # Fase de transformación
            self.logger.info("🔄 Iniciando transformación de datos")
            transformador = TransformadorFactRotacion(datos_extraidos, logger=self.logger)
            df_transformado = transformador.transformar()
            
            if df_transformado.empty:
                self.logger.warning("⚠️ No hay datos después de la transformación")
                return False
                
            # Guardar histórico como CSV (opcional)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            nombre_archivo_csv = f"fact_rotacion_all_pdv_{timestamp}.csv"
            ruta_historico = os.path.join("E:/desarrollo/gestionCompras/historico/ETL_Fact_Rotacion")
            os.makedirs(ruta_historico, exist_ok=True)
            ruta_completa = os.path.join(ruta_historico, nombre_archivo_csv)
            df_transformado.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 Backup CSV guardado en: {ruta_completa}")
            
            # Fase de carga
            self.logger.info("📤 Iniciando carga de datos")
            resultado_carga = self.loader.cargar_dataframe(df_transformado)
            
            if resultado_carga:
                self.logger.info("✅ Proceso ETL completado con éxito")
            else:
                self.logger.error("❌ Error en la carga de datos")
                
            return resultado_carga
            
        except Exception as e:
            self.logger.error(f"💥 Error en el proceso ETL: {e}")
            return False


if __name__ == '__main__':
    # Para ejecutar desde línea de comandos: python -m analysis.etl.etl_runner_fact_rotacion_all_pdv
    runner = FactRotacionETLRunner()
    runner.run()
