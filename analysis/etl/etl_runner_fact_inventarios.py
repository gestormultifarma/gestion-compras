# analysis/etl/etl_runner_fact_inventarios.py

import os
import warnings
from datetime import datetime

from analysis.extractor.extractor_fact_inventarios import ExtractorFactInventarios
from analysis.transformer.transformer_fact_inventarios import TransformadorFactInventarios
from analysis.loader.loader_fact_inventarios import LoaderFactInventarios
from utils.logger_etl import LoggerETL

# Suprimir advertencias de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

class FactInventariosETLRunner:
    """
    Runner ETL para la tabla fact_inventarios.
    Este runner implementa un proceso ETL diferente al estándar,
    ya que extrae datos de las tablas staging en lugar de archivos Excel.
    """
    
    def __init__(self):
        """Inicializa el runner con sus componentes ETL y el logger"""
        self.logger = LoggerETL("ETL Fact Inventarios")
        self.extractor = ExtractorFactInventarios(logger=self.logger)
        self.loader = LoaderFactInventarios(logger=self.logger)
        
    def run(self):
        """Ejecuta el proceso ETL completo para fact_inventarios"""
        try:
            self.logger.info("🚀 Iniciando ETL para fact_inventarios")
            
            # Fase de extracción
            self.logger.info("📥 Iniciando extracción de datos")
            datos_extraidos = self.extractor.extraer()
            self.logger.info(f"📊 Datos extraídos: {len(datos_extraidos)} registros")
            
            # Fase de transformación
            self.logger.info("🔄 Iniciando transformación de datos")
            transformador = TransformadorFactInventarios(datos_extraidos, logger=self.logger)
            df_transformado = transformador.transformar()
            
            # Guardar histórico como CSV (opcional)
            if not df_transformado.empty:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                nombre_archivo_csv = f"fact_inventarios_{timestamp}.csv"
                ruta_historico = os.path.join("E:/desarrollo/gestionCompras/historico/ETL_Fact_Inventarios")
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
    # Para ejecutar desde línea de comandos: python -m analysis.etl.etl_runner_fact_inventarios
    runner = FactInventariosETLRunner()
    runner.run()
