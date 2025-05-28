# analysis/extractor/extractor_fact_inventarios.py

import os
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
import pandas as pd

class ExtractorFactInventarios:
    def __init__(self, logger=None):
        """
        Extractor para obtener datos consolidados de inventario desde las tablas staging
        y calcular métricas para la tabla fact_inventarios.
        
        Args:
            logger: Instancia de logger para registrar eventos
        """
        self.logger = logger
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
    
    def extraer(self):
        """
        Extrae y consolida datos de inventario de todas las tablas staging de inventario.
        
        Returns:
            List[dict]: Lista de registros de inventario consolidados por PDV y fecha
        """
        try:
            # Consultar todas las tablas staging de inventario
            query_tablas = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg_inventario_%'
            """)
            
            with self.engine.connect() as connection:
                tablas = [row[0] for row in connection.execute(query_tablas)]
            
            if self.logger:
                self.logger.info(f"📊 Encontradas {len(tablas)} tablas staging de inventario")
            
            # Lista para almacenar los resultados consolidados por PDV
            resultados_consolidados = []
            
            for tabla in tablas:
                try:
                    # Obtener el nombre del PDV desde el nombre de la tabla
                    nombre_pdv = tabla.replace("stg_inventario_", "")
                    
                    # Consultar dim_pdv para obtener el ID del PDV
                    query_pdv = text(f"""
                        SELECT pdv_sk
                        FROM dim_pdv
                        WHERE LOWER(nombre_pdv) LIKE LOWER(:nombre_pdv)
                    """)
                    
                    with self.engine.connect() as connection:
                        pdv_result = connection.execute(query_pdv, {"nombre_pdv": f"%{nombre_pdv}%"}).fetchone()
                    
                    if not pdv_result:
                        if self.logger:
                            self.logger.warning(f"⚠️ No se encontró PDV para: {nombre_pdv}")
                        continue
                    
                    pdv_id = pdv_result[0]
                    
                    # Consultar los datos de inventario
                    query_inventario = text(f"""
                        SELECT 
                            NOW() as fecha_extraccion,
                            {pdv_id} as pdv_id,
                            CURDATE() as fecha,
                            SUM(inventario_unidad) as cantidad_existencias,
                            SUM(costo_total) as valor_existencias,
                            CASE 
                                WHEN SUM(inventario_unidad) > 0 
                                THEN SUM(costo_total) / SUM(inventario_unidad)
                                ELSE 0
                            END as costo_promedio,
                            NULL as rotacion_dias,
                            NULL as rotacion_semanas,
                            NULL as dias_stock,
                            CASE
                                WHEN SUM(inventario_unidad) = 0 THEN 'Sin stock'
                                WHEN SUM(inventario_unidad) < 100 THEN 'Bajo'
                                WHEN SUM(inventario_unidad) < 500 THEN 'Medio'
                                ELSE 'Alto'
                            END as nivel_stock
                        FROM `{tabla}`
                    """)
                    
                    with self.engine.connect() as connection:
                        resultado = connection.execute(query_inventario).fetchone()
                    
                    if resultado:
                        resultados_consolidados.append({
                            'pdv_id': resultado[1],
                            'fecha': resultado[2],
                            'cantidad_existencias': resultado[3] or 0,
                            'valor_existencias': resultado[4] or 0,
                            'costo_promedio': resultado[5] or 0,
                            'rotacion_dias': resultado[6],
                            'rotacion_semanas': resultado[7],
                            'dias_stock': resultado[8],
                            'nivel_stock': resultado[9],
                            'fecha_creacion': pd.Timestamp.now(),
                            'fecha_actualizacion': pd.Timestamp.now()
                        })
                        
                        if self.logger:
                            self.logger.info(f"✓ Datos extraídos para PDV: {nombre_pdv} (ID: {pdv_id})")
                    
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"💥 Error al procesar tabla {tabla}: {e}")
            
            return resultados_consolidados
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error en extracción de datos de inventario: {e}")
            return []
