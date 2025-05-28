# analysis/loader/loader_fact_inventarios.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
from analysis.loader.loader_base import BaseLoader

class LoaderFactInventarios(BaseLoader):
    """
    Cargador especializado para la tabla fact_inventarios.
    Implementa carga incremental para mantener el historial.
    """
    
    def __init__(self, logger=None):
        """
        Inicializa el cargador para la tabla fact_inventarios.
        
        Args:
            logger: Instancia de logger para registrar eventos
        """
        super().__init__(db_name="gestion_compras", logger=logger)
    
    def cargar_dataframe(self, df, nombre_tabla=None):
        """
        Carga los datos de inventario a la tabla fact_inventarios.
        Implementa una estrategia de insertar nuevos y actualizar existentes.
        
        Args:
            df (pandas.DataFrame): DataFrame con los datos a cargar
            nombre_tabla (str, optional): Ignorado, siempre se usa 'fact_inventarios'
            
        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            if df.empty:
                if self.logger:
                    self.logger.warning("⚠️ DataFrame vacío, no se realizará carga")
                return False
            
            # Siempre usar fact_inventarios independientemente del nombre_tabla proporcionado
            nombre_tabla_destino = "fact_inventarios"
            
            # Iniciar una transacción
            with self.engine.begin() as connection:
                # Enfoque 1: Para cada registro, insertar o actualizar según corresponda
                registros_actualizados = 0
                registros_insertados = 0
                
                for _, row in df.iterrows():
                    # Verificar si ya existe un registro para el mismo PDV y fecha
                    query_verificar = text("""
                        SELECT inventario_sk 
                        FROM fact_inventarios 
                        WHERE pdv_id = :pdv_id AND fecha = :fecha
                    """)
                    
                    result = connection.execute(
                        query_verificar, 
                        {"pdv_id": row['pdv_id'], "fecha": row['fecha']}
                    ).fetchone()
                    
                    if result:
                        # Actualizar registro existente
                        inventario_sk = result[0]
                        query_actualizar = text("""
                            UPDATE fact_inventarios 
                            SET 
                                cantidad_existencias = :cantidad_existencias,
                                valor_existencias = :valor_existencias,
                                costo_promedio = :costo_promedio,
                                rotacion_dias = :rotacion_dias,
                                rotacion_semanas = :rotacion_semanas,
                                dias_stock = :dias_stock,
                                nivel_stock = :nivel_stock,
                                fecha_actualizacion = NOW()
                            WHERE inventario_sk = :inventario_sk
                        """)
                        
                        connection.execute(
                            query_actualizar,
                            {
                                "cantidad_existencias": row['cantidad_existencias'],
                                "valor_existencias": row['valor_existencias'],
                                "costo_promedio": row['costo_promedio'],
                                "rotacion_dias": row['rotacion_dias'],
                                "rotacion_semanas": row['rotacion_semanas'],
                                "dias_stock": row['dias_stock'],
                                "nivel_stock": row['nivel_stock'],
                                "inventario_sk": inventario_sk
                            }
                        )
                        registros_actualizados += 1
                        
                    else:
                        # Insertar nuevo registro
                        query_insertar = text("""
                            INSERT INTO fact_inventarios (
                                pdv_id, fecha, cantidad_existencias, valor_existencias,
                                costo_promedio, rotacion_dias, rotacion_semanas,
                                dias_stock, nivel_stock, fecha_creacion, fecha_actualizacion
                            ) VALUES (
                                :pdv_id, :fecha, :cantidad_existencias, :valor_existencias,
                                :costo_promedio, :rotacion_dias, :rotacion_semanas,
                                :dias_stock, :nivel_stock, NOW(), NOW()
                            )
                        """)
                        
                        connection.execute(
                            query_insertar,
                            {
                                "pdv_id": row['pdv_id'],
                                "fecha": row['fecha'],
                                "cantidad_existencias": row['cantidad_existencias'],
                                "valor_existencias": row['valor_existencias'],
                                "costo_promedio": row['costo_promedio'],
                                "rotacion_dias": row['rotacion_dias'],
                                "rotacion_semanas": row['rotacion_semanas'],
                                "dias_stock": row['dias_stock'],
                                "nivel_stock": row['nivel_stock']
                            }
                        )
                        registros_insertados += 1
            
            # Registrar resultado
            mensaje_exito = (
                f"✅ Tabla '{nombre_tabla_destino}' actualizada con éxito. "
                f"Registros insertados: {registros_insertados}, "
                f"Registros actualizados: {registros_actualizados}"
            )
            print(mensaje_exito)
            
            if self.logger:
                self.logger.info(mensaje_exito)
                
            return True
            
        except Exception as e:
            mensaje_error = f"❌ Error al cargar fact_inventarios: {e}"
            print(mensaje_error)
            
            if self.logger:
                self.logger.error(mensaje_error)
                
            return False
