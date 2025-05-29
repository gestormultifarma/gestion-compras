# etl_fact_inventarios_corregido.py
"""
Script ETL corregido para poblar la tabla fact_inventarios con datos de inventario_caja
de las tablas de staging de cada punto de venta.

Sigue el patrón ETL del proyecto utilizando extracción, transformación y carga.
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

class ETLFactInventarios:
    """Clase para ejecutar el proceso ETL de fact_inventarios"""
    
    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
        self.fecha_proceso = datetime.now()
    
    def extraer_tablas_staging(self):
        """Identifica y extrae las tablas de staging de inventario"""
        try:
            print("🔍 Identificando tablas de staging de inventario...")
            
            with self.engine.connect() as connection:
                # Consulta para obtener todas las tablas de staging de inventario
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg\\_inventario\\_%'
                """
                result = connection.execute(text(query))
                tablas_staging = [row[0] for row in result]
                
                print(f"Se encontraron {len(tablas_staging)} tablas de staging de inventario")
                for tabla in tablas_staging:
                    print(f"  - {tabla}")
                
                # Extraer información de PDVs
                pdvs_info = []
                for tabla in tablas_staging:
                    try:
                        # Obtener el código de PDV de la tabla dim_pdv
                        # El formato típico es stg_inventario_{nombre_pdv}_{codigo_pdv}
                        partes = tabla.replace('stg_inventario_', '').split('_')
                        
                        # El último componente podría ser el código
                        posible_codigo = partes[-1]
                        
                        # Intentar encontrar el PDV en la tabla dim_pdv
                        query_pdv = f"""
                        SELECT pdv_sk, codigo_pdv, nombre_pdv 
                        FROM dim_pdv 
                        WHERE codigo_pdv = '{posible_codigo}'
                        """
                        
                        result = connection.execute(text(query_pdv))
                        row = result.fetchone()
                        
                        if row:
                            # Si encontramos el PDV, usar sus datos
                            pdv_id = row[0]
                            codigo_pdv = row[1]
                            nombre_pdv = row[2]
                        else:
                            # Si no lo encontramos por código, intentar por nombre
                            posible_nombre = '_'.join(partes[:-1]) if len(partes) > 1 else partes[0]
                            query_pdv = f"""
                            SELECT pdv_sk, codigo_pdv, nombre_pdv 
                            FROM dim_pdv 
                            WHERE nombre_pdv LIKE '%{posible_nombre}%'
                            LIMIT 1
                            """
                            
                            result = connection.execute(text(query_pdv))
                            row = result.fetchone()
                            
                            if row:
                                pdv_id = row[0]
                                codigo_pdv = row[1]
                                nombre_pdv = row[2]
                            else:
                                # Si no encontramos nada, usar valores por defecto
                                print(f"⚠️ No se encontró PDV para {tabla}")
                                pdv_id = -1
                                codigo_pdv = posible_codigo
                                nombre_pdv = posible_nombre
                        
                        pdvs_info.append({
                            'tabla': tabla,
                            'pdv_id': pdv_id,
                            'codigo_pdv': codigo_pdv,
                            'nombre_pdv': nombre_pdv
                        })
                        
                    except Exception as e:
                        print(f"⚠️ Error al procesar tabla {tabla}: {str(e)}")
                        continue
                
                return pdvs_info
                
        except Exception as e:
            print(f"❌ Error al extraer tablas staging: {str(e)}")
            return []
    
    def extraer_datos_inventario(self, tabla):
        """Extrae los datos de inventario_caja de una tabla de staging"""
        try:
            print(f"📋 Extrayendo datos de {tabla}...")
            
            # Utilizamos backticks para manejar nombres de tabla con espacios
            query = f"""
            SELECT 
                codigo,
                nombre,
                COALESCE(inventario_caja, 0) as inventario_caja,
                COALESCE(costo_caja, 0) as costo_caja,
                COALESCE(costo_total, 0) as costo_total
            FROM `{tabla}`
            """
            
            df = pd.read_sql(query, self.engine)
            
            print(f"  ✅ Extraídos {len(df)} registros")
            return df
                
        except Exception as e:
            print(f"❌ Error al extraer datos de {tabla}: {str(e)}")
            return pd.DataFrame()
    
    def transformar_datos(self, df, pdv_info):
        """Transforma y enriquece los datos de inventario"""
        try:
            print(f"🔄 Transformando datos para PDV {pdv_info['nombre_pdv']}...")
            
            if df.empty:
                print("  ⚠️ No hay datos para transformar")
                return df
            
            # Agregar información del PDV
            df['pdv_id'] = pdv_info['pdv_id']
            df['codigo_pdv'] = pdv_info['codigo_pdv']
            df['nombre_pdv'] = pdv_info['nombre_pdv']
            
            # Agregar fecha actual
            df['fecha'] = self.fecha_proceso.date()
            
            # Renombrar columnas para consistencia
            df = df.rename(columns={
                'inventario_caja': 'cantidad_existencias',
                'costo_caja': 'costo_promedio',
                'costo_total': 'valor_existencias'
            })
            
            # Calcular campos adicionales
            # Para rotación y días de stock, usaremos valores por defecto
            df['rotacion_dias'] = 30  # Valor por defecto
            df['rotacion_semanas'] = (df['rotacion_dias'] / 7).round(2)
            df['dias_stock'] = 30  # Valor por defecto
            
            # Determinar nivel de stock basado en días de stock
            def determinar_nivel_stock(dias):
                if dias <= 7:
                    return 'CRÍTICO'
                elif dias <= 15:
                    return 'BAJO'
                elif dias <= 30:
                    return 'NORMAL'
                else:
                    return 'EXCESO'
            
            df['nivel_stock'] = df['dias_stock'].apply(determinar_nivel_stock)
            
            # Agregar fechas de creación y actualización
            now = datetime.now()
            df['fecha_creacion'] = now
            df['fecha_actualizacion'] = now
            
            print(f"  ✅ Transformación completada para {len(df)} registros")
            return df
            
        except Exception as e:
            print(f"❌ Error al transformar datos: {str(e)}")
            return pd.DataFrame()
    
    def cargar_datos(self, df):
        """Carga los datos transformados en la tabla fact_inventarios"""
        try:
            if df.empty:
                print("⚠️ No hay datos para cargar")
                return 0
            
            print(f"💾 Cargando {len(df)} registros en fact_inventarios...")
            
            # Seleccionar solo las columnas que existen en fact_inventarios
            columnas_fact = [
                'fecha', 'cantidad_existencias', 'valor_existencias', 
                'costo_promedio', 'rotacion_dias', 'rotacion_semanas', 
                'dias_stock', 'nivel_stock', 'fecha_creacion', 
                'fecha_actualizacion', 'pdv_id'
            ]
            
            # Filtrar el DataFrame para incluir solo las columnas necesarias
            # y asegurarnos de que existan en el DataFrame
            columnas_disponibles = [col for col in columnas_fact if col in df.columns]
            df_final = df[columnas_disponibles]
            
            # Verificar si faltan columnas y agregar columnas por defecto si es necesario
            for col in columnas_fact:
                if col not in df_final.columns:
                    print(f"  ⚠️ Columna {col} no encontrada, agregando valores por defecto")
                    if col in ['cantidad_existencias', 'valor_existencias', 'costo_promedio', 'rotacion_semanas']:
                        df_final[col] = 0.0
                    elif col in ['rotacion_dias', 'dias_stock']:
                        df_final[col] = 30
                    elif col == 'nivel_stock':
                        df_final[col] = 'NORMAL'
                    elif col in ['fecha_creacion', 'fecha_actualizacion']:
                        df_final[col] = datetime.now()
                    elif col == 'fecha':
                        df_final[col] = self.fecha_proceso.date()
                    elif col == 'pdv_id' and 'pdv_id' not in df_final.columns:
                        df_final[col] = -1  # Valor por defecto
            
            # Cargar en la base de datos
            registros_insertados = 0
            
            for pdv_id in df_final['pdv_id'].unique():
                # Filtrar por PDV
                df_pdv = df_final[df_final['pdv_id'] == pdv_id]
                
                with self.engine.connect() as connection:
                    # Eliminar registros existentes para este PDV y fecha
                    fecha_str = self.fecha_proceso.date().isoformat()
                    
                    delete_query = f"""
                    DELETE FROM fact_inventarios 
                    WHERE pdv_id = {pdv_id} AND fecha = '{fecha_str}'
                    """
                    
                    connection.execute(text(delete_query))
                    connection.commit()
                    
                    # Insertar los nuevos registros
                    df_pdv.to_sql(
                        'fact_inventarios', 
                        connection, 
                        if_exists='append', 
                        index=False
                    )
                    
                    registros_insertados += len(df_pdv)
            
            print(f"  ✅ {registros_insertados} registros cargados exitosamente")
            return registros_insertados
                
        except Exception as e:
            import traceback
            print(f"❌ Error al cargar datos: {str(e)}")
            traceback.print_exc()
            return 0
    
    def ejecutar_etl(self):
        """Ejecuta el proceso ETL completo"""
        try:
            print("\n" + "=" * 50)
            print("PROCESO ETL PARA FACT_INVENTARIOS")
            print("=" * 50)
            print(f"Fecha de proceso: {self.fecha_proceso}")
            
            # 1. Extraer tablas de staging
            pdvs_info = self.extraer_tablas_staging()
            
            if not pdvs_info:
                print("❌ No se encontraron tablas de staging para procesar")
                return False
            
            # 2. Procesar cada PDV
            total_registros = 0
            dfs_totales = []
            
            for info in pdvs_info:
                print(f"\nProcesando {info['tabla']} (PDV: {info['nombre_pdv']}, Código: {info['codigo_pdv']})...")
                
                # Extraer datos
                df = self.extraer_datos_inventario(info['tabla'])
                
                if df.empty:
                    print(f"  ⚠️ No hay datos para PDV {info['nombre_pdv']}")
                    continue
                
                # Transformar datos
                df_trans = self.transformar_datos(df, info)
                
                if not df_trans.empty:
                    dfs_totales.append(df_trans)
            
            # 3. Consolidar todos los datos
            if dfs_totales:
                df_consolidado = pd.concat(dfs_totales, ignore_index=True)
                
                # 4. Cargar datos consolidados
                registros_cargados = self.cargar_datos(df_consolidado)
                total_registros += registros_cargados
            
            print("\n" + "=" * 50)
            print(f"RESUMEN DEL PROCESO ETL")
            print(f"PDVs procesados: {len(pdvs_info)}")
            print(f"Total de registros cargados: {total_registros}")
            print("=" * 50)
            
            return total_registros > 0
            
        except Exception as e:
            import traceback
            print(f"❌ Error en el proceso ETL: {str(e)}")
            traceback.print_exc()
            return False


# Función principal
if __name__ == "__main__":
    etl = ETLFactInventarios()
    resultado = etl.ejecutar_etl()
    
    if resultado:
        print("\n✅ Proceso ETL completado exitosamente")
    else:
        print("\n❌ El proceso ETL no pudo completarse correctamente")
