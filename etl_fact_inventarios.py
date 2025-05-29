# etl_fact_inventarios.py
"""
Script ETL para poblar la tabla fact_inventarios con datos de inventario_caja
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
                AND table_name LIKE 'stg_inventario_%'
                """
                result = connection.execute(text(query))
                tablas_staging = [row[0] for row in result]
                
                print(f"Se encontraron {len(tablas_staging)} tablas de staging de inventario")
                
                # Extraer el código y nombre de PDV de cada tabla
                pdvs_info = []
                for tabla in tablas_staging:
                    # El formato es stg_inventario_{nombre_pdv}{codigo_pdv}
                    partes = tabla.split('_')
                    if len(partes) >= 3:
                        # El último elemento contiene el código de PDV
                        ultima_parte = partes[-1]
                        
                        # Intentar encontrar el código numérico al final
                        codigo_pdv = ""
                        nombre_pdv = ""
                        
                        # Buscar el punto donde termina el nombre y empieza el código
                        for i, char in enumerate(ultima_parte):
                            if char.isdigit():
                                codigo_pdv = ultima_parte[i:]
                                nombre_pdv = ultima_parte[:i]
                                break
                        
                        # Si no pudimos separar, usar toda la última parte como nombre
                        if not codigo_pdv:
                            nombre_pdv = ultima_parte
                            
                            # Intentar buscar el código en la tabla dim_pdv
                            query_pdv = f"""
                            SELECT codigo_pdv 
                            FROM dim_pdv 
                            WHERE nombre_pdv LIKE '%{nombre_pdv}%'
                            LIMIT 1
                            """
                            
                            try:
                                result = connection.execute(text(query_pdv))
                                codigo_row = result.fetchone()
                                if codigo_row:
                                    codigo_pdv = codigo_row[0]
                            except:
                                # Si falla, intentamos obtener el código de otra manera
                                pass
                        
                        # Si todavía no tenemos código, usar solo las partes numéricas
                        if not codigo_pdv:
                            codigo_pdv = ''.join(filter(str.isdigit, ultima_parte))
                            
                        # Normalizar el nombre del PDV
                        if not nombre_pdv:
                            # Obtener nombre desde partes anteriores de la tabla
                            nombre_pdv = partes[-2] if len(partes) > 3 else "desconocido"
                        
                        # Agregar a la lista de PDVs
                        pdvs_info.append({
                            'tabla': tabla,
                            'codigo_pdv': codigo_pdv,
                            'nombre_pdv': nombre_pdv
                        })
                
                return pdvs_info
                
        except Exception as e:
            print(f"❌ Error al extraer tablas staging: {str(e)}")
            return []
    
    def extraer_datos_inventario(self, tabla, codigo_pdv):
        """Extrae los datos de inventario_caja de una tabla de staging"""
        try:
            print(f"📋 Extrayendo datos de {tabla}...")
            
            with self.engine.connect() as connection:
                # Consulta para obtener los datos relevantes
                query = f"""
                SELECT 
                    codigo,
                    nombre,
                    COALESCE(inventario_caja, 0) as inventario_caja,
                    COALESCE(costo_caja, 0) as costo_caja,
                    COALESCE(costo_total, 0) as costo_total
                FROM {tabla}
                """
                
                df = pd.read_sql(query, connection)
                
                # Agregar la información del PDV
                df['codigo_pdv'] = codigo_pdv
                
                print(f"  ✅ Extraídos {len(df)} registros")
                return df
                
        except Exception as e:
            print(f"❌ Error al extraer datos de {tabla}: {str(e)}")
            return pd.DataFrame()
    
    def transformar_datos(self, df, nombre_pdv):
        """Transforma y enriquece los datos de inventario"""
        try:
            print(f"🔄 Transformando datos para PDV {nombre_pdv}...")
            
            if df.empty:
                print("  ⚠️ No hay datos para transformar")
                return df
            
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
            # ya que no tenemos esa información en las tablas de staging
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
            
            # Buscar el pdv_id correspondiente al código de PDV
            with self.engine.connect() as connection:
                query = f"""
                SELECT pdv_sk 
                FROM dim_pdv 
                WHERE codigo_pdv = '{df['codigo_pdv'].iloc[0]}'
                LIMIT 1
                """
                
                result = connection.execute(text(query))
                row = result.fetchone()
                
                if row:
                    pdv_id = row[0]
                else:
                    # Si no encontramos el PDV, intentamos insertarlo
                    print(f"  ⚠️ PDV {df['codigo_pdv'].iloc[0]} no encontrado en dim_pdv")
                    
                    # Intentar buscar por nombre similar
                    query = f"""
                    SELECT pdv_sk 
                    FROM dim_pdv 
                    WHERE nombre_pdv LIKE '%{nombre_pdv}%'
                    LIMIT 1
                    """
                    
                    result = connection.execute(text(query))
                    row = result.fetchone()
                    
                    if row:
                        pdv_id = row[0]
                    else:
                        # Si realmente no existe, crear un ID temporal
                        # En un caso real, deberíamos insertarlo en dim_pdv
                        print(f"  ⚠️ Creando ID temporal para PDV {nombre_pdv}")
                        pdv_id = -1  # Valor temporal
            
            df['pdv_id'] = pdv_id
            
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
            
            # Filtrar el DataFrame
            df_final = df[columnas_fact]
            
            # Cargar en la base de datos
            with self.engine.connect() as connection:
                # Verificar si hay registros para la misma fecha y PDV
                for pdv in df['pdv_id'].unique():
                    fecha_str = self.fecha_proceso.date().isoformat()
                    
                    query = f"""
                    DELETE FROM fact_inventarios 
                    WHERE pdv_id = {pdv} AND fecha = '{fecha_str}'
                    """
                    
                    connection.execute(text(query))
                    connection.commit()
                
                # Insertar los nuevos registros
                df_final.to_sql(
                    'fact_inventarios', 
                    connection, 
                    if_exists='append', 
                    index=False
                )
                
                # Verificar cuántos registros se insertaron
                query = f"""
                SELECT COUNT(*) 
                FROM fact_inventarios 
                WHERE fecha = '{self.fecha_proceso.date().isoformat()}'
                """
                
                result = connection.execute(text(query))
                count = result.scalar()
                
                print(f"  ✅ {count} registros cargados exitosamente")
                return count
                
        except Exception as e:
            print(f"❌ Error al cargar datos: {str(e)}")
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
                df = self.extraer_datos_inventario(info['tabla'], info['codigo_pdv'])
                
                if df.empty:
                    print(f"  ⚠️ No hay datos para PDV {info['nombre_pdv']}")
                    continue
                
                # Transformar datos
                df_trans = self.transformar_datos(df, info['nombre_pdv'])
                
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
