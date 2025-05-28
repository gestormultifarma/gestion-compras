# run_etl_per_pdv.py

import os
import pandas as pd
import warnings
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

# Suprimir advertencias de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

def procesar_pdv(codigo_pdv, nombre_pdv, pdv_sk):
    """Procesa un único PDV y carga sus datos en fact_rotacion"""
    try:
        print(f"\n🏪 Procesando PDV: {nombre_pdv} (Código: {codigo_pdv}, SK: {pdv_sk})")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # 1. Obtener tablas de rotación para este PDV
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name LIKE 'stg_rotacion_%'
            AND table_name LIKE :pattern
        """)
        
        with engine.connect() as connection:
            resultado = connection.execute(query, {"pattern": f"%_{codigo_pdv}_%"})
            tablas = [row[0] for row in resultado]
            
        if not tablas:
            print(f"⚠️ No se encontraron tablas de rotación para PDV {codigo_pdv}")
            return False
            
        print(f"📊 Encontradas {len(tablas)} tablas para PDV {codigo_pdv}")
        
        # 2. Procesar cada tabla del PDV
        dfs_pdv = []
        
        for tabla in tablas:
            print(f"📋 Procesando tabla: {tabla}")
            
            # Comprobar columnas de la tabla
            query_columnas = text(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'gestion_compras'
                AND TABLE_NAME = '{tabla}'
            """)
            
            with engine.connect() as connection:
                columnas = [row[0].lower() for row in connection.execute(query_columnas)]
                
            print(f"   - Columnas: {', '.join(columnas[:5])}...")
            
            # Verificar si tenemos las columnas mínimas necesarias
            columnas_obligatorias = ["codigo", "nombre"]
            
            if not all(col in columnas for col in columnas_obligatorias):
                print(f"⚠️ La tabla {tabla} no tiene todas las columnas obligatorias")
                continue
                
            # Comprobar registros
            query_count = text(f"SELECT COUNT(*) FROM `{tabla}`")
            with engine.connect() as connection:
                count = connection.execute(query_count).scalar()
                
            print(f"   - Registros: {count}")
            
            if count == 0:
                print(f"⚠️ La tabla {tabla} no tiene registros")
                continue
                
            # Extraer datos
            try:
                # Construir una consulta adaptada a las columnas disponibles
                campos_select = [
                    "codigo as codigo_producto",
                    "nombre as nombre_producto"
                ]
                
                # Campos opcionales
                mapeo_campos = {
                    "venta_caja": ["venta_caja", "venta_cajas"],
                    "venta_blister": ["venta_blister", "venta_blisters"],
                    "venta_unidad": ["venta_unidad", "venta_unidades", "unidades"],
                    "costo_unitario": ["costo_unitario", "costo"],
                    "inventario_unidad": ["inventario_unidad", "inventario", "stock", "existencia"]
                }
                
                for campo_destino, posibles_nombres in mapeo_campos.items():
                    encontrado = False
                    for nombre in posibles_nombres:
                        if nombre in columnas:
                            campos_select.append(f"{nombre} as {campo_destino}")
                            encontrado = True
                            break
                    if not encontrado:
                        campos_select.append(f"0 as {campo_destino}")
                
                # Ejecutar consulta
                query = text(f"""
                    SELECT 
                        {', '.join(campos_select)},
                        NOW() as fecha
                    FROM `{tabla}`
                """)
                
                with engine.connect() as connection:
                    df = pd.read_sql(query, connection)
                    
                if df.empty:
                    print(f"⚠️ No se obtuvieron datos de la tabla {tabla}")
                    continue
                    
                # Agregar información del PDV
                df['codigo_pdv'] = codigo_pdv
                df['pdv_sk'] = pdv_sk
                
                # Calcular campos adicionales
                if 'venta_total' not in df.columns:
                    # Cálculo básico de venta total
                    venta_cajas = df['venta_caja'] if 'venta_caja' in df.columns else 0
                    venta_unidades = df['venta_unidad'] if 'venta_unidad' in df.columns else 0
                    costo_unitario = df['costo_unitario'] if 'costo_unitario' in df.columns else 0
                    
                    # Estimación simple de precio de venta (costo + 30% margen)
                    precio_venta = costo_unitario * 1.3
                    
                    df['venta_total'] = venta_unidades * precio_venta
                
                # Calcular días de inventario y rotación
                df['inventario_unidades_final'] = df['inventario_unidad']
                df['dias_inventario'] = 30  # Valor predeterminado
                df['rotacion_mes'] = 1     # Valor predeterminado
                
                # Agregar campos requeridos para fact_rotacion
                df['fecha_sk'] = None     # Se llenará después
                df['producto_sk'] = None  # Se llenará después
                df['inventario_unidades_inicial'] = 0  # No tenemos datos históricos
                df['venta_cajas'] = df['venta_caja'] if 'venta_caja' in df.columns else 0
                df['venta_blisters'] = df['venta_blister'] if 'venta_blister' in df.columns else 0
                df['venta_unidades'] = df['venta_unidad'] if 'venta_unidad' in df.columns else 0
                
                print(f"✅ Procesados {len(df)} registros de la tabla {tabla}")
                dfs_pdv.append(df)
                
            except Exception as e:
                print(f"❌ Error al procesar tabla {tabla}: {str(e)}")
                traceback.print_exc()
        
        # 3. Consolidar datos de este PDV
        if not dfs_pdv:
            print(f"❌ No se pudieron procesar datos para PDV {codigo_pdv}")
            return False
            
        df_consolidado = pd.concat(dfs_pdv, ignore_index=True)
        
        # Eliminar duplicados (mismo producto en diferentes tablas)
        df_consolidado = df_consolidado.drop_duplicates(subset=['codigo_producto'])
        
        print(f"📊 Total registros consolidados para PDV {codigo_pdv}: {len(df_consolidado)}")
        
        # 4. Buscar producto_sk para cada código de producto
        print("🔄 Buscando product_sk para cada producto...")
        
        # Crear tabla temporal para la búsqueda
        nombre_tabla_temp = f"temp_productos_{codigo_pdv}"
        df_consolidado[['codigo_producto', 'nombre_producto']].to_sql(
            nombre_tabla_temp, 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        
        # Consulta para buscar producto_sk
        query_productos = text(f"""
            UPDATE {nombre_tabla_temp} t
            JOIN dim_producto p ON t.codigo_producto = p.codigo_producto
            SET t.producto_sk = p.producto_sk
        """)
        
        with engine.connect() as connection:
            connection.execute(query_productos)
            connection.commit()
            
        # Recuperar los producto_sk
        query_recuperar = text(f"""
            SELECT codigo_producto, producto_sk 
            FROM {nombre_tabla_temp}
            WHERE producto_sk IS NOT NULL
        """)
        
        with engine.connect() as connection:
            df_productos = pd.read_sql(query_recuperar, connection)
            
        # Limpiar
        query_drop = text(f"DROP TABLE IF EXISTS {nombre_tabla_temp}")
        with engine.connect() as connection:
            connection.execute(query_drop)
            connection.commit()
            
        # Crear un diccionario para mapear código_producto -> producto_sk
        mapeo_productos = dict(zip(df_productos['codigo_producto'], df_productos['producto_sk']))
        
        # Actualizar producto_sk en el DataFrame
        df_consolidado['producto_sk'] = df_consolidado['codigo_producto'].map(mapeo_productos)
        
        # Eliminar registros sin producto_sk
        df_consolidado = df_consolidado.dropna(subset=['producto_sk'])
        
        print(f"✅ Se encontraron {len(df_consolidado)} productos con producto_sk válido")
        
        # 5. Preparar para inserción
        df_final = df_consolidado.copy()
        
        # Asegurar que todos los campos necesarios estén presentes
        campos_requeridos = [
            'producto_sk', 'pdv_sk', 'codigo_pdv', 'venta_total', 'venta_cajas', 
            'venta_blisters', 'venta_unidades', 'costo_unitario', 'dias_inventario', 
            'rotacion_mes', 'inventario_unidades_final'
        ]
        
        for campo in campos_requeridos:
            if campo not in df_final.columns:
                df_final[campo] = 0
                
        # Agregar fecha de carga
        df_final['fecha_carga'] = datetime.now()
        df_final['fecha_actualizacion'] = datetime.now()
        
        # 6. Insertar en fact_rotacion
        print(f"📤 Insertando {len(df_final)} registros en fact_rotacion para PDV {codigo_pdv}...")
        
        try:
            # Primero eliminar registros existentes para este PDV
            query_delete = text(f"""
                DELETE FROM fact_rotacion
                WHERE codigo_pdv = '{codigo_pdv}'
            """)
            
            with engine.connect() as connection:
                connection.execute(query_delete)
                connection.commit()
                
            # Insertar los nuevos registros
            df_final.to_sql('fact_rotacion', con=engine, if_exists='append', index=False)
            
            # Verificar inserción
            query_count = text(f"""
                SELECT COUNT(*) 
                FROM fact_rotacion
                WHERE codigo_pdv = '{codigo_pdv}'
            """)
            
            with engine.connect() as connection:
                count_after = connection.execute(query_count).scalar()
                
            print(f"✅ Carga completada. Se insertaron {count_after} registros para PDV {codigo_pdv}")
            return True
            
        except Exception as e:
            print(f"❌ Error al insertar datos en fact_rotacion: {str(e)}")
            traceback.print_exc()
            return False
            
    except Exception as e:
        print(f"❌ Error general al procesar PDV {codigo_pdv}: {str(e)}")
        traceback.print_exc()
        return False


def ejecutar_etl_por_pdv():
    """Ejecuta ETL por cada PDV individualmente"""
    try:
        print("🚀 Iniciando ETL por PDV para fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Obtener todos los PDVs
        query = text("""
            SELECT pdv_sk, codigo_pdv, nombre_pdv
            FROM dim_pdv
            WHERE estado_pdv = 'activo'
            ORDER BY pdv_sk
        """)
        
        with engine.connect() as connection:
            result = connection.execute(query)
            pdvs = [
                {
                    'pdv_sk': row[0],
                    'codigo_pdv': str(row[1]),
                    'nombre_pdv': row[2]
                }
                for row in result
            ]
            
        if not pdvs:
            print("❌ No se encontraron PDVs en la dimensión")
            return
            
        print(f"🏪 Se procesarán {len(pdvs)} puntos de venta")
        
        # Procesar cada PDV
        pdvs_exitosos = 0
        for pdv in pdvs:
            resultado = procesar_pdv(
                pdv['codigo_pdv'],
                pdv['nombre_pdv'],
                pdv['pdv_sk']
            )
            
            if resultado:
                pdvs_exitosos += 1
                
        # Resumir resultados
        print(f"\n📊 Resumen de ejecución:")
        print(f"  • PDVs procesados: {len(pdvs)}")
        print(f"  • PDVs cargados exitosamente: {pdvs_exitosos}")
        
        # Verificar resultados finales
        query_final = text("""
            SELECT codigo_pdv, COUNT(*) as total
            FROM fact_rotacion
            GROUP BY codigo_pdv
        """)
        
        with engine.connect() as connection:
            resultados = pd.read_sql(query_final, connection)
            
        print("\n📊 Registros en fact_rotacion por PDV:")
        print(resultados.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error general: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    ejecutar_etl_por_pdv()
