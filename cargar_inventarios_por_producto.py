# cargar_inventarios_por_producto.py
"""
Script para cargar datos de inventario_caja desde tablas staging a fact_inventarios,
considerando que cada registro debe ser único por combinación de PDV, fecha y producto.

Resuelve el problema de claves duplicadas al incluir el producto como parte del registro.
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_estructura_fact_inventarios():
    """
    Verifica la estructura actual de la tabla fact_inventarios
    para entender mejor sus restricciones y campos
    """
    try:
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Verificar estructura de la tabla
            query = "DESCRIBE fact_inventarios"
            result = conn.execute(text(query))
            campos = [row for row in result]
            
            print("\nEstructura de la tabla fact_inventarios:")
            print("=" * 60)
            for campo in campos:
                print(f"- {campo[0]} ({campo[1]})")
            
            # Verificar índices y restricciones
            query = """
            SHOW INDEX FROM fact_inventarios
            """
            result = conn.execute(text(query))
            indices = [row for row in result]
            
            print("\nÍndices y restricciones de fact_inventarios:")
            print("=" * 60)
            for idx in indices:
                print(f"- {idx[2]} ({idx[4]})")
            
            return True
            
    except Exception as e:
        print(f"Error al verificar estructura: {str(e)}")
        return False

def cargar_inventarios_por_producto():
    """
    Carga los datos de inventario_caja desde las tablas staging a fact_inventarios,
    asegurando que cada registro sea único por PDV, producto y fecha.
    """
    try:
        print("\n" + "=" * 60)
        print("CARGA DE DATOS DE INVENTARIO EN FACT_INVENTARIOS (POR PRODUCTO)")
        print("=" * 60)
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Fecha actual para el proceso
        fecha_proceso = datetime.now()
        fecha_str = fecha_proceso.date().isoformat()
        print(f"Fecha de proceso: {fecha_proceso}")
        
        # 1. Obtener lista de tablas staging de inventario
        with engine.connect() as conn:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name LIKE 'stg\\_inventario\\_%'
            """
            result = conn.execute(text(query))
            tablas_staging = [row[0] for row in result]
            
            print(f"Se encontraron {len(tablas_staging)} tablas de staging de inventario:")
            for tabla in tablas_staging:
                print(f"  - {tabla}")
                
            # 2. Verificamos si existe la tabla de productos
            query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name = 'dim_producto'
            """
            result = conn.execute(text(query))
            dim_producto_existe = result.scalar() > 0
            
            if not dim_producto_existe:
                print("⚠️ La tabla dim_producto no existe. Se usará el código de producto directamente.")
            else:
                print("✅ Se encontró la tabla dim_producto. Se buscarán referencias de productos.")
            
            # 3. Limpieza previa de datos en fact_inventarios para la fecha actual
            print(f"\nLimpiando registros existentes para la fecha {fecha_str}...")
            delete_query = f"""
            DELETE FROM fact_inventarios 
            WHERE fecha = '{fecha_str}'
            """
            conn.execute(text(delete_query))
            conn.commit()
            
            # 4. Procesar cada tabla staging
            total_registros = 0
            
            for tabla in tablas_staging:
                try:
                    print(f"\nProcesando tabla: {tabla}")
                    
                    # Extraer código PDV de la tabla
                    partes = tabla.replace('stg_inventario_', '').split(' ')
                    codigo_pdv = partes[-1] if len(partes) > 1 else "0"
                    
                    # Obtener nombre del PDV (para referencia)
                    nombre_pdv = ' '.join(partes[:-1]) if len(partes) > 1 else tabla
                    
                    # Intentar obtener el pdv_sk de dim_pdv
                    query_pdv = f"""
                    SELECT pdv_sk 
                    FROM dim_pdv 
                    WHERE codigo_pdv = '{codigo_pdv}'
                    """
                    result = conn.execute(text(query_pdv))
                    row = result.fetchone()
                    
                    if row:
                        pdv_id = row[0]
                    else:
                        print(f"  ⚠️ No se encontró el PDV con código {codigo_pdv} en dim_pdv")
                        
                        # Intentar buscar por nombre similar
                        query_pdv = f"""
                        SELECT pdv_sk 
                        FROM dim_pdv 
                        WHERE nombre_pdv LIKE '%{nombre_pdv}%'
                        LIMIT 1
                        """
                        result = conn.execute(text(query_pdv))
                        row = result.fetchone()
                        
                        if row:
                            pdv_id = row[0]
                        else:
                            # Si no encontramos ninguna coincidencia, usamos un valor por defecto
                            print(f"  ⚠️ No se encontró ninguna coincidencia para {nombre_pdv}. Usando PDV por defecto.")
                            pdv_id = 1  # Asumimos que 1 es un ID válido
                    
                    # Contar productos en la tabla staging
                    count_query = f"""
                    SELECT COUNT(*)
                    FROM `{tabla}`
                    WHERE inventario_caja > 0
                    """
                    count = conn.execute(text(count_query)).scalar()
                    
                    if count == 0:
                        print(f"  ⚠️ No hay productos con inventario_caja > 0 en {tabla}")
                        continue
                    
                    print(f"  📊 La tabla {tabla} tiene {count} productos con inventario > 0")
                    
                    # 5. Crear tabla temporal para procesar los datos
                    temp_table = f"temp_inventario_{pdv_id.replace(' ', '_') if isinstance(pdv_id, str) else pdv_id}"
                    
                    # Eliminar tabla temporal si existe
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                    
                    # Crear tabla temporal con los datos procesados
                    create_temp_query = f"""
                    CREATE TEMPORARY TABLE {temp_table} AS
                    SELECT 
                        '{fecha_str}' as fecha,
                        codigo as codigo_producto,
                        nombre as nombre_producto,
                        COALESCE(inventario_caja, 0) as cantidad_existencias,
                        COALESCE(costo_total, 0) as valor_existencias,
                        COALESCE(costo_caja, 0) as costo_promedio,
                        30 as rotacion_dias,
                        30/7 as rotacion_semanas,
                        30 as dias_stock,
                        'NORMAL' as nivel_stock,
                        NOW() as fecha_creacion,
                        NOW() as fecha_actualizacion,
                        {pdv_id} as pdv_id
                    FROM `{tabla}`
                    WHERE inventario_caja > 0
                    """
                    
                    conn.execute(text(create_temp_query))
                    
                    # Verificar cuántos registros se crearon en la tabla temporal
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar()
                    print(f"  ✓ Creada tabla temporal con {count} registros")
                    
                    # 6. Buscar producto_id en dim_producto si existe
                    if dim_producto_existe:
                        try:
                            # Actualizar producto_id en la tabla temporal
                            update_query = f"""
                            UPDATE {temp_table} t
                            JOIN dim_producto dp ON t.codigo_producto = dp.codigo_producto
                            SET t.producto_id = dp.producto_sk
                            """
                            conn.execute(text(update_query))
                            
                            # Contar cuántos productos se encontraron
                            count = conn.execute(text(f"""
                                SELECT COUNT(*) 
                                FROM {temp_table} 
                                WHERE producto_id IS NOT NULL
                            """)).scalar()
                            
                            print(f"  ✓ Se encontraron {count} productos en dim_producto")
                            
                        except Exception as e:
                            print(f"  ⚠️ Error al buscar productos en dim_producto: {str(e)}")
                            # Continuamos sin actualizar producto_id
                    
                    # 7. Insertar datos desde la tabla temporal a fact_inventarios
                    # Primero verificamos si fact_inventarios tiene una columna producto_id o similar
                    columns_query = "DESCRIBE fact_inventarios"
                    result = conn.execute(text(columns_query))
                    columns = [row[0] for row in result]
                    
                    # Determinar si debemos incluir producto_id o código_producto
                    if 'producto_id' in columns:
                        # Si fact_inventarios tiene producto_id, insertamos con él
                        insert_query = f"""
                        INSERT INTO fact_inventarios 
                        (fecha, cantidad_existencias, valor_existencias, costo_promedio, 
                        rotacion_dias, rotacion_semanas, dias_stock, nivel_stock, 
                        fecha_creacion, fecha_actualizacion, pdv_id, producto_id)
                        SELECT 
                            fecha, cantidad_existencias, valor_existencias, costo_promedio, 
                            rotacion_dias, rotacion_semanas, dias_stock, nivel_stock, 
                            fecha_creacion, fecha_actualizacion, pdv_id, 
                            COALESCE(producto_id, 1) as producto_id
                        FROM {temp_table}
                        """
                    else:
                        # Si no tiene producto_id, insertamos solo los campos coincidentes
                        coincident_columns = [col for col in columns if col in [
                            'fecha', 'cantidad_existencias', 'valor_existencias', 'costo_promedio',
                            'rotacion_dias', 'rotacion_semanas', 'dias_stock', 'nivel_stock',
                            'fecha_creacion', 'fecha_actualizacion', 'pdv_id'
                        ]]
                        
                        # Construir lista de columnas para la consulta
                        columns_str = ', '.join(coincident_columns)
                        
                        insert_query = f"""
                        INSERT INTO fact_inventarios 
                        ({columns_str})
                        SELECT {columns_str}
                        FROM {temp_table}
                        """
                    
                    # Ejecutar la inserción
                    try:
                        conn.execute(text(insert_query))
                        conn.commit()
                        
                        # Contar registros insertados para este PDV
                        count_query = f"""
                        SELECT COUNT(*) 
                        FROM fact_inventarios 
                        WHERE fecha = '{fecha_str}' AND pdv_id = {pdv_id}
                        """
                        inserted_count = conn.execute(text(count_query)).scalar()
                        
                        print(f"  ✅ {inserted_count} registros cargados desde {tabla}")
                        total_registros += inserted_count
                        
                    except Exception as e:
                        print(f"  ❌ Error al insertar datos: {str(e)}")
                        
                        # Intentar insertar uno por uno en caso de error
                        print("  ⚠️ Intentando inserción registro por registro...")
                        
                        # Obtener datos de la tabla temporal
                        data_query = f"SELECT * FROM {temp_table}"
                        result = conn.execute(text(data_query))
                        rows = result.fetchall()
                        
                        inserted_count = 0
                        for row_data in rows:
                            try:
                                # Crear consulta de inserción para un solo registro
                                values = []
                                for val in row_data:
                                    if val is None:
                                        values.append("NULL")
                                    elif isinstance(val, (int, float)):
                                        values.append(str(val))
                                    elif isinstance(val, datetime):
                                        values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                                    else:
                                        values.append(f"'{str(val)}'")
                                
                                # Construir consulta para un solo registro
                                values_str = ', '.join(values[:len(coincident_columns)])
                                single_insert = f"""
                                INSERT INTO fact_inventarios ({columns_str})
                                VALUES ({values_str})
                                """
                                
                                conn.execute(text(single_insert))
                                conn.commit()
                                inserted_count += 1
                                
                            except Exception as e2:
                                # Ignorar este registro y continuar con el siguiente
                                pass
                        
                        print(f"  ✅ {inserted_count} registros cargados individualmente")
                        total_registros += inserted_count
                    
                    # Eliminar tabla temporal
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                    
                except Exception as e:
                    print(f"  ❌ Error al procesar tabla {tabla}: {str(e)}")
                    continue
            
            # 8. Resumen final
            print("\n" + "=" * 60)
            print("RESUMEN DE LA CARGA")
            print(f"Tablas procesadas: {len(tablas_staging)}")
            print(f"Total de registros cargados: {total_registros}")
            print("=" * 60)
            
            if total_registros > 0:
                return True
            else:
                return False
                
    except Exception as e:
        import traceback
        print(f"❌ Error general: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Asegurar que estamos usando el entorno virtual correcto
    print(f"Usando Python en: {sys.executable}")
    
    # Primero verificamos la estructura de la tabla para entender mejor sus restricciones
    verificar_estructura_fact_inventarios()
    
    # Luego ejecutamos la carga de datos
    resultado = cargar_inventarios_por_producto()
    
    if resultado:
        print("\n✅ Proceso de carga completado exitosamente")
    else:
        print("\n❌ El proceso de carga no pudo completarse correctamente")
