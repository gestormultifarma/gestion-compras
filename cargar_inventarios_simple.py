# cargar_inventarios_simple.py
"""
Script simplificado para cargar datos de inventario_caja desde tablas staging
a la tabla fact_inventarios.

Este script utiliza consultas SQL directas para evitar problemas con pandas y SQLAlchemy.
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def cargar_inventarios():
    """
    Carga los datos de inventario_caja desde las tablas staging a fact_inventarios.
    Utiliza SQL directo para mayor estabilidad.
    """
    try:
        print("\n" + "=" * 60)
        print("CARGA DE DATOS DE INVENTARIO EN FACT_INVENTARIOS")
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
                
            # 2. Limpieza previa de datos en fact_inventarios para la fecha actual
            print(f"\nLimpiando registros existentes para la fecha {fecha_str}...")
            delete_query = f"""
            DELETE FROM fact_inventarios 
            WHERE fecha = '{fecha_str}'
            """
            conn.execute(text(delete_query))
            conn.commit()
            
            # 3. Procesar cada tabla staging
            total_registros = 0
            
            for tabla in tablas_staging:
                try:
                    print(f"\nProcesando tabla: {tabla}")
                    
                    # Extraer código PDV de la tabla
                    partes = tabla.replace('stg_inventario_', '').split(' ')
                    codigo_pdv = partes[-1] if len(partes) > 1 else "0"
                    
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
                        # Si no encontramos el PDV, usamos un valor por defecto
                        pdv_id = 1  # Asumimos que 1 es un ID válido
                    
                    # Insertar datos directamente en fact_inventarios
                    insert_query = f"""
                    INSERT INTO fact_inventarios 
                    (fecha, cantidad_existencias, valor_existencias, costo_promedio, 
                    rotacion_dias, rotacion_semanas, dias_stock, nivel_stock, 
                    fecha_creacion, fecha_actualizacion, pdv_id)
                    SELECT 
                        '{fecha_str}' as fecha,
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
                    
                    result = conn.execute(text(insert_query))
                    conn.commit()
                    
                    # Contar registros insertados
                    count_query = f"""
                    SELECT COUNT(*) 
                    FROM fact_inventarios 
                    WHERE fecha = '{fecha_str}' AND pdv_id = {pdv_id}
                    """
                    count = conn.execute(text(count_query)).scalar()
                    
                    print(f"  ✅ {count} registros cargados desde {tabla}")
                    total_registros += count
                    
                except Exception as e:
                    print(f"  ❌ Error al procesar tabla {tabla}: {str(e)}")
                    continue
            
            # 4. Resumen final
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
    
    resultado = cargar_inventarios()
    
    if resultado:
        print("\n✅ Proceso de carga completado exitosamente")
    else:
        print("\n❌ El proceso de carga no pudo completarse correctamente")
