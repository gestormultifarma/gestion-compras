# corregir_rotacion_cajas.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def corregir_rotacion_cajas():
    """
    Corrige la tabla fact_rotacion para cargar los valores de venta_caja en lugar
    de venta_unidad, asegurando que los datos de rotación sean correctos.
    """
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            print("Corrigiendo tabla fact_rotacion para usar valores de venta en CAJAS...")
            
            # Limpiar la tabla
            print("Limpiando tabla fact_rotacion...")
            connection.execute(text("TRUNCATE TABLE fact_rotacion"))
            connection.commit()
            
            # Crear tabla temporal para acumular datos de todos los PDV y periodos
            print("Creando tabla temporal para procesar datos...")
            connection.execute(text("""
            DROP TABLE IF EXISTS temp_rotacion_cajas
            """))
            
            connection.execute(text("""
            CREATE TEMPORARY TABLE temp_rotacion_cajas (
                codigo_pdv VARCHAR(20),
                nombre_pdv VARCHAR(100),
                codigo_producto VARCHAR(50),
                nombre_producto VARCHAR(255),
                periodo INT,
                venta_cajas DECIMAL(10,2)
            )
            """))
            connection.commit()
            
            # Procesar cada tabla de staging y cargar datos en la tabla temporal
            print("Procesando tablas de staging...")
            
            # Obtener lista de tablas de rotación
            tablas_rotacion = connection.execute(text("SHOW TABLES LIKE 'stg_rotacion_%'")).fetchall()
            total_tablas = len(tablas_rotacion)
            print(f"Se encontraron {total_tablas} tablas de rotación para procesar")
            
            for i, (tabla,) in enumerate(tablas_rotacion):
                # Extraer información del PDV y período desde el nombre de la tabla
                # Ejemplo: stg_rotacion_de_bella_suiza_40350_1
                partes = tabla.split('_')
                try:
                    # El último elemento es el periodo
                    periodo = int(partes[-1])
                    # El penúltimo es el código de PDV
                    codigo_pdv = partes[-2]
                    # Los elementos intermedios forman el nombre del PDV
                    pdv_nombre_partes = partes[3:-2]
                    nombre_pdv = '_'.join(pdv_nombre_partes)
                    
                    # Obtener datos de ventas en CAJAS
                    query = f"""
                    INSERT INTO temp_rotacion_cajas (
                        codigo_pdv, 
                        nombre_pdv,
                        codigo_producto,
                        nombre_producto,
                        periodo,
                        venta_cajas
                    )
                    SELECT 
                        '{codigo_pdv}' as codigo_pdv,
                        '{nombre_pdv}' as nombre_pdv,
                        codigo as codigo_producto,
                        nombre as nombre_producto,
                        {periodo} as periodo,
                        COALESCE(venta_caja, 0) as venta_cajas
                    FROM {tabla}
                    """
                    connection.execute(text(query))
                    connection.commit()
                    
                    # Mostrar progreso
                    if (i+1) % 10 == 0 or (i+1) == total_tablas:
                        print(f"Procesadas {i+1}/{total_tablas} tablas...")
                    
                except Exception as e:
                    print(f"Error procesando tabla {tabla}: {str(e)}")
            
            # Actualizar datos de nombre_pdv desde dim_pdv
            print("Actualizando nombres de PDV...")
            connection.execute(text("""
            UPDATE temp_rotacion_cajas t
            JOIN dim_pdv d ON t.codigo_pdv = d.codigo_pdv
            SET t.nombre_pdv = d.nombre_pdv
            """))
            connection.commit()
            
            # Consolidar datos en fact_rotacion
            print("Consolidando datos en fact_rotacion...")
            
            # Insertar registros consolidados por PDV y producto
            insert_query = """
            INSERT INTO fact_rotacion (
                codigo_pdv,
                nombre_pdv,
                codigo_producto,
                nombre_producto,
                rotacion_m1,
                rotacion_m2,
                rotacion_m3
            )
            SELECT 
                t.codigo_pdv,
                t.nombre_pdv,
                t.codigo_producto,
                MAX(t.nombre_producto) as nombre_producto,
                SUM(CASE WHEN t.periodo = 1 THEN t.venta_cajas ELSE 0 END) as rotacion_m1,
                SUM(CASE WHEN t.periodo = 2 THEN t.venta_cajas ELSE 0 END) as rotacion_m2,
                SUM(CASE WHEN t.periodo = 3 THEN t.venta_cajas ELSE 0 END) as rotacion_m3
            FROM temp_rotacion_cajas t
            GROUP BY t.codigo_pdv, t.nombre_pdv, t.codigo_producto
            """
            connection.execute(text(insert_query))
            connection.commit()
            
            # Verificar resultados
            count_query = "SELECT COUNT(*) FROM fact_rotacion"
            total_registros = connection.execute(text(count_query)).scalar()
            
            # Verificar registros con ventas > 0
            ventas_query = """
            SELECT COUNT(*) 
            FROM fact_rotacion 
            WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0
            """
            total_con_ventas = connection.execute(text(ventas_query)).scalar()
            
            print(f"\nResultados:")
            print(f"Total de registros en fact_rotacion: {total_registros}")
            print(f"Registros con ventas > 0 en algún período: {total_con_ventas}")
            
            # Verificar específicamente el caso mencionado
            verificar_query = """
            SELECT *
            FROM fact_rotacion
            WHERE codigo_pdv = '34060' AND codigo_producto = '100019393'
            """
            
            result = connection.execute(text(verificar_query))
            row = result.fetchone()
            
            if row:
                print("\nVerificación del caso específico mencionado:")
                print(f"PDV: {row[0]} ({row[1]})")
                print(f"Producto: {row[2]} ({row[3]})")
                print(f"Rotación M1: {row[4]}")
                print(f"Rotación M2: {row[5]}")
                print(f"Rotación M3: {row[6]}")
            
            # Mostrar ejemplos de registros con ventas
            ejemplos_query = """
            SELECT *
            FROM fact_rotacion
            WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0
            LIMIT 10
            """
            
            result = connection.execute(text(ejemplos_query))
            rows = result.fetchall()
            
            print("\nEjemplos de registros con ventas (CAJAS):")
            print("=" * 100)
            print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
            print("-" * 100)
            
            for row in rows:
                print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4] or 0:7.2f} | {row[5] or 0:7.2f} | {row[6] or 0:7.2f}")
            
            print("\n✅ Proceso completado exitosamente")
            
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    corregir_rotacion_cajas()
