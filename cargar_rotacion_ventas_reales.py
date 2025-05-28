# cargar_rotacion_ventas_reales.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def cargar_rotacion_ventas_reales():
    """
    Carga datos reales de ventas en la tabla fact_rotacion desde las tablas
    de staging, utilizando las ventas en unidades como valor de rotación.
    """
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            print("Cargando datos reales de ventas en la tabla fact_rotacion...")
            
            # Limpiar la tabla actual
            print("Limpiando tabla fact_rotacion...")
            connection.execute(text("TRUNCATE TABLE fact_rotacion"))
            connection.commit()
            
            # Obtener lista de PDVs
            query_pdvs = """
            SELECT DISTINCT 
                codigo_pdv, 
                nombre_pdv 
            FROM dim_pdv
            """
            pdvs = connection.execute(text(query_pdvs)).fetchall()
            
            total_registros = 0
            
            # Para cada PDV, procesar sus datos de rotación
            for pdv_row in pdvs:
                codigo_pdv = pdv_row[0]
                nombre_pdv = pdv_row[1]
                
                # Obtener nombre normalizado del PDV para buscar tablas de staging
                pdv_nombre_normalizado = nombre_pdv.lower().replace(' ', '_')
                
                print(f"\nProcesando PDV: {nombre_pdv} (código: {codigo_pdv})...")
                
                # Diccionario para acumular datos de los 3 períodos
                productos_data = {}
                
                # Procesar cada período (1, 2, 3)
                for periodo in range(1, 4):
                    tabla_staging = f"stg_rotacion_de_{pdv_nombre_normalizado}_{codigo_pdv}_{periodo}"
                    
                    # Verificar si la tabla existe
                    check_query = f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'gestion_compras' 
                    AND table_name = '{tabla_staging}'
                    """
                    tabla_existe = connection.execute(text(check_query)).scalar() > 0
                    
                    if not tabla_existe:
                        print(f"  - Tabla {tabla_staging} no encontrada, omitiendo periodo {periodo}")
                        continue
                    
                    # Obtener los datos de ventas
                    data_query = f"""
                    SELECT 
                        codigo,
                        nombre,
                        venta_unidad
                    FROM {tabla_staging}
                    """
                    try:
                        rotacion_data = connection.execute(text(data_query)).fetchall()
                        print(f"  - Procesando {len(rotacion_data)} registros del periodo {periodo}")
                        
                        # Procesar cada producto
                        for data_row in rotacion_data:
                            codigo_producto = data_row[0]
                            nombre_producto = data_row[1]
                            venta_unidad = float(data_row[2]) if data_row[2] is not None else 0
                            
                            # Inicializar registro si no existe
                            if codigo_producto not in productos_data:
                                productos_data[codigo_producto] = {
                                    'nombre_producto': nombre_producto,
                                    'rotacion_m1': 0,
                                    'rotacion_m2': 0,
                                    'rotacion_m3': 0
                                }
                            
                            # Actualizar valor de rotación para este período
                            productos_data[codigo_producto][f'rotacion_m{periodo}'] = venta_unidad
                            
                    except Exception as e:
                        print(f"  - Error al procesar tabla {tabla_staging}: {str(e)}")
                        continue
                
                # Insertar datos acumulados en fact_rotacion
                for codigo_producto, datos in productos_data.items():
                    nombre_producto = datos['nombre_producto']
                    rotacion_m1 = datos['rotacion_m1']
                    rotacion_m2 = datos['rotacion_m2']
                    rotacion_m3 = datos['rotacion_m3']
                    
                    # Insertar registro
                    insert_query = f"""
                    INSERT INTO fact_rotacion (
                        codigo_pdv, 
                        nombre_pdv, 
                        codigo_producto, 
                        nombre_producto, 
                        rotacion_m1, 
                        rotacion_m2, 
                        rotacion_m3
                    ) VALUES (
                        '{codigo_pdv}',
                        '{nombre_pdv.replace("'", "''")}',
                        '{codigo_producto}',
                        '{nombre_producto.replace("'", "''")}',
                        {rotacion_m1},
                        {rotacion_m2},
                        {rotacion_m3}
                    )
                    """
                    connection.execute(text(insert_query))
                
                # Confirmar cambios para este PDV
                connection.commit()
                
                # Contar registros procesados para este PDV
                count_query = f"""
                SELECT COUNT(*) 
                FROM fact_rotacion 
                WHERE codigo_pdv = '{codigo_pdv}'
                """
                pdv_count = connection.execute(text(count_query)).scalar()
                print(f"  ✅ Procesados {pdv_count} registros para {nombre_pdv}")
                total_registros += pdv_count
            
            # Verificar los resultados
            print(f"\n✅ Carga completada. Total de registros en fact_rotacion: {total_registros}")
            
            # Mostrar ejemplos
            ejemplos = """
            SELECT * FROM fact_rotacion LIMIT 10
            """
            result = connection.execute(text(ejemplos))
            rows = result.fetchall()
            
            print("\nEjemplos de registros:")
            print("=" * 100)
            print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
            print("-" * 100)
            
            for row in rows:
                print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4] or 0:7.2f} | {row[5] or 0:7.2f} | {row[6] or 0:7.2f}")
            
    except Exception as e:
        import traceback
        print(f"❌ Error al cargar datos: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    cargar_rotacion_ventas_reales()
