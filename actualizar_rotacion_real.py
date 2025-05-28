# actualizar_rotacion_real.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def actualizar_rotacion_real():
    """
    Actualiza la tabla fact_rotacion con los datos reales de rotación histórica
    utilizando las tablas staging de rotación con sufijos _1, _2 y _3.
    """
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            print("Actualizando la tabla fact_rotacion con datos históricos reales...")
            
            # Primero, limpiar la tabla actual para asegurarnos de que podemos insertar nuevos datos
            print("Limpiando tabla fact_rotacion...")
            connection.execute(text("TRUNCATE TABLE fact_rotacion"))
            connection.commit()
            
            # Obtener la lista de PDVs
            query_pdvs = """
            SELECT DISTINCT 
                codigo_pdv, 
                nombre_pdv 
            FROM dim_pdv
            """
            pdvs = connection.execute(text(query_pdvs)).fetchall()
            
            total_registros = 0
            
            # Para cada PDV, procesar sus datos históricos
            for pdv_row in pdvs:
                codigo_pdv = pdv_row[0]
                nombre_pdv = pdv_row[1]
                
                # Obtener nombre normalizado del PDV para buscar tablas de staging
                pdv_nombre_normalizado = nombre_pdv.lower().replace(' ', '_')
                
                print(f"\nProcesando PDV: {nombre_pdv} (código: {codigo_pdv})...")
                
                # Verificar si existen las tablas de staging para este PDV
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
                    
                    # Verificar la estructura de la tabla
                    structure_query = f"DESCRIBE {tabla_staging}"
                    columnas = connection.execute(text(structure_query)).fetchall()
                    columnas_nombres = [col[0].lower() for col in columnas]
                    
                    # Verificar si tenemos las columnas necesarias
                    columnas_requeridas = ['codigo', 'descripcion', 'cantidad']
                    if not all(col in columnas_nombres for col in columnas_requeridas):
                        print(f"  - Tabla {tabla_staging} no tiene todas las columnas requeridas")
                        continue
                    
                    # Obtener los datos de rotación
                    data_query = f"""
                    SELECT 
                        codigo,
                        descripcion,
                        cantidad
                    FROM {tabla_staging}
                    """
                    rotacion_data = connection.execute(text(data_query)).fetchall()
                    
                    print(f"  - Procesando {len(rotacion_data)} registros del periodo {periodo}")
                    
                    # Para cada registro, actualizar o insertar en fact_rotacion
                    for data_row in rotacion_data:
                        codigo_producto = data_row[0]
                        nombre_producto = data_row[1]
                        cantidad = float(data_row[2]) if data_row[2] is not None else 0
                        
                        # Verificar si ya existe el registro
                        check_exists = f"""
                        SELECT COUNT(*) 
                        FROM fact_rotacion 
                        WHERE codigo_pdv = '{codigo_pdv}' 
                        AND codigo_producto = '{codigo_producto}'
                        """
                        registro_existe = connection.execute(text(check_exists)).scalar() > 0
                        
                        if registro_existe:
                            # Actualizar el registro existente
                            update_query = f"""
                            UPDATE fact_rotacion 
                            SET rotacion_m{periodo} = {cantidad}
                            WHERE codigo_pdv = '{codigo_pdv}' 
                            AND codigo_producto = '{codigo_producto}'
                            """
                            connection.execute(text(update_query))
                        else:
                            # Crear un nuevo registro
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
                                {cantidad if periodo == 1 else 0},
                                {cantidad if periodo == 2 else 0},
                                {cantidad if periodo == 3 else 0}
                            )
                            """
                            connection.execute(text(insert_query))
                    
                    # Confirmar cambios para este periodo
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
            print(f"\n✅ Actualización completada. Total de registros en fact_rotacion: {total_registros}")
            
            # Mostrar ejemplos
            ejemplos = """
            SELECT * FROM fact_rotacion LIMIT 10
            """
            result = connection.execute(text(ejemplos))
            rows = result.fetchall()
            
            print("\nEjemplos de registros actualizados:")
            print("=" * 100)
            print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
            print("-" * 100)
            
            for row in rows:
                print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4] or 0:7.2f} | {row[5] or 0:7.2f} | {row[6] or 0:7.2f}")
            
    except Exception as e:
        import traceback
        print(f"❌ Error al actualizar rotación: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    actualizar_rotacion_real()
