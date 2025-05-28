# modificar_fact_rotacion.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def modificar_tabla_fact_rotacion():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        # Verificar si las columnas ya existen
        with engine.connect() as connection:
            result = connection.execute(text("DESCRIBE fact_rotacion"))
            columnas_existentes = [row[0] for row in result.fetchall()]
            
            # Lista de columnas a agregar y sus tipos
            columnas_nuevas = {
                'nombre_pdv': 'VARCHAR(100)',
                'nombre_producto': 'VARCHAR(255)',
                'rotacion_m1': 'DECIMAL(7,2)',
                'rotacion_m2': 'DECIMAL(7,2)',
                'rotacion_m3': 'DECIMAL(7,2)'
            }
            
            # Filtrar solo las columnas que no existen
            columnas_a_agregar = {col: tipo for col, tipo in columnas_nuevas.items() 
                                 if col not in columnas_existentes}
            
            if not columnas_a_agregar:
                print("Todas las columnas ya existen en la tabla fact_rotacion.")
                return
            
            print(f"Se agregarán {len(columnas_a_agregar)} columnas a la tabla fact_rotacion:")
            for col, tipo in columnas_a_agregar.items():
                print(f"- {col} ({tipo})")
            
            # Agregar las columnas
            for col, tipo in columnas_a_agregar.items():
                sql = f"ALTER TABLE fact_rotacion ADD COLUMN {col} {tipo}"
                connection.execute(text(sql))
                print(f"Columna {col} agregada correctamente.")
            
            connection.commit()
            
            # Actualizar las columnas nombre_pdv y nombre_producto con datos de las dimensiones
            if 'nombre_pdv' in columnas_a_agregar:
                update_sql = """
                UPDATE fact_rotacion fr 
                JOIN dim_pdv pdv ON fr.pdv_sk = pdv.pdv_sk 
                SET fr.nombre_pdv = pdv.nombre_pdv
                """
                connection.execute(text(update_sql))
                print("Datos de nombre_pdv actualizados correctamente.")
                connection.commit()
            
            if 'nombre_producto' in columnas_a_agregar:
                update_sql = """
                UPDATE fact_rotacion fr 
                JOIN dim_producto p ON fr.producto_sk = p.producto_sk 
                SET fr.nombre_producto = p.nombre
                """
                connection.execute(text(update_sql))
                print("Datos de nombre_producto actualizados correctamente.")
                connection.commit()
            
            # Simulación de datos históricos para las columnas de rotación
            if any(col in columnas_a_agregar for col in ['rotacion_m1', 'rotacion_m2', 'rotacion_m3']):
                update_sql = """
                UPDATE fact_rotacion 
                SET 
                    rotacion_m1 = rotacion_mes * 0.95,
                    rotacion_m2 = rotacion_mes * 0.90,
                    rotacion_m3 = rotacion_mes * 0.85
                """
                connection.execute(text(update_sql))
                print("Datos históricos de rotación simulados correctamente.")
                connection.commit()
            
            print("\nModificación de la tabla fact_rotacion completada con éxito.")
            
    except Exception as e:
        print(f"Error al modificar la tabla: {str(e)}")

if __name__ == "__main__":
    modificar_tabla_fact_rotacion()
