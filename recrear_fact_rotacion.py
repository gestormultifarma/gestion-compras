# recrear_fact_rotacion.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def recrear_fact_rotacion():
    """
    Recrea la tabla fact_rotacion únicamente con las columnas solicitadas:
    - codigo_pdv
    - nombre_pdv
    - codigo_producto
    - nombre_producto
    - rotacion_m1
    - rotacion_m2
    - rotacion_m3
    """
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Guardar los datos actuales en una tabla temporal
            print("Creando tabla temporal con los datos actuales...")
            crear_temp = """
            CREATE TEMPORARY TABLE temp_fact_rotacion AS
            SELECT 
                codigo_pdv,
                nombre_pdv,
                codigo_producto,
                nombre_producto,
                rotacion_m1,
                rotacion_m2,
                rotacion_m3
            FROM fact_rotacion
            """
            connection.execute(text(crear_temp))
            
            # Verificar que los datos se copiaron
            verificar = "SELECT COUNT(*) FROM temp_fact_rotacion"
            count = connection.execute(text(verificar)).scalar()
            print(f"Se respaldaron {count} registros en la tabla temporal")
            
            # Eliminar la tabla original
            print("Eliminando tabla fact_rotacion...")
            connection.execute(text("DROP TABLE IF EXISTS fact_rotacion"))
            connection.commit()
            
            # Crear la nueva tabla con solo las columnas solicitadas
            print("Creando nueva tabla fact_rotacion con la estructura requerida...")
            crear_nueva = """
            CREATE TABLE fact_rotacion (
                codigo_pdv VARCHAR(20) NOT NULL,
                nombre_pdv VARCHAR(100),
                codigo_producto VARCHAR(50) NOT NULL,
                nombre_producto VARCHAR(255),
                rotacion_m1 DECIMAL(7,2),
                rotacion_m2 DECIMAL(7,2),
                rotacion_m3 DECIMAL(7,2),
                PRIMARY KEY (codigo_pdv, codigo_producto)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            connection.execute(text(crear_nueva))
            connection.commit()
            
            # Restaurar los datos desde la tabla temporal
            print("Restaurando datos a la nueva estructura...")
            restaurar = """
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
                codigo_pdv,
                nombre_pdv,
                codigo_producto,
                nombre_producto,
                rotacion_m1,
                rotacion_m2,
                rotacion_m3
            FROM temp_fact_rotacion
            """
            connection.execute(text(restaurar))
            connection.commit()
            
            # Verificar los datos restaurados
            verificar_final = "SELECT COUNT(*) FROM fact_rotacion"
            count_final = connection.execute(text(verificar_final)).scalar()
            print(f"Se restauraron {count_final} registros en la nueva tabla fact_rotacion")
            
            # Mostrar algunos ejemplos
            ejemplos = """
            SELECT * FROM fact_rotacion LIMIT 5
            """
            result = connection.execute(text(ejemplos))
            rows = result.fetchall()
            
            print("\nEjemplos de registros en la nueva tabla:")
            print("=" * 100)
            print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
            print("-" * 100)
            
            for row in rows:
                print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4]:7.2f} | {row[5]:7.2f} | {row[6]:7.2f}")
            
            # Verificar estructura final
            print("\nEstructura final de la tabla fact_rotacion:")
            result = connection.execute(text("DESCRIBE fact_rotacion"))
            cols = result.fetchall()
            
            for col in cols:
                print(f"- {col[0]} ({col[1]})")
            
            print("\n✅ Tabla fact_rotacion recreada exitosamente con la estructura solicitada")
            
    except Exception as e:
        print(f"❌ Error al recrear la tabla: {str(e)}")

if __name__ == "__main__":
    recrear_fact_rotacion()
