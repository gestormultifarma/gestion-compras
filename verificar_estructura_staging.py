# verificar_estructura_staging.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_estructura_staging():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Seleccionar una tabla de staging de rotación para examinar
            tabla_ejemplo = "stg_rotacion_de_bella_suiza_40350_1"
            
            # Verificar la estructura de la tabla
            query = f"DESCRIBE {tabla_ejemplo}"
            result = connection.execute(text(query))
            
            print(f"Estructura de la tabla {tabla_ejemplo}:")
            print("=" * 50)
            for row in result:
                print(f"- {row[0]} ({row[1]})")
            
            # Obtener una muestra de datos
            query = f"SELECT * FROM {tabla_ejemplo} LIMIT 5"
            result = connection.execute(text(query))
            rows = result.fetchall()
            
            print("\nMuestra de datos:")
            print("=" * 50)
            if rows:
                # Mostrar nombres de columnas
                column_names = result.keys()
                print(" | ".join(column_names))
                print("-" * 80)
                
                # Mostrar datos
                for row in rows:
                    print(" | ".join(str(value) for value in row))
            else:
                print("No hay datos en esta tabla.")
            
    except Exception as e:
        print(f"Error al verificar estructura: {str(e)}")

if __name__ == "__main__":
    verificar_estructura_staging()
