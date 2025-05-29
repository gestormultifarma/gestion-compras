# verificar_fact_inventarios.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_estructura():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Verificar si la tabla existe
            result = connection.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name = 'fact_inventarios'
            """))
            tabla_existe = result.scalar() > 0
            
            if not tabla_existe:
                print("La tabla fact_inventarios no existe. Necesitamos crearla.")
                return False
            
            # Consultar la estructura actual de la tabla
            result = connection.execute(text("DESCRIBE fact_inventarios"))
            rows = result.fetchall()
            
            print("Estructura actual de la tabla fact_inventarios:")
            print("=" * 50)
            for row in rows:
                print(f"- {row[0]} ({row[1]})")
            
            # Verificar contenido
            result = connection.execute(text("SELECT COUNT(*) FROM fact_inventarios"))
            count = result.scalar()
            print(f"\nTotal de registros en fact_inventarios: {count}")
            
            return True
            
    except Exception as e:
        print(f"Error al verificar estructura: {str(e)}")
        return False

if __name__ == "__main__":
    verificar_estructura()
