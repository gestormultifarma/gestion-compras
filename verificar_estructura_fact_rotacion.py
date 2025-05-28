# verificar_estructura_fact_rotacion.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_estructura():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    # Consultar la estructura actual de la tabla
    with engine.connect() as connection:
        result = connection.execute(text("DESCRIBE fact_rotacion"))
        rows = result.fetchall()
        
        print("Estructura actual de la tabla fact_rotacion:")
        print("=" * 50)
        for row in rows:
            print(f"- {row[0]} ({row[1]})")

if __name__ == "__main__":
    verificar_estructura()
