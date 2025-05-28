# verificar_tablas_ventas.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_tablas_ventas():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Listar todas las tablas
            query = "SHOW TABLES"
            result = connection.execute(text(query))
            tablas = [row[0] for row in result]
            
            print(f"Total de tablas en la base de datos: {len(tablas)}")
            print("\nTablas disponibles:")
            for tabla in tablas:
                print(f"- {tabla}")
            
            # Verificar si hay tablas relacionadas con ventas históricas
            tablas_ventas = [t for t in tablas if 'venta' in t.lower() or 'rotacion' in t.lower()]
            
            print("\nTablas relacionadas con ventas o rotación:")
            for tabla in tablas_ventas:
                print(f"- {tabla}")
                
            # Verificar si hay datos históricos por mes
            tablas_meses = [t for t in tablas if any(mes in t.lower() for mes in ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'])]
            
            print("\nTablas con posibles datos mensuales:")
            for tabla in tablas_meses:
                print(f"- {tabla}")
                
    except Exception as e:
        print(f"Error al consultar tablas: {str(e)}")

if __name__ == "__main__":
    verificar_tablas_ventas()
