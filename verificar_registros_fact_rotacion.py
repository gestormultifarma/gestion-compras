# verificar_registros_fact_rotacion.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_registros():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Contar registros
            query_count = "SELECT COUNT(*) FROM fact_rotacion"
            total = connection.execute(text(query_count)).scalar()
            
            print(f"Total de registros en fact_rotacion: {total}")
            
            # Ver algunos ejemplos
            if total > 0:
                query_ejemplos = """
                SELECT * FROM fact_rotacion LIMIT 10
                """
                result = connection.execute(text(query_ejemplos))
                rows = result.fetchall()
                
                print("\nEjemplos de registros:")
                print("=" * 100)
                print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
                print("-" * 100)
                
                for row in rows:
                    print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4] or 0:7.2f} | {row[5] or 0:7.2f} | {row[6] or 0:7.2f}")
                
                # Conteo por PDV
                query_pdv = """
                SELECT nombre_pdv, COUNT(*) as total
                FROM fact_rotacion
                GROUP BY nombre_pdv
                ORDER BY total DESC
                """
                
                result = connection.execute(text(query_pdv))
                pdv_counts = result.fetchall()
                
                print("\nRegistros por PDV:")
                print("=" * 40)
                print(f"{'NOMBRE_PDV':20} | {'TOTAL':10}")
                print("-" * 40)
                
                for row in pdv_counts:
                    print(f"{row[0][:20]:20} | {row[1]:10}")
            else:
                print("No hay registros en la tabla fact_rotacion.")
            
    except Exception as e:
        print(f"Error al verificar registros: {str(e)}")

if __name__ == "__main__":
    verificar_registros()
