# check_fact_tables.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def check_fact_tables():
    """Verifica el contenido de las tablas de hechos"""
    try:
        engine = create_engine(get_mysql_url("gestion_compras"))
        
        # Comprobar las tablas de hechos
        fact_tables = ['fact_rotacion', 'fact_inventarios']
        
        for table in fact_tables:
            try:
                # Verificar si la tabla existe
                check_query = text(f"""
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = 'gestion_compras' 
                    AND table_name = '{table}'
                """)
                
                with engine.connect() as conn:
                    exists = conn.execute(check_query).fetchone() is not None
                
                if not exists:
                    print(f"❌ La tabla {table} no existe en la base de datos")
                    continue
                    
                # Contar registros
                count_query = text(f"SELECT COUNT(*) FROM {table}")
                
                with engine.connect() as conn:
                    count = conn.execute(count_query).scalar()
                
                print(f"📊 Tabla {table}: {count} registros")
                
                # Mostrar algunos registros si existen
                if count > 0:
                    sample_query = text(f"SELECT * FROM {table} LIMIT 5")
                    
                    with engine.connect() as conn:
                        sample_df = pd.read_sql(sample_query, conn)
                    
                    print("\nMuestra de registros:")
                    print(sample_df.head())
                    
                    # Mostrar información por PDV si es fact_rotacion
                    if table == 'fact_rotacion':
                        pdv_query = text(f"""
                            SELECT codigo_pdv, COUNT(*) as total_registros 
                            FROM {table} 
                            GROUP BY codigo_pdv
                        """)
                        
                        with engine.connect() as conn:
                            pdv_counts = pd.read_sql(pdv_query, conn)
                        
                        print("\nRegistros por PDV:")
                        print(pdv_counts)
                
            except Exception as e:
                print(f"Error al verificar tabla {table}: {e}")
        
        # Comprobar logs de ETL si existen
        try:
            log_query = text("""
                SELECT * FROM etl_log
                WHERE nombre_etl LIKE '%Rotacion%'
                ORDER BY fecha_ejecucion DESC
                LIMIT 5
            """)
            
            with engine.connect() as conn:
                result = conn.execute(log_query)
                rows = result.fetchall()
            
            if rows:
                print("\nÚltimos logs de ETL:")
                for row in rows:
                    print(f"- {row}")
        except:
            print("\nNo se encontró tabla de logs de ETL")
        
    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    check_fact_tables()
