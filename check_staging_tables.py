# check_staging_tables.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def check_database_tables():
    """Verifica las tablas existentes en la base de datos"""
    try:
        engine = create_engine(get_mysql_url("gestion_compras"))
        
        # Consultar tablas staging
        with engine.connect() as conn:
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND (table_name LIKE 'stg_%' OR table_name LIKE 'fact_%' OR table_name LIKE 'dim_%')
                ORDER BY table_name
            """)
            
            result = conn.execute(query)
            tables = [row[0] for row in result]
            
            print(f"Total de tablas encontradas: {len(tables)}")
            print("\nTablas staging (stg_):")
            stg_tables = [t for t in tables if t.startswith('stg_')]
            for t in stg_tables:
                print(f"- {t}")
                
            print("\nTablas de dimensiones (dim_):")
            dim_tables = [t for t in tables if t.startswith('dim_')]
            for t in dim_tables:
                print(f"- {t}")
                
            print("\nTablas de hechos (fact_):")
            fact_tables = [t for t in tables if t.startswith('fact_')]
            for t in fact_tables:
                print(f"- {t}")
                
        # Verificar contenido de tablas fact_
        print("\nVerificando contenido de tablas de hechos:")
        for table in fact_tables:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(*) FROM {table}")
                count = conn.execute(query).scalar()
                print(f"- {table}: {count} registros")
        
        # Verificar contenido de algunas tablas staging
        print("\nVerificando contenido de tablas staging (muestra):")
        sample_tables = stg_tables[:5] if len(stg_tables) > 5 else stg_tables
        for table in sample_tables:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(*) FROM {table}")
                count = conn.execute(query).scalar()
                print(f"- {table}: {count} registros")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_database_tables()
