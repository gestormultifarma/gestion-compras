# check_fact_rotacion_schema.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_schema_fact_rotacion():
    """Verifica el esquema de la tabla fact_rotacion"""
    try:
        print("🔍 Verificando estructura de fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Obtener estructura de la tabla
        query_cols = text("""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'gestion_compras'
            AND TABLE_NAME = 'fact_rotacion'
            ORDER BY ORDINAL_POSITION
        """)
        
        with engine.connect() as connection:
            columnas = pd.read_sql(query_cols, connection)
            
        print("\n📋 Estructura de fact_rotacion:")
        print(columnas.to_string())
        
        # Mostrar muestra de datos
        query_sample = text("""
            SELECT * 
            FROM fact_rotacion
            LIMIT 1
        """)
        
        with engine.connect() as connection:
            muestra = pd.read_sql(query_sample, connection)
            
        if not muestra.empty:
            print("\n📋 Ejemplo de registro en fact_rotacion:")
            print(muestra.to_string())
            
            # Mostrar nombres de columnas
            print("\n📋 Nombres de columnas en fact_rotacion:")
            print(", ".join(muestra.columns))
        else:
            print("\n⚠️ No hay registros en fact_rotacion")
            
    except Exception as e:
        print(f"❌ Error al verificar estructura: {str(e)}")

if __name__ == "__main__":
    verificar_schema_fact_rotacion()
