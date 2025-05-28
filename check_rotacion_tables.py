# check_rotacion_tables.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def check_rotacion_tables():
    """Verifica las tablas de rotación y su estructura"""
    try:
        engine = create_engine(get_mysql_url("gestion_compras"))
        
        # 1. Obtener todas las tablas de rotación
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name LIKE 'stg_rotacion_%'
        """)
        
        with engine.connect() as connection:
            resultado = connection.execute(query)
            tablas = [row[0] for row in resultado]
        
        if not tablas:
            print("❌ No se encontraron tablas de rotación (stg_rotacion_*)")
            return
        
        print(f"📊 Encontradas {len(tablas)} tablas de rotación:")
        
        # Agrupar tablas por PDV
        tablas_por_pdv = {}
        for tabla in tablas:
            partes = tabla.split('_')
            if len(partes) >= 3:
                codigo_pdv = partes[-2]
                if codigo_pdv not in tablas_por_pdv:
                    tablas_por_pdv[codigo_pdv] = []
                tablas_por_pdv[codigo_pdv].append(tabla)
        
        print(f"\n🏪 Total PDVs encontrados: {len(tablas_por_pdv)}")
        for codigo, tablas_pdv in tablas_por_pdv.items():
            print(f"\n📍 PDV: {codigo} - {len(tablas_pdv)} tablas:")
            for t in tablas_pdv:
                print(f"  - {t}")
        
        # 2. Analizar estructura de las tablas (muestreo)
        print("\n🔍 Analizando estructura de tablas (muestra):")
        muestras = min(5, len(tablas))
        for i in range(muestras):
            tabla = tablas[i]
            print(f"\n📋 Tabla: {tabla}")
            
            # Obtener estructura
            query_cols = text(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'gestion_compras'
                AND TABLE_NAME = '{tabla}'
            """)
            
            with engine.connect() as connection:
                cols = connection.execute(query_cols)
                print("  Columnas:")
                for col in cols:
                    print(f"    - {col[0]} ({col[1]})")
            
            # Contar registros
            query_count = text(f"SELECT COUNT(*) FROM `{tabla}`")
            with engine.connect() as connection:
                count = connection.execute(query_count).scalar()
                print(f"  Registros: {count}")
            
            # Mostrar algunos valores (primera fila)
            query_sample = text(f"SELECT * FROM `{tabla}` LIMIT 1")
            with engine.connect() as connection:
                try:
                    sample = connection.execute(query_sample).fetchone()
                    if sample:
                        print("  Valores de muestra:")
                        for i, col in enumerate(cols.keys()):
                            if i < len(sample):
                                print(f"    - {col}: {sample[i]}")
                except:
                    print("  ⚠️ No se pudo obtener muestra de datos")
        
        # 3. Verificar dim_pdv
        print("\n🔍 Verificando dim_pdv:")
        query_pdv = text("""
            SELECT pdv_sk, codigo_pdv, nombre_pdv, estado_pdv
            FROM dim_pdv
            ORDER BY pdv_sk
        """)
        
        with engine.connect() as connection:
            pdvs = pd.read_sql(query_pdv, connection)
            
        print(f"  Total PDVs en dimensión: {len(pdvs)}")
        print("\n  Muestra de PDVs:")
        print(pdvs.head().to_string())
        
        # 4. Verificar coincidencias entre códigos PDV
        codigos_dim = set(pdvs['codigo_pdv'].astype(str))
        codigos_tablas = set(tablas_por_pdv.keys())
        
        coincidencias = codigos_dim.intersection(codigos_tablas)
        solo_dim = codigos_dim - codigos_tablas
        solo_tablas = codigos_tablas - codigos_dim
        
        print(f"\n🔄 Coincidencias entre dim_pdv y tablas de rotación:")
        print(f"  - Códigos en ambos: {len(coincidencias)}")
        print(f"  - Códigos solo en dim_pdv: {len(solo_dim)}")
        print(f"  - Códigos solo en tablas rotación: {len(solo_tablas)}")
        
        # Mostrar algunos ejemplos de códigos sin coincidencia
        if solo_tablas:
            print("\n⚠️ Ejemplos de códigos en tablas sin correspondencia en dim_pdv:")
            for codigo in list(solo_tablas)[:5]:
                print(f"  - {codigo}")
                
        # 5. Verificar fact_rotacion
        print("\n📊 Tabla fact_rotacion:")
        query_rotacion = text("""
            SELECT codigo_pdv, COUNT(*) as total
            FROM fact_rotacion
            GROUP BY codigo_pdv
        """)
        
        with engine.connect() as connection:
            rotacion_counts = pd.read_sql(query_rotacion, connection)
            
        print("  Registros por PDV:")
        print(rotacion_counts.to_string())
            
    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    check_rotacion_tables()
