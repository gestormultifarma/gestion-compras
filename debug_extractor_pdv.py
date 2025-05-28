# debug_extractor_pdv.py
import pandas as pd
from sqlalchemy import create_engine, text
import traceback
from utils.db_connection import get_mysql_url

def depurar_extractor_pdv():
    """Depura el proceso de extracción para todos los PDVs"""
    try:
        print("🔍 Iniciando depuración del extractor para todos los PDVs")
        
        # Establecer conexión a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # 1. Verificar tablas de rotación
        print("\n📋 Verificando tablas de rotación:")
        
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name LIKE 'stg_rotacion_%'
        """)
        
        with engine.connect() as connection:
            resultado = connection.execute(query)
            tablas = [row[0] for row in resultado]
        
        print(f"  • Encontradas {len(tablas)} tablas de rotación")
        
        # 2. Extraer PDVs de las tablas
        tablas_por_pdv = {}
        for tabla in tablas:
            # Formato esperado: stg_rotacion_de_NOMBRE_PDV_CODIGO_NUMERO
            partes = tabla.split('_')
            if len(partes) >= 3:
                codigo_pdv = partes[-2]
                if codigo_pdv not in tablas_por_pdv:
                    tablas_por_pdv[codigo_pdv] = []
                tablas_por_pdv[codigo_pdv].append(tabla)
        
        print(f"  • PDVs identificados: {len(tablas_por_pdv)}")
        
        # 3. Verificar dim_pdv
        print("\n📋 Verificando dim_pdv:")
        
        query_pdv = text("""
            SELECT pdv_sk, codigo_pdv, nombre_pdv
            FROM dim_pdv
            ORDER BY pdv_sk
        """)
        
        with engine.connect() as connection:
            pdvs = pd.read_sql(query_pdv, connection)
        
        print(f"  • Total PDVs en dimensión: {len(pdvs)}")
        
        # 4. Verificar correspondencia
        print("\n📋 Verificando correspondencia de PDVs:")
        
        pdvs_dim = {}
        for _, row in pdvs.iterrows():
            pdvs_dim[str(row['codigo_pdv'])] = {
                'pdv_sk': row['pdv_sk'],
                'nombre_pdv': row['nombre_pdv']
            }
        
        print("  • Códigos PDV en dim_pdv:")
        for codigo in pdvs_dim:
            print(f"    - {codigo}: {pdvs_dim[codigo]['nombre_pdv']} (SK: {pdvs_dim[codigo]['pdv_sk']})")
        
        print("\n  • Códigos PDV en tablas de rotación:")
        for codigo in tablas_por_pdv:
            if codigo in pdvs_dim:
                estado = "✓ (Existe en dim_pdv)"
            else:
                estado = "✗ (No existe en dim_pdv)"
            print(f"    - {codigo}: {len(tablas_por_pdv[codigo])} tablas {estado}")
        
        # 5. Probar extracción por PDV
        print("\n📋 Probando extracción para cada PDV:")
        
        for codigo_pdv, tablas in tablas_por_pdv.items():
            print(f"\n🔍 PDV: {codigo_pdv}")
            
            # Comprobar si el PDV existe en la dimensión
            if codigo_pdv not in pdvs_dim:
                print(f"  ❌ Error: PDV {codigo_pdv} no existe en dim_pdv")
                continue
            
            pdv_info = {
                'codigo_pdv': codigo_pdv,
                'nombre_pdv': pdvs_dim[codigo_pdv]['nombre_pdv'],
                'pdv_sk': pdvs_dim[codigo_pdv]['pdv_sk']
            }
            
            print(f"  • Nombre: {pdv_info['nombre_pdv']}")
            print(f"  • SK: {pdv_info['pdv_sk']}")
            print(f"  • Tablas: {len(tablas)}")
            
            # Probar procesamiento de una tabla
            if tablas:
                tabla_prueba = tablas[0]
                print(f"  • Probando extracción de: {tabla_prueba}")
                
                try:
                    # Primero averiguar las columnas de la tabla
                    query_columnas = text(f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = 'gestion_compras'
                        AND TABLE_NAME = '{tabla_prueba}'
                    """)
                    
                    with engine.connect() as connection:
                        columnas = [row[0] for row in connection.execute(query_columnas)]
                    
                    print(f"    - Columnas: {len(columnas)}")
                    
                    # Contar registros
                    query_count = text(f"SELECT COUNT(*) FROM `{tabla_prueba}`")
                    with engine.connect() as connection:
                        count = connection.execute(query_count).scalar()
                    
                    print(f"    - Registros: {count}")
                    
                    # Probar consulta
                    query_test = text(f"""
                        SELECT 
                            * 
                        FROM `{tabla_prueba}`
                        LIMIT 5
                    """)
                    
                    with engine.connect() as connection:
                        df_test = pd.read_sql(query_test, connection)
                    
                    print(f"    - Consulta de prueba: {'✓ (OK)' if not df_test.empty else '❌ (Sin datos)'}")
                    
                    # Comprobar si existe en fact_rotacion
                    query_fact = text(f"""
                        SELECT COUNT(*) 
                        FROM fact_rotacion
                        WHERE codigo_pdv = '{codigo_pdv}'
                    """)
                    
                    with engine.connect() as connection:
                        count_fact = connection.execute(query_fact).scalar()
                    
                    print(f"    - Registros en fact_rotacion: {count_fact}")
                    
                except Exception as e:
                    print(f"    ❌ Error: {str(e)}")
                    traceback.print_exc()
        
        # 6. Comprobar datos en fact_rotacion
        print("\n📋 Comprobando datos en fact_rotacion:")
        
        query_rotacion = text("""
            SELECT codigo_pdv, COUNT(*) as total
            FROM fact_rotacion
            GROUP BY codigo_pdv
        """)
        
        with engine.connect() as connection:
            rotacion_counts = pd.read_sql(query_rotacion, connection)
        
        if rotacion_counts.empty:
            print("  ❌ No hay datos en fact_rotacion")
        else:
            print("  • Registros por PDV:")
            for _, row in rotacion_counts.iterrows():
                pdv_code = str(row['codigo_pdv'])
                pdv_name = pdvs_dim.get(pdv_code, {}).get('nombre_pdv', 'Desconocido')
                print(f"    - {pdv_code} ({pdv_name}): {row['total']} registros")
        
    except Exception as e:
        print(f"❌ Error general: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    depurar_extractor_pdv()
