# check_dim_producto.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_dim_producto():
    """Verifica la estructura de la tabla dim_producto"""
    try:
        print("🔍 Verificando estructura de dim_producto")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Comprobar si la tabla existe
        query_exists = text("""
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name = 'dim_producto'
        """)
        
        with engine.connect() as connection:
            tabla_existe = connection.execute(query_exists).fetchone() is not None
            
        if not tabla_existe:
            print("❌ La tabla dim_producto no existe")
            
            # Buscar si hay otra tabla de productos
            query_similar = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE '%producto%'
            """)
            
            with engine.connect() as connection:
                tablas_similares = [row[0] for row in connection.execute(query_similar)]
                
            if tablas_similares:
                print(f"🔎 Tablas similares encontradas: {', '.join(tablas_similares)}")
                
                # Verificar estructura de la primera tabla similar
                primera_tabla = tablas_similares[0]
                print(f"\n📋 Estructura de {primera_tabla}:")
                
                query_cols = text(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'gestion_compras'
                    AND TABLE_NAME = '{primera_tabla}'
                """)
                
                with engine.connect() as connection:
                    columnas = pd.read_sql(query_cols, connection)
                    print(columnas.to_string(index=False))
                    
                    # Buscar columna de clave primaria
                    posibles_pk = ['producto_sk', 'producto_id', 'id_producto', 'id', 'sk']
                    pk_encontrada = False
                    
                    for pk in posibles_pk:
                        if pk in columnas['COLUMN_NAME'].values:
                            print(f"✅ Posible clave primaria encontrada: {pk}")
                            pk_encontrada = True
                            break
                            
                    if not pk_encontrada:
                        print("⚠️ No se encontró una columna que parezca ser clave primaria")
                        
                    # Buscar columna de código de producto
                    posibles_codigo = ['codigo_producto', 'codigo', 'id_producto_externo', 'sku']
                    codigo_encontrado = False
                    
                    for codigo in posibles_codigo:
                        if codigo in columnas['COLUMN_NAME'].values:
                            print(f"✅ Posible columna de código de producto encontrada: {codigo}")
                            codigo_encontrado = True
                            break
                            
                    if not codigo_encontrado:
                        print("⚠️ No se encontró una columna que parezca ser código de producto")
            else:
                print("❌ No se encontraron tablas similares a 'producto'")
            
            return
        
        # Verificar estructura
        query_cols = text("""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'gestion_compras'
            AND TABLE_NAME = 'dim_producto'
        """)
        
        with engine.connect() as connection:
            columnas = pd.read_sql(query_cols, connection)
            
        print("\n📋 Estructura de dim_producto:")
        print(columnas.to_string(index=False))
        
        # Verificar si tiene las columnas necesarias
        if 'producto_sk' in columnas['COLUMN_NAME'].values:
            print("✅ La tabla tiene la columna 'producto_sk'")
        else:
            print("❌ La tabla NO tiene la columna 'producto_sk'")
            
        if 'codigo_producto' in columnas['COLUMN_NAME'].values:
            print("✅ La tabla tiene la columna 'codigo_producto'")
        else:
            print("❌ La tabla NO tiene la columna 'codigo_producto'")
            
            # Buscar columnas similares
            posibles_codigos = ['codigo', 'id_producto', 'sku', 'id_externo']
            for codigo in posibles_codigos:
                if codigo in columnas['COLUMN_NAME'].values:
                    print(f"🔍 Se encontró una columna similar: '{codigo}'")
            
        # Verificar contenido
        query_count = text("SELECT COUNT(*) FROM dim_producto")
        with engine.connect() as connection:
            count = connection.execute(query_count).scalar()
            
        print(f"\n📊 La tabla dim_producto tiene {count} registros")
        
        if count > 0:
            # Mostrar algunos registros
            query_sample = text("""
                SELECT * 
                FROM dim_producto
                LIMIT 5
            """)
            
            with engine.connect() as connection:
                muestra = pd.read_sql(query_sample, connection)
                
            print("\n📋 Muestra de registros:")
            print(muestra.to_string())
            
    except Exception as e:
        print(f"❌ Error al verificar dim_producto: {str(e)}")

if __name__ == "__main__":
    verificar_dim_producto()
