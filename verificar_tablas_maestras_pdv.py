"""
Script para verificar las tablas maestras por punto de venta en el staging
"""
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_tablas_maestras():
    """Lista todas las tablas de staging de maestras por PDV"""
    print("\n=== TABLAS MAESTRAS DE PDV EN STAGING ===")
    
    try:
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Buscar tablas con patrón stg_maestra_pdv_%
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gestion_compras' 
            AND table_name LIKE 'stg\\_maestra\\_pdv\\_%'
            """
            result = conn.execute(text(query))
            tablas = [row[0] for row in result]
            
            if not tablas:
                print("No se encontraron tablas maestras de PDV")
                
                # Busquemos con un patrón más amplio para ver qué hay
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND (table_name LIKE 'stg\\_maestra%' OR table_name LIKE '%maestra%')
                """
                result = conn.execute(text(query))
                tablas_alt = [row[0] for row in result]
                
                if tablas_alt:
                    print("\nSe encontraron estas tablas alternativas que podrían ser maestras:")
                    for tabla in tablas_alt:
                        print(f"- {tabla}")
                return
            
            print(f"Se encontraron {len(tablas)} tablas maestras de PDV:")
            
            for tabla in tablas:
                # Obtener estructura de la tabla
                cols_query = f"DESCRIBE `{tabla}`"
                cols_result = conn.execute(text(cols_query))
                columnas = [row for row in cols_result]
                
                # Contar registros
                count_query = f"SELECT COUNT(*) FROM `{tabla}`"
                count = conn.execute(text(count_query)).scalar()
                
                print(f"\n- {tabla}: {count} registros")
                print("  Columnas:")
                for col in columnas[:10]:  # Mostrar primeras 10 columnas
                    print(f"  - {col[0]} ({col[1]})")
                
                if len(columnas) > 10:
                    print(f"  ... y {len(columnas) - 10} columnas más")
                
                # Mostrar una muestra de datos
                sample_query = f"SELECT * FROM `{tabla}` LIMIT 1"
                sample = conn.execute(text(sample_query)).fetchone()
                
                if sample and sample[0] is not None:
                    print("  Muestra de datos:")
                    for i, col in enumerate(columnas[:5]):  # Mostrar solo primeros 5 campos
                        if i < len(sample) and sample[i] is not None:
                            print(f"  - {col[0]}: {sample[i]}")
    
    except Exception as e:
        print(f"Error al verificar tablas maestras: {str(e)}")

if __name__ == "__main__":
    verificar_tablas_maestras()
