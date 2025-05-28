# examinar_datos_rotacion.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def examinar_datos_rotacion():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Seleccionar una tabla de staging para examinar
            tabla = "stg_rotacion_de_bella_suiza_40350_1"
            
            # Verificar estructura completa
            print(f"Estructura de la tabla {tabla}:")
            query = f"DESCRIBE {tabla}"
            result = connection.execute(text(query))
            rows = result.fetchall()
            
            print("=" * 70)
            print(f"{'COLUMNA':25} | {'TIPO':25} | {'NULO':5} | {'CLAVE':5}")
            print("-" * 70)
            for row in rows:
                print(f"{row[0][:25]:25} | {row[1][:25]:25} | {row[2][:5]:5} | {row[3][:5]:5}")
            
            # Obtener ejemplos con ventas reales > 0
            print(f"\nEjemplos de registros con ventas_unidad > 0 en {tabla}:")
            query = f"""
            SELECT 
                codigo, 
                nombre, 
                venta_unidad 
            FROM {tabla} 
            WHERE venta_unidad > 0 
            LIMIT 10
            """
            result = connection.execute(text(query))
            rows = result.fetchall()
            
            if rows:
                print("=" * 80)
                print(f"{'CODIGO':15} | {'NOMBRE':40} | {'VENTA_UNIDAD':10}")
                print("-" * 80)
                for row in rows:
                    print(f"{row[0][:15]:15} | {row[1][:40]:40} | {row[2]:10}")
            else:
                print("No se encontraron registros con ventas > 0")
            
            # Verificar si hay datos válidos en alguna tabla de rotación
            print("\nVerificando tablas de rotación con datos válidos:")
            tablas_rotacion = connection.execute(text("SHOW TABLES LIKE 'stg_rotacion%'")).fetchall()
            
            for (tabla,) in tablas_rotacion[:5]:  # Limitamos a 5 tablas para no hacer demasiado largo el análisis
                query = f"""
                SELECT COUNT(*) FROM {tabla} WHERE venta_unidad > 0
                """
                count = connection.execute(text(query)).scalar()
                print(f"- {tabla}: {count} registros con ventas > 0")
            
            # Verificar si los códigos de productos en las tablas de staging coinciden con fact_rotacion
            print("\nVerificando coincidencia de códigos de productos:")
            query = f"""
            SELECT COUNT(*) 
            FROM (
                SELECT codigo FROM {tabla}
                INTERSECT
                SELECT codigo_producto FROM fact_rotacion
            ) as coincidencias
            """
            count = connection.execute(text(query)).scalar()
            print(f"Coincidencias de códigos entre {tabla} y fact_rotacion: {count}")
            
    except Exception as e:
        import traceback
        print(f"Error al examinar datos: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    examinar_datos_rotacion()
