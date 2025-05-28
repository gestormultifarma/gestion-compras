# ejecutar_ajustes_fact_rotacion.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def ejecutar_ajustes_fact_rotacion():
    """Ejecuta los ajustes a la tabla fact_rotacion para mejorar el análisis"""
    try:
        print("🔄 Iniciando ajustes a la tabla fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Leer el archivo SQL con los ajustes
        ruta_sql = os.path.join("E:/desarrollo/gestionCompras", "ajustar_fact_rotacion.sql")
        
        with open(ruta_sql, 'r', encoding='utf-8') as f:
            contenido_sql = f.read()
            
        # Separar las sentencias SQL
        sentencias_sql = [s.strip() for s in contenido_sql.split(';') if s.strip()]
        
        # Ejecutar cada sentencia SQL
        for i, sentencia in enumerate(sentencias_sql):
            try:
                # Ignorar comentarios
                if sentencia.strip().startswith('--'):
                    continue
                    
                print(f"Ejecutando sentencia {i+1}/{len(sentencias_sql)}...")
                with engine.connect() as connection:
                    connection.execute(text(sentencia))
                    connection.commit()
                print(f"✅ Sentencia {i+1} ejecutada correctamente")
                
            except Exception as e:
                print(f"❌ Error en sentencia {i+1}: {str(e)}")
                print(f"Sentencia: {sentencia[:100]}...")
        
        # Verificar que las columnas se hayan añadido correctamente
        print("\n📋 Verificando estructura actualizada de fact_rotacion:")
        query_estructura = text("""
            DESCRIBE fact_rotacion
        """)
        
        with engine.connect() as connection:
            estructura = pd.read_sql(query_estructura, connection)
            
        print(estructura.to_string(index=False))
        
        # Verificar que las vistas se hayan creado correctamente
        print("\n📋 Verificando vistas creadas:")
        query_vistas = text("""
            SHOW FULL TABLES IN gestion_compras WHERE TABLE_TYPE LIKE 'VIEW'
        """)
        
        with engine.connect() as connection:
            vistas = pd.read_sql(query_vistas, connection)
            
        print(vistas.to_string(index=False))
        
        # Mostrar ejemplo de datos con las nuevas columnas
        print("\n📋 Ejemplo de datos con las nuevas columnas:")
        query_ejemplo = text("""
            SELECT 
                codigo_producto,
                venta_unidades,
                dias_inventario,
                semanas_inventario,
                rotacion_mes,
                abc_clasificacion,
                estado_inventario,
                inventario_critico,
                margen_porcentaje,
                alta_rentabilidad
            FROM fact_rotacion
            LIMIT 5
        """)
        
        with engine.connect() as connection:
            ejemplo = pd.read_sql(query_ejemplo, connection)
            
        print(ejemplo.to_string(index=False))
        
        # Mostrar ejemplo de productos estrella
        print("\n📋 Ejemplo de productos estrella (vista):")
        query_estrella = text("""
            SELECT * FROM vw_productos_estrella LIMIT 5
        """)
        
        with engine.connect() as connection:
            estrella = pd.read_sql(query_estrella, connection)
            
        print(estrella.to_string(index=False))
        
        print("\n✅ Ajustes a fact_rotacion completados correctamente")
        
    except Exception as e:
        print(f"❌ Error general: {str(e)}")

if __name__ == "__main__":
    ejecutar_ajustes_fact_rotacion()
