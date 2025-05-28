# verificar_ajustes_fact_rotacion.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_ajustes_fact_rotacion():
    """Verifica el estado actual de la tabla fact_rotacion después de los ajustes"""
    try:
        print("🔍 Verificando estado actual de la tabla fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Verificar estructura actual
        query_estructura = text("DESCRIBE fact_rotacion")
        
        with engine.connect() as connection:
            estructura = pd.read_sql(query_estructura, connection)
            
        print("\n📋 Estructura actual de fact_rotacion:")
        print(estructura.to_string(index=False))
        
        # Verificar vistas creadas
        query_vistas = text("SHOW FULL TABLES IN gestion_compras WHERE TABLE_TYPE LIKE 'VIEW'")
        
        with engine.connect() as connection:
            vistas = pd.read_sql(query_vistas, connection)
            
        print("\n📋 Vistas disponibles:")
        print(vistas.to_string(index=False))
        
        # Obtener muestra de datos con las nuevas columnas
        print("\n📋 Muestra de datos con las nuevas columnas:")
        
        try:
            query_muestra = text("""
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
                muestra = pd.read_sql(query_muestra, connection)
                
            print(muestra.to_string(index=False))
            
        except Exception as e:
            print(f"❌ Error al obtener muestra: {str(e)}")
            
            # Intentar una consulta más básica
            query_basica = text("""
                SELECT * FROM fact_rotacion LIMIT 5
            """)
            
            with engine.connect() as connection:
                muestra_basica = pd.read_sql(query_basica, connection)
                
            print("Columnas disponibles:")
            print(list(muestra_basica.columns))
        
        # Comprobar qué columnas faltan por agregar
        columnas_esperadas = [
            'abc_clasificacion', 'semanas_inventario', 'estado_inventario', 
            'inventario_critico', 'alta_rentabilidad'
        ]
        
        columnas_actuales = estructura['Field'].tolist()
        
        columnas_faltantes = [col for col in columnas_esperadas if col not in columnas_actuales]
        if columnas_faltantes:
            print("\n⚠️ Columnas que faltan por agregar:")
            for col in columnas_faltantes:
                print(f"  - {col}")
        else:
            print("\n✅ Todas las columnas necesarias fueron agregadas")
            
        # Verificar índices
        query_indices = text("""
            SHOW INDEX FROM fact_rotacion
        """)
        
        with engine.connect() as connection:
            indices = pd.read_sql(query_indices, connection)
            
        print("\n📋 Índices actuales:")
        print(indices[['Key_name', 'Column_name']].to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error general: {str(e)}")

if __name__ == "__main__":
    verificar_ajustes_fact_rotacion()
