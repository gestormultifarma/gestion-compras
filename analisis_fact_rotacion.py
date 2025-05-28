# analisis_fact_rotacion.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def analizar_fact_rotacion():
    """Analiza la estructura y contenido de fact_rotacion para identificar mejoras"""
    try:
        print("🔍 Analizando tabla fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # 1. Analizar estructura
        query_cols = text("""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'gestion_compras'
            AND TABLE_NAME = 'fact_rotacion'
            ORDER BY ORDINAL_POSITION
        """)
        
        with engine.connect() as connection:
            columnas = pd.read_sql(query_cols, connection)
            
        print("\n📋 Estructura actual de fact_rotacion:")
        print(columnas.to_string(index=False))
        
        # 2. Obtener estadísticas de las columnas numéricas
        query_stats = text("""
            SELECT 
                MIN(venta_unidades) as min_venta_unidades,
                MAX(venta_unidades) as max_venta_unidades,
                AVG(venta_unidades) as avg_venta_unidades,
                
                MIN(venta_total) as min_venta_total,
                MAX(venta_total) as max_venta_total,
                AVG(venta_total) as avg_venta_total,
                
                MIN(costo_total) as min_costo_total,
                MAX(costo_total) as max_costo_total,
                AVG(costo_total) as avg_costo_total,
                
                MIN(margen_porcentaje) as min_margen,
                MAX(margen_porcentaje) as max_margen,
                AVG(margen_porcentaje) as avg_margen,
                
                MIN(rotacion_mes) as min_rotacion,
                MAX(rotacion_mes) as max_rotacion,
                AVG(rotacion_mes) as avg_rotacion,
                
                MIN(dias_inventario) as min_dias_inv,
                MAX(dias_inventario) as max_dias_inv,
                AVG(dias_inventario) as avg_dias_inv
            FROM fact_rotacion
        """)
        
        with engine.connect() as connection:
            stats = pd.read_sql(query_stats, connection)
            
        print("\n📊 Estadísticas de las columnas numéricas:")
        print(stats.transpose().to_string())
        
        # 3. Verificar valores nulos o vacíos
        query_nulls = text("""
            SELECT 
                SUM(CASE WHEN producto_sk IS NULL THEN 1 ELSE 0 END) as null_producto_sk,
                SUM(CASE WHEN pdv_sk IS NULL THEN 1 ELSE 0 END) as null_pdv_sk,
                SUM(CASE WHEN fecha_sk IS NULL THEN 1 ELSE 0 END) as null_fecha_sk,
                SUM(CASE WHEN codigo_producto IS NULL THEN 1 ELSE 0 END) as null_codigo_producto,
                SUM(CASE WHEN codigo_pdv IS NULL THEN 1 ELSE 0 END) as null_codigo_pdv,
                SUM(CASE WHEN fecha IS NULL THEN 1 ELSE 0 END) as null_fecha,
                SUM(CASE WHEN venta_unidades IS NULL THEN 1 ELSE 0 END) as null_venta_unidades,
                SUM(CASE WHEN venta_total IS NULL THEN 1 ELSE 0 END) as null_venta_total,
                SUM(CASE WHEN costo_total IS NULL THEN 1 ELSE 0 END) as null_costo_total
            FROM fact_rotacion
        """)
        
        with engine.connect() as connection:
            nulls = pd.read_sql(query_nulls, connection)
            
        print("\n📊 Valores nulos por columna:")
        print(nulls.transpose().to_string())
        
        # 4. Analizar distribución por PDV
        query_pdv = text("""
            SELECT 
                f.codigo_pdv, 
                d.nombre_pdv,
                COUNT(*) as total_registros,
                SUM(f.venta_total) as venta_total,
                AVG(f.margen_porcentaje) as margen_promedio,
                AVG(f.rotacion_mes) as rotacion_promedio
            FROM fact_rotacion f
            JOIN dim_pdv d ON f.pdv_sk = d.pdv_sk
            GROUP BY f.codigo_pdv, d.nombre_pdv
            ORDER BY total_registros DESC
        """)
        
        with engine.connect() as connection:
            pdv_stats = pd.read_sql(query_pdv, connection)
            
        print("\n📊 Estadísticas por PDV:")
        print(pdv_stats.to_string(index=False))
        
        # 5. Verificar claves de dimensión y consistencia
        query_dim_check = text("""
            SELECT 
                'productos sin correspondencia' as check_name,
                COUNT(DISTINCT f.codigo_producto) as total_codes,
                SUM(CASE WHEN p.producto_sk IS NULL THEN 1 ELSE 0 END) as missing_codes
            FROM fact_rotacion f
            LEFT JOIN dim_producto p ON f.producto_sk = p.producto_sk
            
            UNION ALL
            
            SELECT 
                'pdvs sin correspondencia' as check_name,
                COUNT(DISTINCT f.codigo_pdv) as total_codes,
                SUM(CASE WHEN pd.pdv_sk IS NULL THEN 1 ELSE 0 END) as missing_codes
            FROM fact_rotacion f
            LEFT JOIN dim_pdv pd ON f.pdv_sk = pd.pdv_sk
        """)
        
        with engine.connect() as connection:
            dim_check = pd.read_sql(query_dim_check, connection)
            
        print("\n📊 Verificación de integridad con dimensiones:")
        print(dim_check.to_string(index=False))
        
        # 6. Proponer columnas calculadas adicionales
        print("\n💡 Propuestas de columnas calculadas adicionales para fact_rotacion:")
        print("  1. venta_diaria: Promedio de venta diaria para análisis de tendencias")
        print("  2. abc_clasificacion: Clasificación ABC de productos (A: alta rotación, B: media, C: baja)")
        print("  3. semaforo_inventario: Clasificación de inventario (Verde: óptimo, Amarillo: alerta, Rojo: crítico)")
        print("  4. semanas_inventario: Días de inventario en semanas para facilitar análisis")
        print("  5. trimestre: Trimestre del año para análisis estacional")
        print("  6. mes: Mes del año para análisis estacional")
        
    except Exception as e:
        print(f"❌ Error al analizar fact_rotacion: {str(e)}")

if __name__ == "__main__":
    analizar_fact_rotacion()
