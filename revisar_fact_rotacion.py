# revisar_fact_rotacion.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def revisar_fact_rotacion():
    """Revisa la estructura de fact_rotacion y muestra ejemplos para análisis"""
    try:
        print("🔍 Revisando tabla fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Revisar estructura
        query_estructura = text("""
            DESCRIBE fact_rotacion
        """)
        
        with engine.connect() as connection:
            estructura = pd.read_sql(query_estructura, connection)
            
        print("\n📋 Estructura actual de fact_rotacion:")
        print(estructura.to_string(index=False))
        
        # Obtener muestra de datos
        query_muestra = text("""
            SELECT * 
            FROM fact_rotacion
            LIMIT 5
        """)
        
        with engine.connect() as connection:
            muestra = pd.read_sql(query_muestra, connection)
            
        print("\n📋 Ejemplo de datos en fact_rotacion:")
        print(muestra.to_string())
        
        # Resumen por PDV
        query_pdv = text("""
            SELECT 
                codigo_pdv,
                COUNT(*) as total_registros,
                SUM(venta_unidades) as total_unidades_vendidas,
                SUM(venta_total) as total_venta,
                AVG(margen_porcentaje) as margen_promedio,
                AVG(rotacion_mes) as rotacion_promedio,
                AVG(dias_inventario) as dias_inventario_promedio
            FROM fact_rotacion
            GROUP BY codigo_pdv
            ORDER BY total_venta DESC
        """)
        
        with engine.connect() as connection:
            resumen_pdv = pd.read_sql(query_pdv, connection)
            
        print("\n📊 Resumen por PDV:")
        print(resumen_pdv.to_string(index=False))
        
        # Propuesta de modificaciones
        print("\n🔄 Propuesta de modificaciones para mejorar el análisis:")
        print("1. Agregar columnas de clasificación y segmentación:")
        
        # Generar propuesta SQL para modificaciones
        propuesta_sql = """
        -- 1. Agregar columna de clasificación ABC basada en rotación
        ALTER TABLE fact_rotacion ADD COLUMN abc_clasificacion VARCHAR(1) AFTER rotacion_mes;
        
        -- Actualizar clasificación ABC (A: alta rotación, B: media, C: baja)
        UPDATE fact_rotacion 
        SET abc_clasificacion = 
            CASE 
                WHEN rotacion_mes >= 2 THEN 'A'  -- Alta rotación (2+ veces al mes)
                WHEN rotacion_mes >= 1 THEN 'B'  -- Media rotación (1-2 veces al mes)
                ELSE 'C'                         -- Baja rotación (menos de 1 vez al mes)
            END;
            
        -- 2. Agregar columna de estado de inventario
        ALTER TABLE fact_rotacion ADD COLUMN estado_inventario VARCHAR(10) AFTER dias_inventario;
        
        -- Actualizar estado de inventario
        UPDATE fact_rotacion 
        SET estado_inventario = 
            CASE 
                WHEN dias_inventario <= 15 THEN 'BAJO'        -- Menos de 15 días
                WHEN dias_inventario <= 45 THEN 'ÓPTIMO'      -- Entre 15 y 45 días
                WHEN dias_inventario <= 60 THEN 'ALTO'        -- Entre 45 y 60 días
                ELSE 'EXCESO'                                 -- Más de 60 días
            END;
            
        -- 3. Agregar columna de semanas de inventario para facilitar análisis
        ALTER TABLE fact_rotacion ADD COLUMN semanas_inventario DECIMAL(10,2) AFTER dias_inventario;
        
        -- Actualizar semanas de inventario
        UPDATE fact_rotacion SET semanas_inventario = dias_inventario / 7;
        
        -- 4. Agregar columna para indicar si el inventario está agotado o crítico
        ALTER TABLE fact_rotacion ADD COLUMN inventario_critico BOOLEAN AFTER estado_inventario;
        
        -- Actualizar estado crítico (menos de 7 días de inventario)
        UPDATE fact_rotacion SET inventario_critico = (dias_inventario < 7);
        
        -- 5. Agregar columna para la fecha de la última venta (sería calculada desde otras tablas)
        ALTER TABLE fact_rotacion ADD COLUMN ultima_venta DATE AFTER fecha;
        
        -- 6. Agregar columna para indicar si el producto es de alta rentabilidad
        ALTER TABLE fact_rotacion ADD COLUMN alta_rentabilidad BOOLEAN AFTER margen_porcentaje;
        
        -- Actualizar alta rentabilidad (más del 30% de margen)
        UPDATE fact_rotacion SET alta_rentabilidad = (margen_porcentaje > 30);
        
        -- 7. Agregar índices para mejorar consultas de análisis
        CREATE INDEX idx_fact_rotacion_abc ON fact_rotacion(abc_clasificacion);
        CREATE INDEX idx_fact_rotacion_estado_inv ON fact_rotacion(estado_inventario);
        CREATE INDEX idx_fact_rotacion_rentabilidad ON fact_rotacion(alta_rentabilidad);
        """
        
        print("\nPropuesta SQL para modificaciones:")
        print(propuesta_sql)
        
        # Propuesta de vistas para análisis
        propuesta_vistas = """
        -- Vista para análisis de rotación por PDV
        CREATE OR REPLACE VIEW vw_rotacion_por_pdv AS
        SELECT 
            p.codigo_pdv,
            p.nombre_pdv,
            COUNT(f.producto_sk) as total_productos,
            SUM(CASE WHEN f.abc_clasificacion = 'A' THEN 1 ELSE 0 END) as productos_a,
            SUM(CASE WHEN f.abc_clasificacion = 'B' THEN 1 ELSE 0 END) as productos_b,
            SUM(CASE WHEN f.abc_clasificacion = 'C' THEN 1 ELSE 0 END) as productos_c,
            AVG(f.rotacion_mes) as rotacion_promedio,
            SUM(f.venta_total) as venta_total,
            SUM(f.inventario_unidades_final * f.costo_unitario) as valor_inventario,
            SUM(CASE WHEN f.inventario_critico THEN 1 ELSE 0 END) as productos_criticos
        FROM fact_rotacion f
        JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
        GROUP BY p.codigo_pdv, p.nombre_pdv;
        
        -- Vista para análisis de productos de alta rotación y alta rentabilidad
        CREATE OR REPLACE VIEW vw_productos_estrella AS
        SELECT 
            pr.nombre as nombre_producto,
            p.nombre_pdv,
            f.venta_unidades,
            f.venta_total,
            f.margen_porcentaje,
            f.rotacion_mes,
            f.abc_clasificacion,
            f.estado_inventario
        FROM fact_rotacion f
        JOIN dim_producto pr ON f.producto_sk = pr.producto_sk
        JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
        WHERE f.abc_clasificacion = 'A' AND f.alta_rentabilidad = 1
        ORDER BY f.venta_total DESC;
        
        -- Vista para análisis de inventario en exceso
        CREATE OR REPLACE VIEW vw_inventario_exceso AS
        SELECT 
            pr.nombre as nombre_producto,
            p.nombre_pdv,
            f.inventario_unidades_final,
            f.venta_unidades,
            f.dias_inventario,
            f.estado_inventario,
            f.costo_unitario,
            (f.inventario_unidades_final * f.costo_unitario) as valor_inmovilizado
        FROM fact_rotacion f
        JOIN dim_producto pr ON f.producto_sk = pr.producto_sk
        JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
        WHERE f.estado_inventario = 'EXCESO'
        ORDER BY valor_inmovilizado DESC;
        
        -- Vista para análisis de productos con inventario crítico
        CREATE OR REPLACE VIEW vw_inventario_critico AS
        SELECT 
            pr.nombre as nombre_producto,
            p.nombre_pdv,
            f.inventario_unidades_final,
            f.venta_unidades,
            f.dias_inventario,
            f.estado_inventario,
            f.costo_unitario,
            f.abc_clasificacion
        FROM fact_rotacion f
        JOIN dim_producto pr ON f.producto_sk = pr.producto_sk
        JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
        WHERE f.inventario_critico = 1
        ORDER BY f.abc_clasificacion, f.dias_inventario;
        """
        
        print("\nPropuesta de vistas para análisis:")
        print(propuesta_vistas)
        
    except Exception as e:
        print(f"❌ Error al revisar fact_rotacion: {str(e)}")

if __name__ == "__main__":
    revisar_fact_rotacion()
