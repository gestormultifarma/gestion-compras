# ajustar_fact_rotacion_paso_a_paso.py
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def ejecutar_ajustes_fact_rotacion():
    """Ejecuta los ajustes a la tabla fact_rotacion paso a paso"""
    try:
        print("🔄 Iniciando ajustes a la tabla fact_rotacion")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Lista de ajustes a realizar (cada uno en una transacción separada)
        ajustes = [
            # 1. Agregar columna de clasificación ABC
            """
            ALTER TABLE fact_rotacion 
            ADD COLUMN abc_clasificacion VARCHAR(1) AFTER rotacion_mes
            """,
            
            # 2. Actualizar clasificación ABC
            """
            UPDATE fact_rotacion 
            SET abc_clasificacion = 
                CASE 
                    WHEN rotacion_mes >= 2 THEN 'A'
                    WHEN rotacion_mes >= 1 THEN 'B'
                    ELSE 'C'
                END
            """,
            
            # 3. Agregar columna de semanas de inventario
            """
            ALTER TABLE fact_rotacion 
            ADD COLUMN semanas_inventario DECIMAL(10,2) AFTER dias_inventario
            """,
            
            # 4. Actualizar semanas de inventario
            """
            UPDATE fact_rotacion 
            SET semanas_inventario = dias_inventario / 7
            """,
            
            # 5. Agregar columna de estado de inventario
            """
            ALTER TABLE fact_rotacion 
            ADD COLUMN estado_inventario VARCHAR(10) AFTER semanas_inventario
            """,
            
            # 6. Actualizar estado de inventario
            """
            UPDATE fact_rotacion 
            SET estado_inventario = 
                CASE 
                    WHEN dias_inventario <= 15 THEN 'BAJO'
                    WHEN dias_inventario <= 45 THEN 'ÓPTIMO'
                    WHEN dias_inventario <= 60 THEN 'ALTO'
                    ELSE 'EXCESO'
                END
            """,
            
            # 7. Agregar columna de inventario crítico
            """
            ALTER TABLE fact_rotacion 
            ADD COLUMN inventario_critico BOOLEAN AFTER estado_inventario
            """,
            
            # 8. Actualizar inventario crítico
            """
            UPDATE fact_rotacion 
            SET inventario_critico = (dias_inventario < 7)
            """,
            
            # 9. Agregar columna de alta rentabilidad
            """
            ALTER TABLE fact_rotacion 
            ADD COLUMN alta_rentabilidad BOOLEAN AFTER margen_porcentaje
            """,
            
            # 10. Actualizar alta rentabilidad
            """
            UPDATE fact_rotacion 
            SET alta_rentabilidad = (margen_porcentaje > 30)
            """,
            
            # 11. Agregar índice para clasificación ABC
            """
            CREATE INDEX idx_fact_rotacion_abc ON fact_rotacion(abc_clasificacion)
            """,
            
            # 12. Agregar índice para estado de inventario
            """
            CREATE INDEX idx_fact_rotacion_estado_inv ON fact_rotacion(estado_inventario)
            """,
            
            # 13. Agregar índice para alta rentabilidad
            """
            CREATE INDEX idx_fact_rotacion_rentabilidad ON fact_rotacion(alta_rentabilidad)
            """
        ]
        
        # Ejecutar cada ajuste de forma secuencial
        for i, ajuste in enumerate(ajustes):
            try:
                print(f"Ejecutando ajuste {i+1}/{len(ajustes)}...")
                with engine.connect() as connection:
                    connection.execute(text(ajuste))
                    connection.commit()
                print(f"✅ Ajuste {i+1} ejecutado correctamente")
            except Exception as e:
                print(f"❌ Error en ajuste {i+1}: {str(e)}")
                print(f"SQL: {ajuste.strip()}")
        
        # Lista de vistas a crear
        vistas = [
            # Vista para análisis de rotación por PDV
            """
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
            GROUP BY p.codigo_pdv, p.nombre_pdv
            """,
            
            # Vista para productos estrella
            """
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
            ORDER BY f.venta_total DESC
            """,
            
            # Vista para inventario en exceso
            """
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
            ORDER BY valor_inmovilizado DESC
            """,
            
            # Vista para inventario crítico
            """
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
            ORDER BY f.abc_clasificacion, f.dias_inventario
            """
        ]
        
        # Crear las vistas
        print("\n🔄 Creando vistas para análisis...")
        for i, vista in enumerate(vistas):
            try:
                print(f"Creando vista {i+1}/{len(vistas)}...")
                with engine.connect() as connection:
                    connection.execute(text(vista))
                    connection.commit()
                print(f"✅ Vista {i+1} creada correctamente")
            except Exception as e:
                print(f"❌ Error al crear vista {i+1}: {str(e)}")
        
        # Verificar que las columnas se hayan añadido correctamente
        print("\n📋 Verificando estructura actualizada de fact_rotacion:")
        query_estructura = text("DESCRIBE fact_rotacion")
        
        with engine.connect() as connection:
            estructura = pd.read_sql(query_estructura, connection)
            
        print(estructura.to_string(index=False))
        
        # Verificar que las vistas se hayan creado correctamente
        print("\n📋 Verificando vistas creadas:")
        query_vistas = text("SHOW FULL TABLES IN gestion_compras WHERE TABLE_TYPE LIKE 'VIEW'")
        
        with engine.connect() as connection:
            vistas_creadas = pd.read_sql(query_vistas, connection)
            
        print(vistas_creadas.to_string(index=False))
        
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
        
        try:
            with engine.connect() as connection:
                ejemplo = pd.read_sql(query_ejemplo, connection)
                
            print(ejemplo.to_string(index=False))
        except Exception as e:
            print(f"❌ Error al obtener ejemplo de datos: {str(e)}")
        
        print("\n✅ Ajustes a fact_rotacion completados")
        
    except Exception as e:
        print(f"❌ Error general: {str(e)}")

if __name__ == "__main__":
    ejecutar_ajustes_fact_rotacion()
