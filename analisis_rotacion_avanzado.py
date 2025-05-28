# analisis_rotacion_avanzado.py
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
import os
from datetime import datetime

def analisis_rotacion_avanzado():
    """
    Realiza análisis avanzados de rotación de inventario utilizando las nuevas 
    columnas agregadas a la tabla fact_rotacion.
    """
    try:
        print("🔍 Iniciando análisis avanzado de rotación de inventario")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Crear directorio para guardar resultados
        ruta_resultados = os.path.join("E:/desarrollo/gestionCompras/resultados_analisis")
        os.makedirs(ruta_resultados, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Análisis por clasificación ABC
        print("\n📊 Análisis por clasificación ABC:")
        query_abc = text("""
            SELECT 
                abc_clasificacion,
                COUNT(*) as total_productos,
                ROUND(AVG(venta_unidades), 2) as promedio_venta_unidades,
                ROUND(AVG(dias_inventario), 2) as promedio_dias_inventario,
                ROUND(SUM(venta_total), 2) as venta_total,
                ROUND(SUM(inventario_unidades_final * costo_unitario), 2) as valor_inventario,
                ROUND((SUM(venta_total) / SUM(inventario_unidades_final * costo_unitario)) * 100, 2) as eficiencia_inventario
            FROM fact_rotacion
            GROUP BY abc_clasificacion
            ORDER BY abc_clasificacion
        """)
        
        with engine.connect() as connection:
            analisis_abc = pd.read_sql(query_abc, connection)
            
        print(analisis_abc.to_string(index=False))
        
        # Guardar en CSV
        ruta_abc = os.path.join(ruta_resultados, f"analisis_abc_{timestamp}.csv")
        analisis_abc.to_csv(ruta_abc, index=False)
        print(f"✅ Análisis ABC guardado en {ruta_abc}")
        
        # 2. Análisis por estado de inventario
        print("\n📊 Análisis por estado de inventario:")
        query_estado = text("""
            SELECT 
                estado_inventario,
                COUNT(*) as total_productos,
                ROUND(AVG(dias_inventario), 2) as promedio_dias_inventario,
                ROUND(SUM(inventario_unidades_final), 2) as total_unidades,
                ROUND(SUM(inventario_unidades_final * costo_unitario), 2) as valor_inventario,
                ROUND(AVG(rotacion_mes), 2) as promedio_rotacion
            FROM fact_rotacion
            GROUP BY estado_inventario
            ORDER BY 
                CASE 
                    WHEN estado_inventario = 'BAJO' THEN 1
                    WHEN estado_inventario = 'ÓPTIMO' THEN 2
                    WHEN estado_inventario = 'ALTO' THEN 3
                    WHEN estado_inventario = 'EXCESO' THEN 4
                    ELSE 5
                END
        """)
        
        with engine.connect() as connection:
            analisis_estado = pd.read_sql(query_estado, connection)
            
        print(analisis_estado.to_string(index=False))
        
        # Guardar en CSV
        ruta_estado = os.path.join(ruta_resultados, f"analisis_estado_inventario_{timestamp}.csv")
        analisis_estado.to_csv(ruta_estado, index=False)
        print(f"✅ Análisis por estado guardado en {ruta_estado}")
        
        # 3. Análisis de PDVs por distribución ABC
        print("\n📊 Análisis de PDVs por distribución de productos ABC:")
        query_pdv_abc = text("""
            SELECT 
                p.nombre_pdv,
                COUNT(*) as total_productos,
                SUM(CASE WHEN f.abc_clasificacion = 'A' THEN 1 ELSE 0 END) as productos_a,
                SUM(CASE WHEN f.abc_clasificacion = 'B' THEN 1 ELSE 0 END) as productos_b,
                SUM(CASE WHEN f.abc_clasificacion = 'C' THEN 1 ELSE 0 END) as productos_c,
                ROUND(SUM(CASE WHEN f.abc_clasificacion = 'A' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as porcentaje_a,
                ROUND(SUM(CASE WHEN f.abc_clasificacion = 'B' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as porcentaje_b,
                ROUND(SUM(CASE WHEN f.abc_clasificacion = 'C' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as porcentaje_c,
                ROUND(AVG(f.rotacion_mes), 2) as rotacion_promedio
            FROM fact_rotacion f
            JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
            GROUP BY p.nombre_pdv
            ORDER BY porcentaje_a DESC
        """)
        
        with engine.connect() as connection:
            analisis_pdv_abc = pd.read_sql(query_pdv_abc, connection)
            
        print(analisis_pdv_abc.to_string(index=False))
        
        # Guardar en CSV
        ruta_pdv_abc = os.path.join(ruta_resultados, f"analisis_pdv_abc_{timestamp}.csv")
        analisis_pdv_abc.to_csv(ruta_pdv_abc, index=False)
        print(f"✅ Análisis PDV por ABC guardado en {ruta_pdv_abc}")
        
        # 4. Análisis de inventario crítico por PDV
        print("\n📊 Análisis de inventario crítico por PDV:")
        query_critico = text("""
            SELECT 
                p.nombre_pdv,
                COUNT(*) as total_productos,
                SUM(CASE WHEN f.inventario_critico = 1 THEN 1 ELSE 0 END) as productos_criticos,
                ROUND(SUM(CASE WHEN f.inventario_critico = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as porcentaje_critico,
                SUM(CASE WHEN f.inventario_critico = 1 AND f.abc_clasificacion = 'A' THEN 1 ELSE 0 END) as criticos_clase_a,
                SUM(CASE WHEN f.inventario_critico = 1 AND f.abc_clasificacion = 'B' THEN 1 ELSE 0 END) as criticos_clase_b,
                SUM(CASE WHEN f.inventario_critico = 1 AND f.abc_clasificacion = 'C' THEN 1 ELSE 0 END) as criticos_clase_c
            FROM fact_rotacion f
            JOIN dim_pdv p ON f.pdv_sk = p.pdv_sk
            GROUP BY p.nombre_pdv
            ORDER BY criticos_clase_a DESC
        """)
        
        with engine.connect() as connection:
            analisis_critico = pd.read_sql(query_critico, connection)
            
        print(analisis_critico.to_string(index=False))
        
        # Guardar en CSV
        ruta_critico = os.path.join(ruta_resultados, f"analisis_inventario_critico_{timestamp}.csv")
        analisis_critico.to_csv(ruta_critico, index=False)
        print(f"✅ Análisis inventario crítico guardado en {ruta_critico}")
        
        # 5. Análisis de rentabilidad vs rotación
        print("\n📊 Análisis de rentabilidad vs rotación:")
        query_rentabilidad = text("""
            SELECT 
                abc_clasificacion,
                CASE 
                    WHEN alta_rentabilidad = 1 THEN 'Alta rentabilidad'
                    ELSE 'Baja rentabilidad'
                END as tipo_rentabilidad,
                COUNT(*) as total_productos,
                ROUND(AVG(margen_porcentaje), 2) as margen_promedio,
                ROUND(AVG(rotacion_mes), 2) as rotacion_promedio,
                ROUND(SUM(venta_total), 2) as venta_total,
                ROUND(SUM(margen_bruto), 2) as margen_bruto_total
            FROM fact_rotacion
            GROUP BY abc_clasificacion, alta_rentabilidad
            ORDER BY abc_clasificacion, alta_rentabilidad DESC
        """)
        
        with engine.connect() as connection:
            analisis_rentabilidad = pd.read_sql(query_rentabilidad, connection)
            
        print(analisis_rentabilidad.to_string(index=False))
        
        # Guardar en CSV
        ruta_rentabilidad = os.path.join(ruta_resultados, f"analisis_rentabilidad_rotacion_{timestamp}.csv")
        analisis_rentabilidad.to_csv(ruta_rentabilidad, index=False)
        print(f"✅ Análisis rentabilidad vs rotación guardado en {ruta_rentabilidad}")
        
        # 6. Resumen ejecutivo
        print("\n📊 Resumen ejecutivo del inventario:")
        query_resumen = text("""
            SELECT
                COUNT(*) as total_productos,
                ROUND(SUM(inventario_unidades_final * costo_unitario), 2) as valor_total_inventario,
                ROUND(SUM(venta_total), 2) as venta_total,
                ROUND(SUM(margen_bruto), 2) as margen_bruto_total,
                ROUND(AVG(rotacion_mes), 2) as rotacion_promedio,
                ROUND(AVG(dias_inventario), 2) as dias_inventario_promedio,
                SUM(CASE WHEN abc_clasificacion = 'A' THEN 1 ELSE 0 END) as productos_a,
                SUM(CASE WHEN abc_clasificacion = 'B' THEN 1 ELSE 0 END) as productos_b,
                SUM(CASE WHEN abc_clasificacion = 'C' THEN 1 ELSE 0 END) as productos_c,
                SUM(CASE WHEN inventario_critico = 1 THEN 1 ELSE 0 END) as productos_criticos,
                SUM(CASE WHEN estado_inventario = 'EXCESO' THEN 1 ELSE 0 END) as productos_exceso
            FROM fact_rotacion
        """)
        
        with engine.connect() as connection:
            resumen = pd.read_sql(query_resumen, connection)
            
        print(resumen.to_string(index=False))
        
        # Guardar en CSV
        ruta_resumen = os.path.join(ruta_resultados, f"resumen_ejecutivo_{timestamp}.csv")
        resumen.to_csv(ruta_resumen, index=False)
        print(f"✅ Resumen ejecutivo guardado en {ruta_resumen}")
        
        print(f"\n🎉 Análisis completado. Todos los resultados guardados en {ruta_resultados}")
        
    except Exception as e:
        print(f"❌ Error en el análisis: {str(e)}")

if __name__ == "__main__":
    analisis_rotacion_avanzado()
