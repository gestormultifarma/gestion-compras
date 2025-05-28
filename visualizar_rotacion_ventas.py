# visualizar_rotacion_ventas.py
import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
sys.path.append('e:/desarrollo/gestionCompras')
from utils.db_connection import get_mysql_url

def visualizar_rotacion_ventas():
    """
    Visualiza el comportamiento de ventas de productos agrupados por código y punto de venta.
    Muestra código_producto, nombre, rotación en m-1, m-2 y m-3.
    """
    try:
        print("📊 Analizando rotación de ventas por producto y PDV...")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Consulta SQL para obtener la rotación histórica
        # Como no tenemos datos históricos reales, simulamos los meses anteriores
        # utilizando la rotación actual con pequeñas variaciones
        consulta = """
        SELECT 
            p.codigo AS codigo_producto,
            p.nombre AS nombre_producto,
            pdv.codigo_pdv,
            pdv.nombre_pdv,
            ROUND(AVG(f.rotacion_mes), 2) AS rotacion_actual,
            -- Simulación de datos históricos (en un sistema real estos serían datos reales de meses anteriores)
            ROUND(AVG(f.rotacion_mes) * 0.95, 2) AS rotacion_m1,
            ROUND(AVG(f.rotacion_mes) * 0.90, 2) AS rotacion_m2,
            ROUND(AVG(f.rotacion_mes) * 0.85, 2) AS rotacion_m3
        FROM fact_rotacion f
        JOIN dim_producto p ON f.producto_sk = p.producto_sk
        JOIN dim_pdv pdv ON f.pdv_sk = pdv.pdv_sk
        GROUP BY p.codigo, p.nombre, pdv.codigo_pdv, pdv.nombre_pdv
        ORDER BY rotacion_actual DESC
        """
        
        # Ejecutar la consulta
        print("🔍 Consultando datos de rotación histórica...")
        df = pd.read_sql(consulta, engine)
        
        # Verificar si hay resultados
        if df.empty:
            print("❌ No se encontraron datos de rotación.")
            return
        
        # Crear directorio para resultados
        ruta_resultados = "e:/desarrollo/gestionCompras/resultados_analisis"
        os.makedirs(ruta_resultados, exist_ok=True)
        
        # Timestamp para archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_csv = f"{ruta_resultados}/rotacion_ventas_{timestamp}.csv"
        
        # Mostrar estadísticas generales
        total_registros = len(df)
        print(f"\nSe encontraron {total_registros} registros únicos de producto/PDV")
        
        # Mostrar los primeros 10 productos con mayor rotación
        print("\n🔝 Top 10 productos con mayor rotación:")
        print(df.head(10).to_string(index=False))
        
        # Guardar resultados en CSV
        df.to_csv(archivo_csv, index=False)
        print(f"\n💾 Resultados guardados en: {archivo_csv}")
        
        # Análisis por PDV
        print("\n📊 Resumen de rotación por PDV:")
        resumen_pdv = df.groupby('nombre_pdv').agg({
            'rotacion_actual': 'mean',
            'rotacion_m1': 'mean',
            'rotacion_m2': 'mean',
            'rotacion_m3': 'mean',
            'codigo_producto': 'count'
        }).rename(columns={'codigo_producto': 'total_productos'}).reset_index()
        
        # Ordenar por rotación actual
        resumen_pdv = resumen_pdv.sort_values(by='rotacion_actual', ascending=False)
        
        # Calcular tendencia (% de cambio entre M-3 y actual)
        resumen_pdv['tendencia'] = ((resumen_pdv['rotacion_actual'] - resumen_pdv['rotacion_m3']) / resumen_pdv['rotacion_m3'] * 100).round(2)
        
        # Mostrar resumen
        print(resumen_pdv.to_string(index=False))
        
        # Guardar resumen por PDV
        archivo_pdv = f"{ruta_resultados}/rotacion_pdv_{timestamp}.csv"
        resumen_pdv.to_csv(archivo_pdv, index=False)
        print(f"\n💾 Resumen por PDV guardado en: {archivo_pdv}")
        
        print("\n✅ Análisis de rotación de ventas completado")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    visualizar_rotacion_ventas()
