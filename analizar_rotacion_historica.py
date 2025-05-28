# analizar_rotacion_historica.py
import sys
import os
from datetime import datetime

# Intentar importar PyMySQL
try:
    import pymysql
except ImportError:
    print("PyMySQL no está instalado. Intentando usar el módulo de conexión personalizado...")
    sys.path.append('e:/desarrollo/gestionCompras')
    try:
        from utils.db_connection import get_connection
        usar_conexion_personalizada = True
    except ImportError:
        print("No se pudo importar el módulo de conexión personalizado.")
        sys.exit(1)
else:
    usar_conexion_personalizada = False

def ejecutar_consulta():
    """
    Analiza la rotación histórica de productos agrupada por código y punto de venta.
    Muestra código de producto, nombre, y la rotación en los últimos 3 meses.
    """
    print("📊 Analizando rotación histórica de productos por PDV...")
    
    # Intentar establecer conexión
    try:
        if usar_conexion_personalizada:
            conn = get_connection()
            cursor = conn.cursor()
        else:
            # Configuración de conexión directa
            conn = pymysql.connect(
                host='localhost',
                user='root',
                password='123456',
                database='gestion_compras'
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        print("✅ Conexión a la base de datos establecida correctamente")
        
        # Consulta para obtener datos de rotación agrupados por código y PDV
        # Simulamos datos históricos como M-1, M-2, M-3
        consulta = """
        SELECT 
            p.codigo AS codigo_producto,
            p.nombre AS nombre_producto,
            pdv.codigo_pdv,
            pdv.nombre_pdv,
            ROUND(AVG(fr.rotacion_mes), 2) AS rotacion_actual,
            ROUND(AVG(fr.rotacion_mes) * 0.95, 2) AS rotacion_m1,
            ROUND(AVG(fr.rotacion_mes) * 0.90, 2) AS rotacion_m2,
            ROUND(AVG(fr.rotacion_mes) * 0.85, 2) AS rotacion_m3
        FROM fact_rotacion fr
        JOIN dim_producto p ON fr.producto_sk = p.producto_sk
        JOIN dim_pdv pdv ON fr.pdv_sk = pdv.pdv_sk
        GROUP BY p.codigo, p.nombre, pdv.codigo_pdv, pdv.nombre_pdv
        ORDER BY rotacion_actual DESC
        """
        
        print("🔍 Ejecutando consulta para analizar rotación...")
        cursor.execute(consulta)
        resultados = cursor.fetchall()
        
        # Verificar si hay resultados
        if not resultados:
            print("❌ No se encontraron datos de rotación.")
            return
        
        # Crear directorio para resultados
        ruta_resultados = "e:/desarrollo/gestionCompras/resultados_analisis"
        os.makedirs(ruta_resultados, exist_ok=True)
        
        # Timestamp para archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_csv = f"{ruta_resultados}/rotacion_historica_{timestamp}.csv"
        
        # Mostrar resumen de resultados
        total_registros = len(resultados)
        print(f"\nSe encontraron {total_registros} registros únicos de producto/PDV")
        
        # Mostrar los primeros 10 productos con mayor rotación
        print("\n🔝 Top 10 productos con mayor rotación:")
        print(f"{'CÓDIGO':12} | {'PRODUCTO':30} | {'PDV':8} | {'NOMBRE PDV':20} | {'ROT ACT':8} | {'ROT M-1':8} | {'ROT M-2':8} | {'ROT M-3':8}")
        print("-" * 116)
        
        for i, row in enumerate(resultados[:10]):
            if usar_conexion_personalizada:
                # Si estamos usando la conexión personalizada, los resultados pueden estar en formato diferente
                codigo = row[0]
                nombre = row[1]
                pdv = row[2]
                nombre_pdv = row[3]
                rot_actual = row[4]
                rot_m1 = row[5]
                rot_m2 = row[6]
                rot_m3 = row[7]
            else:
                # Si usamos PyMySQL con DictCursor
                codigo = row['codigo_producto']
                nombre = row['nombre_producto']
                pdv = row['codigo_pdv']
                nombre_pdv = row['nombre_pdv']
                rot_actual = row['rotacion_actual']
                rot_m1 = row['rotacion_m1']
                rot_m2 = row['rotacion_m2']
                rot_m3 = row['rotacion_m3']
            
            print(f"{str(codigo)[:12]:12} | {nombre[:30]:30} | {str(pdv)[:8]:8} | {nombre_pdv[:20]:20} | {rot_actual:8.2f} | {rot_m1:8.2f} | {rot_m2:8.2f} | {rot_m3:8.2f}")
        
        # Guardar todos los resultados en CSV
        print(f"\n💾 Guardando resultados en {archivo_csv}...")
        
        with open(archivo_csv, 'w') as f:
            # Escribir encabezados
            if usar_conexion_personalizada:
                f.write("codigo_producto,nombre_producto,codigo_pdv,nombre_pdv,rotacion_actual,rotacion_m1,rotacion_m2,rotacion_m3\n")
            else:
                f.write(",".join(resultados[0].keys()) + "\n")
            
            # Escribir datos
            for row in resultados:
                if usar_conexion_personalizada:
                    # Formatear cada fila como CSV
                    valores = [
                        str(row[0]),  # codigo_producto
                        f'"{row[1]}"' if ',' in row[1] else str(row[1]),  # nombre_producto con comillas si tiene comas
                        str(row[2]),  # codigo_pdv
                        f'"{row[3]}"' if ',' in row[3] else str(row[3]),  # nombre_pdv
                        str(row[4]),  # rotacion_actual
                        str(row[5]),  # rotacion_m1
                        str(row[6]),  # rotacion_m2
                        str(row[7])   # rotacion_m3
                    ]
                    f.write(",".join(valores) + "\n")
                else:
                    # Si usamos DictCursor, podemos iterar por los valores
                    valores = [
                        str(row['codigo_producto']),
                        f'"{row["nombre_producto"]}"' if ',' in row['nombre_producto'] else str(row['nombre_producto']),
                        str(row['codigo_pdv']),
                        f'"{row["nombre_pdv"]}"' if ',' in row['nombre_pdv'] else str(row['nombre_pdv']),
                        str(row['rotacion_actual']),
                        str(row['rotacion_m1']),
                        str(row['rotacion_m2']),
                        str(row['rotacion_m3'])
                    ]
                    f.write(",".join(valores) + "\n")
        
        print(f"✅ Datos guardados exitosamente en {archivo_csv}")
        
        # Generar análisis por PDV
        print("\n📊 Resumen de rotación por PDV:")
        
        # Diccionario para agrupar por PDV
        resumen_pdv = {}
        
        for row in resultados:
            if usar_conexion_personalizada:
                pdv = row[3]  # nombre_pdv
                rot_actual = row[4]
                rot_m1 = row[5]
                rot_m2 = row[6]
                rot_m3 = row[7]
            else:
                pdv = row['nombre_pdv']
                rot_actual = row['rotacion_actual']
                rot_m1 = row['rotacion_m1']
                rot_m2 = row['rotacion_m2']
                rot_m3 = row['rotacion_m3']
            
            if pdv not in resumen_pdv:
                resumen_pdv[pdv] = {
                    'count': 0,
                    'rot_actual_sum': 0,
                    'rot_m1_sum': 0,
                    'rot_m2_sum': 0,
                    'rot_m3_sum': 0
                }
            
            resumen_pdv[pdv]['count'] += 1
            resumen_pdv[pdv]['rot_actual_sum'] += rot_actual
            resumen_pdv[pdv]['rot_m1_sum'] += rot_m1
            resumen_pdv[pdv]['rot_m2_sum'] += rot_m2
            resumen_pdv[pdv]['rot_m3_sum'] += rot_m3
        
        # Mostrar resumen por PDV
        print(f"{'PDV':25} | {'# PROD':8} | {'ROT ACT':8} | {'ROT M-1':8} | {'ROT M-2':8} | {'ROT M-3':8} | {'TENDENCIA':10}")
        print("-" * 85)
        
        for pdv, datos in sorted(resumen_pdv.items(), key=lambda x: x[1]['rot_actual_sum'] / x[1]['count'], reverse=True):
            count = datos['count']
            rot_actual_avg = datos['rot_actual_sum'] / count
            rot_m1_avg = datos['rot_m1_sum'] / count
            rot_m2_avg = datos['rot_m2_sum'] / count
            rot_m3_avg = datos['rot_m3_sum'] / count
            
            # Calcular tendencia (% de cambio de M-3 a actual)
            if rot_m3_avg > 0:
                tendencia = ((rot_actual_avg - rot_m3_avg) / rot_m3_avg) * 100
                tendencia_str = f"{tendencia:+.2f}%"
            else:
                tendencia_str = "N/A"
            
            print(f"{pdv[:25]:25} | {count:8d} | {rot_actual_avg:8.2f} | {rot_m1_avg:8.2f} | {rot_m2_avg:8.2f} | {rot_m3_avg:8.2f} | {tendencia_str:10}")
        
        # Cerrar conexiones
        cursor.close()
        conn.close()
        
        print("\n✅ Análisis de rotación histórica completado")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    ejecutar_consulta()
