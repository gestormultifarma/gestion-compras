# visualizar_rotacion_simple.py
import os
import mysql.connector
from datetime import datetime

def ejecutar_vista_rotacion():
    """
    Crea una vista en la base de datos para analizar la rotación histórica
    y muestra los resultados agrupados por producto y PDV.
    """
    try:
        print("📊 Creando vista de rotación histórica y analizando datos...")
        
        # Leer el archivo SQL
        ruta_sql = "e:/desarrollo/gestionCompras/crear_vista_rotacion.sql"
        with open(ruta_sql, 'r') as f:
            sql_script = f.read()
        
        # Establecer conexión a la base de datos
        # Ajusta estos valores según tu configuración
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'gestion_compras'
        }
        
        # Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Ejecutar el script SQL por partes (cada sentencia)
        statements = sql_script.split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)
                conn.commit()
        
        print("✅ Vistas de análisis creadas correctamente")
        
        # Consultar los resultados de la vista
        print("\n🔍 Consultando resultados de rotación histórica...")
        
        # Obtener el conteo total de registros
        cursor.execute("SELECT COUNT(*) FROM vw_rotacion_historica")
        total_registros = cursor.fetchone()[0]
        print(f"Total de productos con análisis de rotación: {total_registros}")
        
        # Obtener los 10 productos con mayor tendencia de crecimiento
        print("\n🔝 Top 10 productos con mayor crecimiento en rotación:")
        cursor.execute("""
            SELECT 
                codigo_producto, 
                nombre_producto, 
                codigo_pdv, 
                nombre_pdv, 
                rotacion_m3, 
                rotacion_m2, 
                rotacion_m1, 
                rotacion_actual, 
                tendencia_porcentaje
            FROM vw_rotacion_historica
            ORDER BY tendencia_porcentaje DESC
            LIMIT 10
        """)
        
        # Mostrar los resultados en formato tabular
        resultados = cursor.fetchall()
        print(f"{'CÓDIGO':10} | {'PRODUCTO':30} | {'PDV':5} | {'NOMBRE PDV':15} | {'ROT M-3':8} | {'ROT M-2':8} | {'ROT M-1':8} | {'ROT ACT':8} | {'TEND %':8}")
        print("-" * 110)
        
        for row in resultados:
            codigo, nombre, cod_pdv, nombre_pdv, rot_m3, rot_m2, rot_m1, rot_act, tend = row
            print(f"{codigo[:10]:10} | {nombre[:30]:30} | {cod_pdv[:5]:5} | {nombre_pdv[:15]:15} | {rot_m3:8.2f} | {rot_m2:8.2f} | {rot_m1:8.2f} | {rot_act:8.2f} | {tend:8.2f}")
        
        # Obtener los 5 productos con mayor caída
        print("\n🔻 5 productos con mayor caída en rotación:")
        cursor.execute("""
            SELECT 
                codigo_producto, 
                nombre_producto, 
                codigo_pdv, 
                nombre_pdv, 
                rotacion_m3, 
                rotacion_m2, 
                rotacion_m1, 
                rotacion_actual, 
                tendencia_porcentaje
            FROM vw_rotacion_historica
            WHERE rotacion_m3 > 0
            ORDER BY tendencia_porcentaje ASC
            LIMIT 5
        """)
        
        # Mostrar los resultados en formato tabular
        resultados = cursor.fetchall()
        print(f"{'CÓDIGO':10} | {'PRODUCTO':30} | {'PDV':5} | {'NOMBRE PDV':15} | {'ROT M-3':8} | {'ROT M-2':8} | {'ROT M-1':8} | {'ROT ACT':8} | {'TEND %':8}")
        print("-" * 110)
        
        for row in resultados:
            codigo, nombre, cod_pdv, nombre_pdv, rot_m3, rot_m2, rot_m1, rot_act, tend = row
            print(f"{codigo[:10]:10} | {nombre[:30]:30} | {cod_pdv[:5]:5} | {nombre_pdv[:15]:15} | {rot_m3:8.2f} | {rot_m2:8.2f} | {rot_m1:8.2f} | {rot_act:8.2f} | {tend:8.2f}")
        
        # Mostrar resumen por PDV
        print("\n📊 Resumen de rotación por PDV:")
        cursor.execute("""
            SELECT 
                nombre_pdv,
                codigo_pdv,
                total_productos,
                rotacion_actual_prom,
                rotacion_m1_prom,
                rotacion_m2_prom,
                rotacion_m3_prom,
                tendencia_prom
            FROM vw_rotacion_pdv
            ORDER BY tendencia_prom DESC
        """)
        
        resultados = cursor.fetchall()
        print(f"{'NOMBRE PDV':20} | {'COD PDV':7} | {'# PROD':7} | {'ROT ACT':8} | {'ROT M-1':8} | {'ROT M-2':8} | {'ROT M-3':8} | {'TEND %':8}")
        print("-" * 90)
        
        for row in resultados:
            nombre_pdv, cod_pdv, total, rot_act, rot_m1, rot_m2, rot_m3, tend = row
            print(f"{nombre_pdv[:20]:20} | {cod_pdv[:7]:7} | {total:7d} | {rot_act:8.2f} | {rot_m1:8.2f} | {rot_m2:8.2f} | {rot_m3:8.2f} | {tend:8.2f}")
        
        # Exportar resultados a archivos CSV
        try:
            # Crear directorio para resultados
            ruta_resultados = os.path.join("E:/desarrollo/gestionCompras/resultados_analisis")
            os.makedirs(ruta_resultados, exist_ok=True)
            
            # Timestamp para nombres de archivos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Exportar datos completos
            cursor.execute("SELECT * FROM vw_rotacion_historica")
            resultados = cursor.fetchall()
            
            # Obtener nombres de columnas
            cursor.execute("SHOW COLUMNS FROM vw_rotacion_historica")
            columnas = [column[0] for column in cursor.fetchall()]
            
            # Crear archivo CSV
            ruta_csv = os.path.join(ruta_resultados, f"rotacion_historica_{timestamp}.csv")
            with open(ruta_csv, 'w') as f:
                # Escribir encabezados
                f.write(','.join(columnas) + '\n')
                
                # Escribir datos
                for row in resultados:
                    f.write(','.join([str(val) for val in row]) + '\n')
            
            print(f"\n✅ Resultados completos guardados en: {ruta_csv}")
            
            # Exportar resumen por PDV
            cursor.execute("SELECT * FROM vw_rotacion_pdv")
            resultados = cursor.fetchall()
            
            # Obtener nombres de columnas
            cursor.execute("SHOW COLUMNS FROM vw_rotacion_pdv")
            columnas = [column[0] for column in cursor.fetchall()]
            
            # Crear archivo CSV
            ruta_csv_pdv = os.path.join(ruta_resultados, f"rotacion_pdv_{timestamp}.csv")
            with open(ruta_csv_pdv, 'w') as f:
                # Escribir encabezados
                f.write(','.join(columnas) + '\n')
                
                # Escribir datos
                for row in resultados:
                    f.write(','.join([str(val) for val in row]) + '\n')
            
            print(f"✅ Resumen por PDV guardado en: {ruta_csv_pdv}")
            
        except Exception as e:
            print(f"⚠️ Error al exportar resultados: {str(e)}")
        
        # Cerrar conexiones
        cursor.close()
        conn.close()
        
        print("\n✅ Análisis de rotación histórica completado")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    ejecutar_vista_rotacion()
