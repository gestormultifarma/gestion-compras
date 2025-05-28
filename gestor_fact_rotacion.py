# gestor_fact_rotacion.py
"""
Script consolidado para gestionar la tabla fact_rotacion.
Incluye funcionalidades para cargar, actualizar, verificar y analizar los datos de rotación.

Este script reemplaza múltiples scripts individuales creados durante el desarrollo:
- corregir_rotacion_cajas.py (carga de datos correcta en cajas)
- visualizar_rotacion_ventas.py (visualización de datos)
- verificar_estructura_fact_rotacion.py (verificación de estructura)
- analisis_rotacion_avanzado.py (análisis de rotación)
"""

import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
import matplotlib.pyplot as plt

class GestorFactRotacion:
    """Clase principal para gestionar la tabla fact_rotacion"""
    
    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
        self.directorio_resultados = "e:/desarrollo/gestionCompras/resultados_analisis"
        os.makedirs(self.directorio_resultados, exist_ok=True)
    
    def verificar_estructura(self):
        """Verifica la estructura actual de la tabla fact_rotacion"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("DESCRIBE fact_rotacion"))
                rows = result.fetchall()
                
                print("Estructura actual de la tabla fact_rotacion:")
                print("=" * 50)
                for row in rows:
                    print(f"- {row[0]} ({row[1]})")
                
                # Verificar si tiene las columnas esperadas
                columnas_esperadas = ['codigo_pdv', 'nombre_pdv', 'codigo_producto', 
                                      'nombre_producto', 'rotacion_m1', 'rotacion_m2', 'rotacion_m3']
                
                columnas_actuales = [row[0] for row in rows]
                
                columnas_faltantes = [col for col in columnas_esperadas if col not in columnas_actuales]
                if columnas_faltantes:
                    print("\nAdvertencia: Faltan las siguientes columnas esperadas:")
                    for col in columnas_faltantes:
                        print(f"- {col}")
                else:
                    print("\nLa tabla tiene todas las columnas esperadas.")
                    
                # Verificar conteo de registros
                count = connection.execute(text("SELECT COUNT(*) FROM fact_rotacion")).scalar()
                print(f"\nTotal de registros en fact_rotacion: {count}")
                
                # Verificar registros con ventas
                count_ventas = connection.execute(text(
                    "SELECT COUNT(*) FROM fact_rotacion WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0"
                )).scalar()
                print(f"Registros con ventas > 0 en algún período: {count_ventas}")
                
                return True
                
        except Exception as e:
            print(f"Error al verificar estructura: {str(e)}")
            return False
    
    def cargar_rotacion_cajas(self):
        """
        Carga los datos reales de ventas en cajas desde las tablas de staging
        en la tabla fact_rotacion. Este es el método correcto para poblar la tabla.
        """
        try:
            with self.engine.connect() as connection:
                print("Cargando datos de rotación en cajas en fact_rotacion...")
                
                # Limpiar la tabla
                print("Limpiando tabla fact_rotacion...")
                connection.execute(text("TRUNCATE TABLE fact_rotacion"))
                connection.commit()
                
                # Crear tabla temporal para acumular datos
                print("Creando tabla temporal para procesar datos...")
                connection.execute(text("DROP TABLE IF EXISTS temp_rotacion_cajas"))
                
                connection.execute(text("""
                CREATE TEMPORARY TABLE temp_rotacion_cajas (
                    codigo_pdv VARCHAR(20),
                    nombre_pdv VARCHAR(100),
                    codigo_producto VARCHAR(50),
                    nombre_producto VARCHAR(255),
                    periodo INT,
                    venta_cajas DECIMAL(10,2)
                )
                """))
                connection.commit()
                
                # Procesar cada tabla de staging
                print("Procesando tablas de staging...")
                
                # Obtener lista de tablas de rotación
                tablas_rotacion = connection.execute(text("SHOW TABLES LIKE 'stg_rotacion_%'")).fetchall()
                total_tablas = len(tablas_rotacion)
                print(f"Se encontraron {total_tablas} tablas de rotación para procesar")
                
                for i, (tabla,) in enumerate(tablas_rotacion):
                    # Extraer información del PDV y período desde el nombre de la tabla
                    partes = tabla.split('_')
                    try:
                        # El último elemento es el periodo
                        periodo = int(partes[-1])
                        # El penúltimo es el código de PDV
                        codigo_pdv = partes[-2]
                        # Los elementos intermedios forman el nombre del PDV
                        pdv_nombre_partes = partes[3:-2]
                        nombre_pdv = '_'.join(pdv_nombre_partes)
                        
                        # Obtener datos de ventas en CAJAS
                        query = f"""
                        INSERT INTO temp_rotacion_cajas (
                            codigo_pdv, 
                            nombre_pdv,
                            codigo_producto,
                            nombre_producto,
                            periodo,
                            venta_cajas
                        )
                        SELECT 
                            '{codigo_pdv}' as codigo_pdv,
                            '{nombre_pdv}' as nombre_pdv,
                            codigo as codigo_producto,
                            nombre as nombre_producto,
                            {periodo} as periodo,
                            COALESCE(venta_caja, 0) as venta_cajas
                        FROM {tabla}
                        """
                        connection.execute(text(query))
                        connection.commit()
                        
                        # Mostrar progreso
                        if (i+1) % 10 == 0 or (i+1) == total_tablas:
                            print(f"Procesadas {i+1}/{total_tablas} tablas...")
                        
                    except Exception as e:
                        print(f"Error procesando tabla {tabla}: {str(e)}")
                
                # Actualizar nombres de PDV desde dim_pdv
                print("Actualizando nombres de PDV...")
                connection.execute(text("""
                UPDATE temp_rotacion_cajas t
                JOIN dim_pdv d ON t.codigo_pdv = d.codigo_pdv
                SET t.nombre_pdv = d.nombre_pdv
                """))
                connection.commit()
                
                # Consolidar datos en fact_rotacion
                print("Consolidando datos en fact_rotacion...")
                
                # Insertar registros consolidados por PDV y producto
                insert_query = """
                INSERT INTO fact_rotacion (
                    codigo_pdv,
                    nombre_pdv,
                    codigo_producto,
                    nombre_producto,
                    rotacion_m1,
                    rotacion_m2,
                    rotacion_m3
                )
                SELECT 
                    t.codigo_pdv,
                    t.nombre_pdv,
                    t.codigo_producto,
                    MAX(t.nombre_producto) as nombre_producto,
                    SUM(CASE WHEN t.periodo = 1 THEN t.venta_cajas ELSE 0 END) as rotacion_m1,
                    SUM(CASE WHEN t.periodo = 2 THEN t.venta_cajas ELSE 0 END) as rotacion_m2,
                    SUM(CASE WHEN t.periodo = 3 THEN t.venta_cajas ELSE 0 END) as rotacion_m3
                FROM temp_rotacion_cajas t
                GROUP BY t.codigo_pdv, t.nombre_pdv, t.codigo_producto
                """
                connection.execute(text(insert_query))
                connection.commit()
                
                # Verificar resultados
                total_registros = connection.execute(text("SELECT COUNT(*) FROM fact_rotacion")).scalar()
                
                # Verificar registros con ventas > 0
                total_con_ventas = connection.execute(text(
                    "SELECT COUNT(*) FROM fact_rotacion WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0"
                )).scalar()
                
                print(f"\nResultados:")
                print(f"Total de registros en fact_rotacion: {total_registros}")
                print(f"Registros con ventas > 0 en algún período: {total_con_ventas}")
                
                # Mostrar ejemplos de registros con ventas
                resultado_ejemplos = connection.execute(text("""
                SELECT *
                FROM fact_rotacion
                WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0
                LIMIT 10
                """)).fetchall()
                
                print("\nEjemplos de registros con ventas (CAJAS):")
                print("=" * 100)
                print(f"{'CODIGO_PDV':10} | {'NOMBRE_PDV':15} | {'CODIGO_PRODUCTO':15} | {'NOMBRE_PRODUCTO':30} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
                print("-" * 100)
                
                for row in resultado_ejemplos:
                    print(f"{row[0]:10} | {row[1][:15]:15} | {row[2][:15]:15} | {row[3][:30]:30} | {row[4] or 0:7.2f} | {row[5] or 0:7.2f} | {row[6] or 0:7.2f}")
                
                print("\n✅ Carga de datos completada exitosamente")
                return True
                
        except Exception as e:
            import traceback
            print(f"❌ Error al cargar datos: {str(e)}")
            traceback.print_exc()
            return False
    
    def visualizar_rotacion(self):
        """
        Visualiza el comportamiento de ventas de productos agrupados por código y punto de venta.
        Muestra código_producto, nombre, rotación en m-1, m-2 y m-3.
        """
        try:
            print("📊 Analizando rotación de ventas por producto y PDV...")
            
            # Consulta SQL para obtener la rotación histórica
            consulta = """
            SELECT 
                codigo_pdv,
                nombre_pdv,
                codigo_producto,
                nombre_producto,
                rotacion_m1,
                rotacion_m2,
                rotacion_m3,
                (rotacion_m1 + rotacion_m2 + rotacion_m3) as rotacion_total
            FROM fact_rotacion
            WHERE rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0
            ORDER BY rotacion_total DESC
            """
            
            # Ejecutar la consulta
            print("🔍 Consultando datos de rotación histórica...")
            df = pd.read_sql(consulta, self.engine)
            
            # Verificar si hay resultados
            if df.empty:
                print("❌ No se encontraron datos de rotación.")
                return
            
            # Timestamp para archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archivo_csv = f"{self.directorio_resultados}/rotacion_ventas_{timestamp}.csv"
            
            # Mostrar estadísticas generales
            total_registros = len(df)
            print(f"\nSe encontraron {total_registros} registros con ventas")
            
            # Calcular tendencia (% de cambio entre M-3 y M-1)
            df['tendencia'] = 0.0
            mask = df['rotacion_m3'] > 0
            df.loc[mask, 'tendencia'] = ((df.loc[mask, 'rotacion_m1'] - df.loc[mask, 'rotacion_m3']) / df.loc[mask, 'rotacion_m3'] * 100).round(2)
            
            # Mostrar los primeros 10 productos con mayor rotación total
            print("\n🔝 Top 10 productos con mayor rotación total:")
            print(df.head(10).to_string(index=False))
            
            # Guardar resultados en CSV
            df.to_csv(archivo_csv, index=False)
            print(f"\n💾 Resultados guardados en: {archivo_csv}")
            
            # Análisis por PDV
            print("\n📊 Resumen de rotación por PDV:")
            resumen_pdv = df.groupby('nombre_pdv').agg({
                'rotacion_m1': 'sum',
                'rotacion_m2': 'sum',
                'rotacion_m3': 'sum',
                'codigo_producto': 'count'
            }).rename(columns={'codigo_producto': 'total_productos'}).reset_index()
            
            # Ordenar por rotación total
            resumen_pdv['rotacion_total'] = resumen_pdv['rotacion_m1'] + resumen_pdv['rotacion_m2'] + resumen_pdv['rotacion_m3']
            resumen_pdv = resumen_pdv.sort_values(by='rotacion_total', ascending=False)
            
            # Calcular tendencia para PDV
            resumen_pdv['tendencia'] = ((resumen_pdv['rotacion_m1'] - resumen_pdv['rotacion_m3']) / resumen_pdv['rotacion_m3'] * 100).round(2)
            
            # Mostrar resumen
            print(resumen_pdv.to_string(index=False))
            
            # Guardar resumen por PDV
            archivo_pdv = f"{self.directorio_resultados}/rotacion_pdv_{timestamp}.csv"
            resumen_pdv.to_csv(archivo_pdv, index=False)
            print(f"\n💾 Resumen por PDV guardado en: {archivo_pdv}")
            
            # Crear gráfico de tendencias
            try:
                plt.figure(figsize=(12, 6))
                
                # Gráfico de barras para rotación por PDV
                plt.subplot(1, 2, 1)
                top_pdvs = resumen_pdv.head(10)
                bars = plt.barh(top_pdvs['nombre_pdv'], top_pdvs['rotacion_total'])
                plt.xlabel('Rotación Total (Cajas)')
                plt.ylabel('Punto de Venta')
                plt.title('Top 10 PDVs por Rotación Total')
                plt.tight_layout()
                
                # Gráfico de líneas para tendencia temporal
                plt.subplot(1, 2, 2)
                for i, pdv in enumerate(top_pdvs['nombre_pdv'][:5]):  # Solo los primeros 5 para claridad
                    pdv_data = top_pdvs[top_pdvs['nombre_pdv'] == pdv]
                    plt.plot(['M-3', 'M-2', 'M-1'], 
                              [pdv_data['rotacion_m3'].values[0], 
                               pdv_data['rotacion_m2'].values[0], 
                               pdv_data['rotacion_m1'].values[0]], 
                              marker='o', label=pdv)
                
                plt.xlabel('Período')
                plt.ylabel('Rotación (Cajas)')
                plt.title('Tendencia de Rotación por PDV')
                plt.legend()
                plt.tight_layout()
                
                # Guardar gráfico
                grafico_path = f"{self.directorio_resultados}/grafico_rotacion_{timestamp}.png"
                plt.savefig(grafico_path)
                print(f"📈 Gráfico guardado en: {grafico_path}")
                
            except Exception as e:
                print(f"No se pudo generar gráficos: {str(e)}")
            
            print("\n✅ Análisis de rotación de ventas completado")
            return True
            
        except Exception as e:
            import traceback
            print(f"❌ Error: {str(e)}")
            traceback.print_exc()
            return False
    
    def analizar_tendencias(self):
        """
        Realiza un análisis avanzado de tendencias en la rotación de productos
        para identificar productos en crecimiento y decrecimiento.
        """
        try:
            print("🔍 Analizando tendencias de rotación...")
            
            # Obtener datos de rotación con tendencias
            query = """
            SELECT 
                codigo_pdv,
                nombre_pdv,
                codigo_producto,
                nombre_producto,
                rotacion_m1,
                rotacion_m2,
                rotacion_m3,
                CASE 
                    WHEN rotacion_m3 > 0 THEN ((rotacion_m1 - rotacion_m3) / rotacion_m3) * 100
                    ELSE 0 
                END as tendencia_porcentaje
            FROM fact_rotacion
            WHERE (rotacion_m1 > 0 OR rotacion_m2 > 0 OR rotacion_m3 > 0)
            """
            
            df = pd.read_sql(query, self.engine)
            
            # Verificar si hay datos
            if df.empty:
                print("No hay datos suficientes para analizar tendencias.")
                return
            
            # Clasificar productos según tendencia
            df['tendencia_porcentaje'] = df['tendencia_porcentaje'].astype(float)
            df['clasificacion_tendencia'] = pd.cut(
                df['tendencia_porcentaje'], 
                bins=[-float('inf'), -20, 20, float('inf')],
                labels=['Decreciente', 'Estable', 'Creciente']
            )
            
            # Timestamp para archivos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Analizar productos en crecimiento
            print("\n🚀 Productos con mayor crecimiento:")
            productos_crecimiento = df[df['tendencia_porcentaje'] > 20].sort_values(by='tendencia_porcentaje', ascending=False)
            print(productos_crecimiento.head(10).to_string(index=False))
            
            # Analizar productos en decrecimiento
            print("\n📉 Productos con mayor decrecimiento:")
            productos_decrecimiento = df[df['tendencia_porcentaje'] < -20].sort_values(by='tendencia_porcentaje')
            print(productos_decrecimiento.head(10).to_string(index=False))
            
            # Distribución por clasificación
            print("\nDistribución de productos por tendencia:")
            distribucion = df['clasificacion_tendencia'].value_counts()
            print(distribucion)
            
            # Guardar resultados
            archivo_tendencias = f"{self.directorio_resultados}/tendencias_rotacion_{timestamp}.csv"
            df.to_csv(archivo_tendencias, index=False)
            print(f"\n💾 Análisis de tendencias guardado en: {archivo_tendencias}")
            
            # Crear gráfico de distribución de tendencias
            try:
                plt.figure(figsize=(10, 6))
                distribucion.plot(kind='pie', autopct='%1.1f%%')
                plt.title('Distribución de Productos por Tendencia')
                plt.tight_layout()
                
                # Guardar gráfico
                grafico_path = f"{self.directorio_resultados}/grafico_tendencias_{timestamp}.png"
                plt.savefig(grafico_path)
                print(f"📈 Gráfico guardado en: {grafico_path}")
                
            except Exception as e:
                print(f"No se pudo generar gráficos: {str(e)}")
            
            print("\n✅ Análisis de tendencias completado")
            return True
            
        except Exception as e:
            print(f"Error al analizar tendencias: {str(e)}")
            return False
    
    def crear_fact_rotacion(self):
        """
        Crea o recrea la tabla fact_rotacion con la estructura correcta
        """
        try:
            with self.engine.connect() as connection:
                print("Creando/recreando tabla fact_rotacion...")
                
                # Verificar si la tabla existe
                table_exists = connection.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'gestion_compras' 
                    AND table_name = 'fact_rotacion'
                """)).scalar() > 0
                
                # Si existe, eliminarla
                if table_exists:
                    print("La tabla ya existe. Eliminando...")
                    connection.execute(text("DROP TABLE fact_rotacion"))
                    connection.commit()
                
                # Crear la tabla con la estructura correcta
                connection.execute(text("""
                CREATE TABLE fact_rotacion (
                    codigo_pdv VARCHAR(20) NOT NULL,
                    nombre_pdv VARCHAR(100),
                    codigo_producto VARCHAR(50) NOT NULL,
                    nombre_producto VARCHAR(255),
                    rotacion_m1 DECIMAL(7,2),
                    rotacion_m2 DECIMAL(7,2),
                    rotacion_m3 DECIMAL(7,2),
                    PRIMARY KEY (codigo_pdv, codigo_producto)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                connection.commit()
                
                print("✅ Tabla fact_rotacion creada correctamente")
                return True
                
        except Exception as e:
            print(f"Error al crear tabla: {str(e)}")
            return False


def mostrar_menu():
    """Muestra el menú principal del gestor"""
    print("\n" + "=" * 50)
    print("GESTOR DE FACT_ROTACION")
    print("=" * 50)
    print("1. Verificar estructura de fact_rotacion")
    print("2. Crear/recrear tabla fact_rotacion")
    print("3. Cargar datos de rotación (en cajas)")
    print("4. Visualizar rotación de ventas")
    print("5. Analizar tendencias de rotación")
    print("0. Salir")
    print("=" * 50)
    return input("Seleccione una opción: ")


if __name__ == "__main__":
    gestor = GestorFactRotacion()
    
    if len(sys.argv) > 1:
        # Si hay argumentos, ejecutar la función específica
        comando = sys.argv[1].lower()
        
        if comando == "verificar":
            gestor.verificar_estructura()
        elif comando == "crear":
            gestor.crear_fact_rotacion()
        elif comando == "cargar":
            gestor.cargar_rotacion_cajas()
        elif comando == "visualizar":
            gestor.visualizar_rotacion()
        elif comando == "tendencias":
            gestor.analizar_tendencias()
        elif comando == "completo":
            # Ejecutar proceso completo
            gestor.crear_fact_rotacion()
            gestor.cargar_rotacion_cajas()
            gestor.visualizar_rotacion()
            gestor.analizar_tendencias()
        else:
            print(f"Comando no reconocido: {comando}")
            print("Comandos válidos: verificar, crear, cargar, visualizar, tendencias, completo")
    else:
        # Modo interactivo
        salir = False
        while not salir:
            opcion = mostrar_menu()
            
            if opcion == "1":
                gestor.verificar_estructura()
            elif opcion == "2":
                gestor.crear_fact_rotacion()
            elif opcion == "3":
                gestor.cargar_rotacion_cajas()
            elif opcion == "4":
                gestor.visualizar_rotacion()
            elif opcion == "5":
                gestor.analizar_tendencias()
            elif opcion == "0":
                salir = True
                print("¡Hasta pronto!")
            else:
                print("Opción no válida. Intente de nuevo.")
