"""
Gestor de Fact Inventarios
--------------------------
Script para gestionar la tabla fact_inventarios, incluyendo funciones para:
1. Cargar datos desde las tablas de staging
2. Verificar la estructura y datos
3. Generar reportes básicos

Este script está diseñado para ser simple y directo, evitando complejidades
innecesarias que puedan causar errores.
"""

import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

class GestorFactInventarios:
    """Gestor para la tabla fact_inventarios"""
    
    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
        self.fecha_proceso = datetime.now().date()
    
    def verificar_estructura(self):
        """Verifica la estructura de la tabla fact_inventarios"""
        print("\n=== ESTRUCTURA DE FACT_INVENTARIOS ===")
        
        try:
            with self.engine.connect() as conn:
                # Verificar si la tabla existe
                query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name = 'fact_inventarios'
                """
                result = conn.execute(text(query))
                tabla_existe = result.scalar() > 0
                
                if not tabla_existe:
                    print("❌ La tabla fact_inventarios no existe")
                    return False
                
                # Obtener estructura de la tabla
                query = "DESCRIBE fact_inventarios"
                result = conn.execute(text(query))
                campos = result.fetchall()
                
                print("Campos de la tabla:")
                for campo in campos:
                    print(f"- {campo[0]} ({campo[1]})")
                
                # Verificar índices
                query = "SHOW INDEX FROM fact_inventarios"
                result = conn.execute(text(query))
                indices = result.fetchall()
                
                print("\nÍndices de la tabla:")
                for idx in indices:
                    print(f"- {idx[2]} (Columna: {idx[4]})")
                
                # Verificar registros existentes
                query = "SELECT COUNT(*) FROM fact_inventarios"
                result = conn.execute(text(query))
                count = result.scalar()
                
                print(f"\nTotal de registros: {count}")
                
                return True
                
        except Exception as e:
            print(f"❌ Error al verificar estructura: {str(e)}")
            return False
    
    def listar_tablas_staging(self):
        """Lista todas las tablas de staging de inventario"""
        print("\n=== TABLAS DE STAGING DE INVENTARIO ===")
        
        try:
            with self.engine.connect() as conn:
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg\\_inventario\\_%'
                """
                result = conn.execute(text(query))
                tablas = [row[0] for row in result]
                
                print(f"Se encontraron {len(tablas)} tablas:")
                for tabla in tablas:
                    # Contar registros en la tabla
                    count_query = f"SELECT COUNT(*) FROM `{tabla}`"
                    count = conn.execute(text(count_query)).scalar()
                    
                    # Contar registros con inventario
                    inv_query = f"SELECT COUNT(*) FROM `{tabla}` WHERE inventario_caja > 0"
                    inv_count = conn.execute(text(inv_query)).scalar()
                    
                    print(f"- {tabla}: {count} registros ({inv_count} con inventario)")
                
                return tablas
                
        except Exception as e:
            print(f"❌ Error al listar tablas: {str(e)}")
            return []
    
    def cargar_inventarios(self, fecha=None):
        """Carga los datos de inventario desde las tablas staging"""
        fecha_carga = fecha if fecha else self.fecha_proceso
        print(f"\n=== CARGA DE INVENTARIOS ({fecha_carga}) ===")
        
        try:
            # 1. Obtener tablas de staging
            tablas = self.listar_tablas_staging()
            if not tablas:
                print("❌ No se encontraron tablas de staging para procesar")
                return False
            
            # 2. Procesar cada tabla
            total_registros = 0
            
            for tabla in tablas:
                print(f"\nProcesando {tabla}...")
                
                with self.engine.connect() as conn:
                    # Extraer partes del nombre de la tabla para identificar el PDV
                    nombre_tabla = tabla.replace("stg_inventario_", "")
                    codigo_pdv = ''.join(filter(str.isdigit, nombre_tabla))
                    nombre_pdv = ''.join(filter(lambda x: not x.isdigit(), nombre_tabla)).strip()
                    
                    # Intentar encontrar el PDV en la tabla dim_pdv
                    query = f"""
                    SELECT pdv_sk 
                    FROM dim_pdv 
                    WHERE codigo_pdv = '{codigo_pdv}'
                    """
                    result = conn.execute(text(query))
                    row = result.fetchone()
                    
                    if row:
                        pdv_id = row[0]
                    else:
                        # Intentar buscar por nombre
                        query = f"""
                        SELECT pdv_sk 
                        FROM dim_pdv 
                        WHERE nombre_pdv LIKE '%{nombre_pdv}%'
                        LIMIT 1
                        """
                        result = conn.execute(text(query))
                        row = result.fetchone()
                        
                        if row:
                            pdv_id = row[0]
                        else:
                            print(f"⚠️ No se encontró el PDV para {tabla}. Usando PDV por defecto.")
                            pdv_id = 1  # Valor por defecto
                    
                    # Limpiar registros existentes para este PDV y fecha
                    delete_query = f"""
                    DELETE FROM fact_inventarios 
                    WHERE pdv_id = {pdv_id} AND fecha = '{fecha_carga}'
                    """
                    conn.execute(text(delete_query))
                    conn.commit()
                    
                    # Insertar nuevos registros
                    insert_query = f"""
                    INSERT INTO fact_inventarios 
                    (fecha, cantidad_existencias, valor_existencias, costo_promedio, 
                     rotacion_dias, rotacion_semanas, dias_stock, nivel_stock, 
                     fecha_creacion, fecha_actualizacion, pdv_id)
                    SELECT 
                        '{fecha_carga}' as fecha,
                        COALESCE(SUM(inventario_caja), 0) as cantidad_existencias,
                        COALESCE(SUM(costo_total), 0) as valor_existencias,
                        CASE 
                            WHEN SUM(inventario_caja) > 0 THEN SUM(costo_total) / SUM(inventario_caja)
                            ELSE 0 
                        END as costo_promedio,
                        30 as rotacion_dias,
                        30/7 as rotacion_semanas,
                        30 as dias_stock,
                        'NORMAL' as nivel_stock,
                        NOW() as fecha_creacion,
                        NOW() as fecha_actualizacion,
                        {pdv_id} as pdv_id
                    FROM `{tabla}`
                    WHERE inventario_caja > 0
                    """
                    
                    try:
                        conn.execute(text(insert_query))
                        conn.commit()
                        
                        # Verificar registros insertados
                        count_query = f"""
                        SELECT COUNT(*) 
                        FROM fact_inventarios 
                        WHERE pdv_id = {pdv_id} AND fecha = '{fecha_carga}'
                        """
                        count = conn.execute(text(count_query)).scalar()
                        
                        print(f"✅ Registros cargados: {count}")
                        total_registros += count
                        
                    except Exception as e:
                        print(f"❌ Error al insertar datos: {str(e)}")
                        # Continuar con la siguiente tabla
            
            print(f"\nTotal de registros cargados: {total_registros}")
            return total_registros > 0
            
        except Exception as e:
            print(f"❌ Error en la carga de inventarios: {str(e)}")
            return False
    
    def generar_reporte(self, fecha=None):
        """Genera un reporte de inventarios por PDV"""
        fecha_reporte = fecha if fecha else self.fecha_proceso
        print(f"\n=== REPORTE DE INVENTARIOS ({fecha_reporte}) ===")
        
        try:
            with self.engine.connect() as conn:
                query = f"""
                SELECT 
                    p.nombre_pdv,
                    i.cantidad_existencias,
                    i.valor_existencias,
                    i.costo_promedio
                FROM fact_inventarios i
                JOIN dim_pdv p ON i.pdv_id = p.pdv_sk
                WHERE i.fecha = '{fecha_reporte}'
                ORDER BY i.valor_existencias DESC
                """
                
                df = pd.read_sql(query, conn)
                
                if df.empty:
                    print("⚠️ No hay datos para la fecha especificada")
                    return False
                
                print("\nResumen de inventarios por PDV:")
                print("-" * 80)
                print(f"{'PDV':<30} {'Cantidad':<15} {'Valor':<15} {'Costo Prom.':<15}")
                print("-" * 80)
                
                for _, row in df.iterrows():
                    print(f"{row['nombre_pdv']:<30} {row['cantidad_existencias']:<15.2f} {row['valor_existencias']:<15.2f} {row['costo_promedio']:<15.2f}")
                
                print("-" * 80)
                print(f"Total: {df['cantidad_existencias'].sum():<15.2f} {df['valor_existencias'].sum():<15.2f}")
                
                # Guardar reporte en CSV
                archivo_csv = f"reporte_inventarios_{fecha_reporte}.csv"
                df.to_csv(archivo_csv, index=False)
                print(f"\nReporte guardado en {archivo_csv}")
                
                return True
                
        except Exception as e:
            print(f"❌ Error al generar reporte: {str(e)}")
            return False

def menu_principal():
    """Muestra el menú principal del gestor"""
    gestor = GestorFactInventarios()
    
    while True:
        print("\n" + "=" * 50)
        print("GESTOR DE FACT_INVENTARIOS")
        print("=" * 50)
        print("1. Verificar estructura de la tabla")
        print("2. Listar tablas de staging de inventario")
        print("3. Cargar inventarios (fecha actual)")
        print("4. Generar reporte de inventarios")
        print("5. Salir")
        
        opcion = input("\nSeleccione una opción: ")
        
        if opcion == "1":
            gestor.verificar_estructura()
        elif opcion == "2":
            gestor.listar_tablas_staging()
        elif opcion == "3":
            gestor.cargar_inventarios()
        elif opcion == "4":
            gestor.generar_reporte()
        elif opcion == "5":
            print("¡Hasta pronto!")
            break
        else:
            print("Opción no válida. Intente nuevamente.")

def procesar_argumentos():
    """Procesa argumentos de línea de comandos"""
    if len(sys.argv) < 2:
        menu_principal()
        return
    
    comando = sys.argv[1].lower()
    gestor = GestorFactInventarios()
    
    if comando == "verificar":
        gestor.verificar_estructura()
    elif comando == "listar":
        gestor.listar_tablas_staging()
    elif comando == "cargar":
        fecha = sys.argv[2] if len(sys.argv) > 2 else None
        gestor.cargar_inventarios(fecha)
    elif comando == "reporte":
        fecha = sys.argv[2] if len(sys.argv) > 2 else None
        gestor.generar_reporte(fecha)
    elif comando == "completo":
        gestor.verificar_estructura()
        gestor.listar_tablas_staging()
        gestor.cargar_inventarios()
        gestor.generar_reporte()
    else:
        print(f"Comando '{comando}' no reconocido")
        print("Comandos disponibles: verificar, listar, cargar, reporte, completo")

if __name__ == "__main__":
    print(f"Ejecutando desde: {sys.executable}")
    procesar_argumentos()
