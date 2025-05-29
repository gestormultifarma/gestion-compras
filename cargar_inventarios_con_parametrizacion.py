"""
Script para cargar la tabla fact_inventarios considerando la parametrización específica
de cada producto en cada punto de venta según su tabla maestra correspondiente.

Esto garantiza que el stock se calcule correctamente según la configuración
de contenido_caja específica de cada PDV.
"""

import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

class CargadorInventariosParametrizado:
    """
    Clase para cargar inventarios considerando la parametrización específica
    de cada producto en cada punto de venta.
    """
    
    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
        self.fecha_proceso = datetime.now().date()
    
    def obtener_tablas_staging(self):
        """Obtiene las tablas de staging de inventario y sus correspondientes maestras"""
        try:
            print("\n=== IDENTIFICANDO TABLAS DE STAGING ===")
            
            with self.engine.connect() as conn:
                # Obtener tablas de inventario
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg\\_inventario\\_%'
                """
                result = conn.execute(text(query))
                tablas_inventario = [row[0] for row in result]
                
                # Obtener tablas maestras
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg\\_maestra\\_pdv\\_%'
                """
                result = conn.execute(text(query))
                tablas_maestra = [row[0] for row in result]
                
                # Emparejar tablas de inventario con sus maestras correspondientes
                pdvs = []
                
                for tabla_inv in tablas_inventario:
                    # Extraer nombre del PDV de la tabla de inventario
                    nombre_pdv = tabla_inv.replace("stg_inventario_", "")
                    
                    # Intentar encontrar la tabla maestra correspondiente
                    tabla_maestra = None
                    nombre_maestra = None
                    
                    for maestra in tablas_maestra:
                        nombre_maestra = maestra.replace("stg_maestra_pdv_", "")
                        
                        # Comprobar si el nombre de la maestra está en el nombre del inventario
                        # o viceversa (manejo de diferentes formatos de nombres)
                        if (nombre_maestra.lower() in nombre_pdv.lower() or 
                            nombre_pdv.lower() in nombre_maestra.lower() or
                            self._nombres_similares(nombre_pdv, nombre_maestra)):
                            tabla_maestra = maestra
                            break
                    
                    # Si no encontramos correspondencia exacta, buscar por el código numérico
                    if not tabla_maestra:
                        codigo_pdv = ''.join(filter(str.isdigit, nombre_pdv))
                        if codigo_pdv:
                            for maestra in tablas_maestra:
                                if codigo_pdv in maestra:
                                    tabla_maestra = maestra
                                    nombre_maestra = maestra.replace("stg_maestra_pdv_", "")
                                    break
                    
                    # Agregar a la lista de PDVs
                    pdvs.append({
                        'nombre_pdv': nombre_pdv,
                        'tabla_inventario': tabla_inv,
                        'tabla_maestra': tabla_maestra,
                        'nombre_maestra': nombre_maestra
                    })
                
                print(f"Se encontraron {len(tablas_inventario)} tablas de inventario y {len(tablas_maestra)} tablas maestras")
                print("\nCorrespondencias identificadas:")
                
                for pdv in pdvs:
                    status = "✅" if pdv['tabla_maestra'] else "❌"
                    print(f"{status} Inventario: {pdv['tabla_inventario']}")
                    print(f"   Maestra: {pdv['tabla_maestra'] if pdv['tabla_maestra'] else 'No encontrada'}")
                
                return pdvs
                
        except Exception as e:
            print(f"Error al obtener tablas: {str(e)}")
            return []
    
    def _nombres_similares(self, nombre1, nombre2):
        """Verifica si dos nombres de PDV son similares aunque no sean idénticos"""
        # Normalizar nombres
        n1 = nombre1.lower().replace("_", " ").replace("-", " ")
        n2 = nombre2.lower().replace("_", " ").replace("-", " ")
        
        # Manejar casos especiales
        if "cosmocentro" in n1 and "cosmocentro" in n2:
            # Extraer número de Cosmocentro si existe
            num1 = ''.join(filter(str.isdigit, n1))
            num2 = ''.join(filter(str.isdigit, n2))
            return num1 == num2
        
        # Verificar si uno contiene al otro
        return n1 in n2 or n2 in n1
    
    def procesar_pdv(self, info_pdv):
        """
        Procesa un punto de venta, cargando sus datos de inventario
        considerando la parametrización de su tabla maestra.
        """
        try:
            print(f"\n=== PROCESANDO PDV: {info_pdv['nombre_pdv']} ===")
            
            # Verificar si tenemos tabla maestra
            if not info_pdv['tabla_maestra']:
                print("⚠️ No se encontró tabla maestra para este PDV. Usando valores predeterminados.")
                return self._procesar_sin_maestra(info_pdv)
            
            with self.engine.connect() as conn:
                # 1. Identificar el PDV en la tabla dim_pdv
                nombre_busqueda = info_pdv['nombre_pdv']
                codigo_pdv = ''.join(filter(str.isdigit, nombre_busqueda))
                
                query_pdv = f"""
                SELECT pdv_sk, codigo_pdv, nombre_pdv 
                FROM dim_pdv 
                WHERE codigo_pdv = '{codigo_pdv}'
                """
                result = conn.execute(text(query_pdv))
                row = result.fetchone()
                
                if not row:
                    # Intentar buscar por nombre
                    nombre_busqueda = ''.join(filter(lambda x: not x.isdigit(), info_pdv['nombre_pdv'])).strip()
                    query_pdv = f"""
                    SELECT pdv_sk, codigo_pdv, nombre_pdv 
                    FROM dim_pdv 
                    WHERE nombre_pdv LIKE '%{nombre_busqueda}%'
                    LIMIT 1
                    """
                    result = conn.execute(text(query_pdv))
                    row = result.fetchone()
                
                if row:
                    pdv_id = row[0]
                    codigo_pdv = row[1]
                    nombre_pdv_db = row[2]
                else:
                    print(f"⚠️ No se encontró el PDV en dim_pdv. Usando ID por defecto.")
                    pdv_id = 1
                    nombre_pdv_db = info_pdv['nombre_pdv']
                
                print(f"PDV identificado: {nombre_pdv_db} (ID: {pdv_id})")
                
                # 2. Crear tabla temporal para cruzar inventario con maestra
                temp_table = f"temp_inv_param_{pdv_id}"
                
                # Eliminar tabla temporal si existe
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                
                # Crear tabla temporal
                create_temp_query = f"""
                CREATE TEMPORARY TABLE {temp_table} AS
                SELECT 
                    i.codigo as codigo_producto,
                    i.nombre as nombre_producto,
                    i.inventario_caja,
                    i.costo_caja,
                    i.costo_total,
                    COALESCE(m.contenido_caja, 1) as contenido_caja_param
                FROM `{info_pdv['tabla_inventario']}` i
                LEFT JOIN `{info_pdv['tabla_maestra']}` m ON i.codigo = m.codigo
                WHERE i.inventario_caja > 0
                """
                
                conn.execute(text(create_temp_query))
                
                # Verificar datos en tabla temporal
                count_query = f"SELECT COUNT(*) FROM {temp_table}"
                count = conn.execute(text(count_query)).scalar()
                
                if count == 0:
                    print(f"⚠️ No se encontraron productos con inventario para este PDV")
                    return 0
                
                print(f"Se encontraron {count} productos con inventario")
                
                # 3. Obtener estadísticas de parametrización
                stats_query = f"""
                SELECT 
                    MIN(contenido_caja_param) as min_contenido,
                    MAX(contenido_caja_param) as max_contenido,
                    AVG(contenido_caja_param) as avg_contenido,
                    COUNT(CASE WHEN contenido_caja_param <> 1 THEN 1 END) as count_parametrizados
                FROM {temp_table}
                """
                
                stats = conn.execute(text(stats_query)).fetchone()
                
                print(f"Estadísticas de parametrización:")
                print(f"- Contenido mínimo: {stats[0]}")
                print(f"- Contenido máximo: {stats[1]}")
                print(f"- Contenido promedio: {stats[2]:.2f}")
                print(f"- Productos con parametrización específica: {stats[3]} de {count} ({stats[3]/count*100:.1f}%)")
                
                # 4. Limpiar registros existentes para este PDV y fecha
                delete_query = f"""
                DELETE FROM fact_inventarios 
                WHERE pdv_id = {pdv_id} AND fecha = '{self.fecha_proceso}'
                """
                conn.execute(text(delete_query))
                
                # 5. Insertar registros considerando la parametrización
                insert_query = f"""
                INSERT INTO fact_inventarios 
                (fecha, cantidad_existencias, valor_existencias, costo_promedio,
                 rotacion_dias, rotacion_semanas, dias_stock, nivel_stock,
                 fecha_creacion, fecha_actualizacion, pdv_id)
                SELECT 
                    '{self.fecha_proceso}' as fecha,
                    SUM(inventario_caja * contenido_caja_param) as cantidad_existencias,
                    SUM(costo_total) as valor_existencias,
                    CASE 
                        WHEN SUM(inventario_caja * contenido_caja_param) > 0 
                        THEN SUM(costo_total) / SUM(inventario_caja * contenido_caja_param) 
                        ELSE 0 
                    END as costo_promedio,
                    30 as rotacion_dias,
                    30/7 as rotacion_semanas,
                    30 as dias_stock,
                    'NORMAL' as nivel_stock,
                    NOW() as fecha_creacion,
                    NOW() as fecha_actualizacion,
                    {pdv_id} as pdv_id
                FROM {temp_table}
                """
                
                conn.execute(text(insert_query))
                
                # 6. Verificar registros insertados
                count_query = f"""
                SELECT COUNT(*) FROM fact_inventarios 
                WHERE pdv_id = {pdv_id} AND fecha = '{self.fecha_proceso}'
                """
                count_inserted = conn.execute(text(count_query)).scalar()
                
                print(f"✅ Se cargaron {count_inserted} registros en fact_inventarios")
                
                # 7. Limpiar tabla temporal
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                
                return count_inserted
                
        except Exception as e:
            import traceback
            print(f"❌ Error al procesar PDV {info_pdv['nombre_pdv']}: {str(e)}")
            traceback.print_exc()
            return 0
    
    def _procesar_sin_maestra(self, info_pdv):
        """Procesa un PDV sin tabla maestra disponible"""
        try:
            print(f"Procesando {info_pdv['nombre_pdv']} sin tabla maestra...")
            
            with self.engine.connect() as conn:
                # Identificar PDV
                nombre_busqueda = info_pdv['nombre_pdv']
                codigo_pdv = ''.join(filter(str.isdigit, nombre_busqueda))
                
                query_pdv = f"""
                SELECT pdv_sk
                FROM dim_pdv 
                WHERE codigo_pdv = '{codigo_pdv}'
                """
                result = conn.execute(text(query_pdv))
                row = result.fetchone()
                
                if row:
                    pdv_id = row[0]
                else:
                    print(f"⚠️ No se encontró el PDV en dim_pdv. Usando ID por defecto.")
                    pdv_id = 1
                
                # Limpiar registros existentes
                delete_query = f"""
                DELETE FROM fact_inventarios 
                WHERE pdv_id = {pdv_id} AND fecha = '{self.fecha_proceso}'
                """
                conn.execute(text(delete_query))
                
                # Insertar datos (usando contenido_caja directamente)
                insert_query = f"""
                INSERT INTO fact_inventarios 
                (fecha, cantidad_existencias, valor_existencias, costo_promedio,
                 rotacion_dias, rotacion_semanas, dias_stock, nivel_stock,
                 fecha_creacion, fecha_actualizacion, pdv_id)
                SELECT 
                    '{self.fecha_proceso}' as fecha,
                    SUM(inventario_caja) as cantidad_existencias,
                    SUM(costo_total) as valor_existencias,
                    CASE 
                        WHEN SUM(inventario_caja) > 0 
                        THEN SUM(costo_total) / SUM(inventario_caja) 
                        ELSE 0 
                    END as costo_promedio,
                    30 as rotacion_dias,
                    30/7 as rotacion_semanas,
                    30 as dias_stock,
                    'NORMAL' as nivel_stock,
                    NOW() as fecha_creacion,
                    NOW() as fecha_actualizacion,
                    {pdv_id} as pdv_id
                FROM `{info_pdv['tabla_inventario']}`
                WHERE inventario_caja > 0
                """
                
                conn.execute(text(insert_query))
                
                # Verificar registros insertados
                count_query = f"""
                SELECT COUNT(*) FROM fact_inventarios 
                WHERE pdv_id = {pdv_id} AND fecha = '{self.fecha_proceso}'
                """
                count = conn.execute(text(count_query)).scalar()
                
                print(f"✅ Se cargaron {count} registros en fact_inventarios (sin parametrización específica)")
                
                return count
                
        except Exception as e:
            print(f"❌ Error al procesar PDV sin maestra: {str(e)}")
            return 0
    
    def cargar_inventarios(self):
        """Ejecuta el proceso completo de carga de inventarios parametrizados"""
        try:
            print("\n" + "=" * 70)
            print("CARGA DE INVENTARIOS CON PARAMETRIZACIÓN ESPECÍFICA POR PDV")
            print("=" * 70)
            print(f"Fecha de proceso: {self.fecha_proceso}")
            
            # 1. Obtener tablas de staging y sus correspondientes maestras
            pdvs = self.obtener_tablas_staging()
            
            if not pdvs:
                print("❌ No se encontraron tablas para procesar")
                return False
            
            # 2. Procesar cada PDV
            total_registros = 0
            
            for pdv in pdvs:
                registros = self.procesar_pdv(pdv)
                total_registros += registros
            
            # 3. Mostrar resumen
            print("\n" + "=" * 70)
            print("RESUMEN DE CARGA")
            print(f"PDVs procesados: {len(pdvs)}")
            print(f"Total de registros cargados: {total_registros}")
            print("=" * 70)
            
            return total_registros > 0
            
        except Exception as e:
            import traceback
            print(f"❌ Error general: {str(e)}")
            traceback.print_exc()
            return False
    
    def generar_reporte(self):
        """Genera un reporte de inventarios para la fecha de proceso"""
        try:
            print("\n" + "=" * 70)
            print(f"REPORTE DE INVENTARIOS ({self.fecha_proceso})")
            print("=" * 70)
            
            with self.engine.connect() as conn:
                query = f"""
                SELECT 
                    p.nombre_pdv,
                    i.cantidad_existencias,
                    i.valor_existencias,
                    i.costo_promedio
                FROM fact_inventarios i
                JOIN dim_pdv p ON i.pdv_id = p.pdv_sk
                WHERE i.fecha = '{self.fecha_proceso}'
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
                archivo_csv = f"reporte_inventarios_param_{self.fecha_proceso}.csv"
                df.to_csv(archivo_csv, index=False)
                print(f"\nReporte guardado en {archivo_csv}")
                
                return True
                
        except Exception as e:
            print(f"❌ Error al generar reporte: {str(e)}")
            return False

if __name__ == "__main__":
    print(f"Usando Python en: {sys.executable}")
    
    cargador = CargadorInventariosParametrizado()
    resultado = cargador.cargar_inventarios()
    
    if resultado:
        print("\n✅ Proceso de carga completado exitosamente")
        cargador.generar_reporte()
    else:
        print("\n❌ El proceso de carga no pudo completarse correctamente")
