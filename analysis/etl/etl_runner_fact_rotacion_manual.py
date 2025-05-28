# analysis/etl/etl_runner_fact_rotacion_manual.py

import os
import pandas as pd
import warnings
from datetime import datetime
from sqlalchemy import create_engine, text
from analysis.transformer.transformer_fact_rotacion import TransformadorFactRotacion
from analysis.loader.loader_fact_rotacion import LoaderFactRotacion
from utils.db_connection import get_mysql_url
from utils.logger_etl import LoggerETL

# Suprimir advertencias de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

class FactRotacionETLRunnerManual:
    """
    Runner ETL para la tabla fact_rotacion que procesa manualmente
    todos los puntos de venta disponibles.
    """
    
    def __init__(self):
        """Inicializa el runner con sus componentes ETL y el logger"""
        self.logger = LoggerETL("ETL Fact Rotacion Manual (Todos los PDVs)")
        self.loader = LoaderFactRotacion(logger=self.logger)
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
        
    def run(self):
        """Ejecuta el proceso ETL completo para fact_rotacion procesando todos los PDVs"""
        try:
            self.logger.info("🚀 Iniciando ETL para fact_rotacion (TODOS los PDVs)")
            
            # 1. Obtener todos los PDVs
            pdvs = self._obtener_todos_pdvs()
            if not pdvs:
                self.logger.error("❌ No se encontraron PDVs en la dimensión")
                return False
                
            self.logger.info(f"🏪 Se procesarán {len(pdvs)} puntos de venta")
            
            # 2. Procesar cada PDV por separado
            resultados_globales = []
            
            for pdv in pdvs:
                pdv_sk = pdv['pdv_sk']
                codigo_pdv = pdv['codigo_pdv']
                nombre_pdv = pdv['nombre_pdv']
                
                self.logger.info(f"\n🏪 Procesando PDV: {nombre_pdv} (Código: {codigo_pdv})")
                
                # Obtener tablas de rotación para este PDV
                tablas_rotacion = self._obtener_tablas_rotacion_pdv(codigo_pdv)
                if not tablas_rotacion:
                    self.logger.warning(f"⚠️ No se encontraron tablas de rotación para PDV {codigo_pdv}")
                    continue
                    
                self.logger.info(f"📊 Encontradas {len(tablas_rotacion)} tablas para PDV {codigo_pdv}")
                
                # Procesar cada tabla
                dfs_pdv = []
                for tabla in tablas_rotacion:
                    self.logger.info(f"📋 Procesando tabla: {tabla}")
                    
                    # Extraer datos de la tabla
                    df_tabla = self._extraer_datos_tabla(tabla, pdv)
                    
                    if not df_tabla.empty:
                        self.logger.info(f"✅ Extraídos {len(df_tabla)} registros de {tabla}")
                        dfs_pdv.append(df_tabla)
                    else:
                        self.logger.warning(f"⚠️ No se obtuvieron datos de la tabla {tabla}")
                
                # Consolidar datos de este PDV
                if dfs_pdv:
                    df_consolidado_pdv = pd.concat(dfs_pdv, ignore_index=True)
                    
                    # Eliminar duplicados (mismo producto en diferentes tablas)
                    df_consolidado_pdv = df_consolidado_pdv.drop_duplicates(subset=['codigo_producto'])
                    
                    self.logger.info(f"✅ Total registros para PDV {codigo_pdv}: {len(df_consolidado_pdv)}")
                    resultados_globales.append(df_consolidado_pdv)
                else:
                    self.logger.warning(f"⚠️ No se obtuvieron datos para PDV {codigo_pdv}")
            
            # 3. Consolidar todos los resultados
            if not resultados_globales:
                self.logger.error("❌ No se obtuvieron datos para ningún PDV")
                return False
                
            df_final = pd.concat(resultados_globales, ignore_index=True)
            self.logger.info(f"📊 Total registros consolidados: {len(df_final)}")
            
            # 4. Transformar
            self.logger.info("🔄 Iniciando transformación de datos")
            transformador = TransformadorFactRotacion(df_final, logger=self.logger)
            df_transformado = transformador.transformar()
            
            if df_transformado.empty:
                self.logger.warning("⚠️ No hay datos después de la transformación")
                return False
                
            # 5. Guardar histórico como CSV (opcional)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            nombre_archivo_csv = f"fact_rotacion_all_pdv_{timestamp}.csv"
            ruta_historico = os.path.join("E:/desarrollo/gestionCompras/historico/ETL_Fact_Rotacion")
            os.makedirs(ruta_historico, exist_ok=True)
            ruta_completa = os.path.join(ruta_historico, nombre_archivo_csv)
            df_transformado.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 Backup CSV guardado en: {ruta_completa}")
            
            # 6. Cargar
            self.logger.info("📤 Iniciando carga de datos")
            resultado_carga = self.loader.cargar_dataframe(df_transformado)
            
            if resultado_carga:
                self.logger.info("✅ Proceso ETL completado con éxito")
            else:
                self.logger.error("❌ Error en la carga de datos")
                
            return resultado_carga
            
        except Exception as e:
            self.logger.error(f"💥 Error en el proceso ETL: {e}")
            return False
            
    def _obtener_todos_pdvs(self):
        """Obtiene todos los PDVs desde la dimensión dim_pdv"""
        try:
            query = text("""
                SELECT pdv_sk, codigo_pdv, nombre_pdv
                FROM dim_pdv
                WHERE estado_pdv = 'activo'
                ORDER BY pdv_sk
            """)
            
            with self.engine.connect() as connection:
                result = connection.execute(query)
                pdvs = [
                    {
                        'pdv_sk': row[0],
                        'codigo_pdv': str(row[1]),
                        'nombre_pdv': row[2]
                    }
                    for row in result
                ]
                
            return pdvs
            
        except Exception as e:
            self.logger.error(f"💥 Error al obtener PDVs: {e}")
            return []
            
    def _obtener_tablas_rotacion_pdv(self, codigo_pdv):
        """Obtiene las tablas de rotación para un PDV específico"""
        try:
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg_rotacion_%'
                AND table_name LIKE :pattern
            """)
            
            with self.engine.connect() as connection:
                resultado = connection.execute(query, {"pattern": f"%_{codigo_pdv}_%"})
                tablas = [row[0] for row in resultado]
            
            return tablas
            
        except Exception as e:
            self.logger.error(f"💥 Error al obtener tablas para PDV {codigo_pdv}: {e}")
            return []
            
    def _extraer_datos_tabla(self, nombre_tabla, pdv_info):
        """
        Obtiene y procesa los datos de una tabla de rotación.
        
        Args:
            nombre_tabla (str): Nombre de la tabla
            pdv_info (dict): Información del PDV
            
        Returns:
            pd.DataFrame: DataFrame con los datos procesados
        """
        try:
            # Primero averiguar las columnas de la tabla
            query_columnas = text(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'gestion_compras'
                AND TABLE_NAME = '{nombre_tabla}'
            """)
            
            with self.engine.connect() as connection:
                columnas = [row[0] for row in connection.execute(query_columnas)]
            
            # Mapeo de columnas estándar a nombres en la tabla
            mapeos_posibles = {
                'codigo': ['codigo', 'codigo_producto', 'cod_producto', 'id_producto'],
                'producto': ['producto', 'nombre_producto', 'descripcion', 'nombre'],
                'ventas': ['ventas', 'venta', 'venta_total', 'venta_pesos'],
                'unidades': ['unidades', 'cantidad', 'unidades_vendidas', 'venta_unidad'],
                'costo': ['costo', 'costo_total', 'costo_unidad', 'costo_unitario'],
                'margen': ['margen', 'margen_bruto', 'margen_porcentaje', 'porcentaje_margen'],
                'stock': ['stock', 'stock_actual', 'existencia', 'inventario_unidad']
            }
            
            # Encontrar las columnas disponibles en la tabla
            columnas_a_usar = {}
            for campo_estandar, posibles_nombres in mapeos_posibles.items():
                for nombre in posibles_nombres:
                    if nombre in columnas:
                        columnas_a_usar[campo_estandar] = nombre
                        break
            
            # Construir la consulta según las columnas disponibles
            campos_select = []
            
            if 'codigo' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['codigo']} as codigo_producto")
            else:
                campos_select.append("'desconocido' as codigo_producto")
                
            if 'producto' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['producto']} as nombre_producto")
            else:
                campos_select.append("'' as nombre_producto")
                
            if 'ventas' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['ventas']} as venta_total")
            else:
                campos_select.append("0 as venta_total")
                
            if 'unidades' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['unidades']} as venta_unidades")
            else:
                campos_select.append("0 as venta_unidades")
                
            if 'costo' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['costo']} as costo_total")
            else:
                campos_select.append("0 as costo_total")
                
            if 'margen' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['margen']} as margen_porcentaje")
            else:
                campos_select.append("0 as margen_porcentaje")
                
            if 'stock' in columnas_a_usar:
                campos_select.append(f"{columnas_a_usar['stock']} as inventario_unidades_final")
            else:
                campos_select.append("0 as inventario_unidades_final")
            
            # Consultar todos los registros sin límite
            query = text(f"""
                SELECT 
                    {', '.join(campos_select)},
                    NOW() as fecha
                FROM `{nombre_tabla}`
            """)
            
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            if not df.empty:
                # Agregar información del PDV
                df['codigo_pdv'] = pdv_info.get('codigo_pdv')
                df['pdv_sk'] = pdv_info.get('pdv_sk')
                
                # Calcular campos adicionales si no están presentes
                if 'margen_porcentaje' not in df.columns or df['margen_porcentaje'].isna().all():
                    df['margen_porcentaje'] = df.apply(
                        lambda row: ((row['venta_total'] - row['costo_total']) / row['venta_total'] * 100) 
                                    if row['venta_total'] > 0 else 0,
                        axis=1
                    )
                
                # Calcular días de inventario (simplificado)
                df['dias_inventario'] = df.apply(
                    lambda row: (row['inventario_unidades_final'] * 30 / row['venta_unidades']) 
                                if row['venta_unidades'] > 0 else 90,  # 90 días si no hay ventas
                    axis=1
                )
                df['dias_inventario'] = df['dias_inventario'].clip(0, 90)  # Limitar a 90 días máximo
                
                # Calcular rotación mensual
                df['rotacion_mes'] = df.apply(
                    lambda row: (30 / row['dias_inventario']) if row['dias_inventario'] > 0 else 0,
                    axis=1
                )
                
                # Campos iniciales para fact_rotacion
                df['fecha_sk'] = None  # Se llenará en el transformer
                df['producto_sk'] = None  # Se llenará en el transformer
                df['inventario_unidades_inicial'] = 0  # No tenemos datos históricos
                
                # Para los campos faltantes, crear columnas con valores predeterminados
                if 'venta_cajas' not in df.columns:
                    df['venta_cajas'] = 0
                if 'venta_blisters' not in df.columns:
                    df['venta_blisters'] = 0
                if 'costo_unitario' not in df.columns:
                    df['costo_unitario'] = df.apply(
                        lambda row: row['costo_total'] / row['venta_unidades'] if row['venta_unidades'] > 0 else 0,
                        axis=1
                    )
                if 'precio_venta_unitario' not in df.columns:
                    df['precio_venta_unitario'] = df.apply(
                        lambda row: row['venta_total'] / row['venta_unidades'] if row['venta_unidades'] > 0 else 0,
                        axis=1
                    )
                if 'margen_bruto' not in df.columns:
                    df['margen_bruto'] = df['venta_total'] - df['costo_total']
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"💥 Error al obtener datos de tabla {nombre_tabla}: {e}")
            return pd.DataFrame()


if __name__ == '__main__':
    # Para ejecutar desde línea de comandos: python -m analysis.etl.etl_runner_fact_rotacion_manual
    runner = FactRotacionETLRunnerManual()
    runner.run()
