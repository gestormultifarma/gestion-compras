# analysis/extractor/extractor_fact_rotacion_all_pdv.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

class ExtractorFactRotacion:
    """
    Extractor para obtener datos de las tablas de rotación existentes (stg_rotacion_*)
    y prepararlos para la tabla fact_rotacion, procesando TODOS los puntos de venta.
    """
    
    def __init__(self, logger=None):
        """
        Inicializa el extractor para fact_rotacion.
        
        Args:
            logger: Instancia de logger para registrar eventos
        """
        self.logger = logger
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
    
    def extraer(self):
        """
        Extrae datos de las tablas stg_rotacion_* existentes para todos los PDVs.
        
        Returns:
            pd.DataFrame: DataFrame con los datos consolidados para fact_rotacion
        """
        try:
            # 1. Obtener todas las tablas de rotación
            tablas_rotacion = self._obtener_tablas_rotacion()
            
            if not tablas_rotacion:
                if self.logger:
                    self.logger.warning("⚠️ No se encontraron tablas de rotación (stg_rotacion_*)")
                return pd.DataFrame()
                
            if self.logger:
                self.logger.info(f"📊 Encontradas {len(tablas_rotacion)} tablas de rotación")
                
            # Agrupar tablas por PDV para mejor seguimiento
            tablas_por_pdv = {}
            for tabla in tablas_rotacion:
                # Formato esperado: stg_rotacion_de_NOMBRE_PDV_CODIGO_NUMERO
                partes = tabla.split('_')
                if len(partes) >= 3:
                    codigo_pdv = partes[-2]
                    if codigo_pdv not in tablas_por_pdv:
                        tablas_por_pdv[codigo_pdv] = []
                    tablas_por_pdv[codigo_pdv].append(tabla)
            
            if self.logger:
                self.logger.info(f"🏪 PDVs encontrados: {len(tablas_por_pdv)}")
                for codigo, tablas in tablas_por_pdv.items():
                    self.logger.info(f"📍 PDV {codigo}: {len(tablas)} tablas")
            
            # 2. Procesar cada tabla y consolidar los resultados
            resultados_consolidados = []
            pdvs_procesados = set()
            
            for tabla in tablas_rotacion:
                try:
                    # Extraer información del PDV desde el nombre de la tabla
                    pdv_info = self._extraer_info_pdv(tabla)
                    if not pdv_info:
                        if self.logger:
                            self.logger.warning(f"⚠️ No se pudo extraer info de PDV para tabla: {tabla}")
                        continue
                        
                    # Registrar PDV procesado
                    pdv_codigo = pdv_info.get('codigo_pdv')
                    pdv_nombre = pdv_info.get('nombre_pdv')
                    pdvs_procesados.add(pdv_codigo)
                    
                    if self.logger:
                        self.logger.info(f"🏪 Procesando PDV: {pdv_nombre} (Código: {pdv_codigo})")
                        
                    # Obtener los datos de la tabla
                    df_tabla = self._obtener_datos_tabla(tabla, pdv_info)
                    
                    if not df_tabla.empty:
                        if self.logger:
                            self.logger.info(f"✅ Obtenidos {len(df_tabla)} registros de {tabla}")
                        resultados_consolidados.append(df_tabla)
                    else:
                        if self.logger:
                            self.logger.warning(f"⚠️ No se obtuvieron datos de tabla: {tabla}")
                        
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"💥 Error al procesar tabla {tabla}: {e}")
            
            # 3. Combinar todos los resultados
            if resultados_consolidados:
                df_final = pd.concat(resultados_consolidados, ignore_index=True)
                
                if self.logger:
                    self.logger.info(f"✅ Extracción completada. Total registros: {len(df_final)}")
                    self.logger.info(f"✅ PDVs procesados: {len(pdvs_procesados)}")
                
                return df_final
            else:
                if self.logger:
                    self.logger.warning("⚠️ No se obtuvieron datos de las tablas de rotación")
                return pd.DataFrame()
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error en extracción de datos de rotación: {e}")
            return pd.DataFrame()
    
    def _obtener_tablas_rotacion(self):
        """Obtiene la lista de tablas stg_rotacion_* disponibles"""
        try:
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg_rotacion_%'
            """)
            
            with self.engine.connect() as connection:
                resultado = connection.execute(query)
                tablas = [row[0] for row in resultado]
            
            return tablas
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al obtener tablas de rotación: {e}")
            return []
    
    def _extraer_info_pdv(self, nombre_tabla):
        """
        Extrae información del PDV desde el nombre de la tabla.
        
        Args:
            nombre_tabla (str): Nombre de la tabla (ej: stg_rotacion_de_bella_suiza_40350_1)
            
        Returns:
            dict: Información del PDV o None si no se pudo extraer
        """
        try:
            # Formato esperado: stg_rotacion_de_NOMBRE_PDV_CODIGO_NUMERO
            partes = nombre_tabla.split('_')
            
            # Extraer el código del PDV (penúltima parte)
            codigo_pdv = partes[-2] if len(partes) >= 3 else None
            
            if not codigo_pdv or not codigo_pdv.isdigit():
                if self.logger:
                    self.logger.warning(f"⚠️ No se pudo extraer código PDV válido de tabla: {nombre_tabla}")
                return None
            
            # Extraer el nombre del PDV (todo entre "de_" y el código)
            indice_de = 0
            for i, parte in enumerate(partes):
                if parte == "de":
                    indice_de = i
                    break
                    
            if indice_de > 0 and indice_de < len(partes) - 2:
                # Obtener partes del nombre del PDV y unirlas
                partes_nombre = partes[indice_de+1:-2]
                nombre_pdv = '_'.join(partes_nombre)
                
                # Buscar el PDV en la dimensión
                pdv_sk = self._buscar_pdv_sk(nombre_pdv, codigo_pdv)
                
                if not pdv_sk:
                    if self.logger:
                        self.logger.warning(f"⚠️ No se encontró pdv_sk para: {nombre_pdv} (código: {codigo_pdv})")
                    # Intentar buscar solo por código PDV como fallback
                    pdv_sk = self._buscar_pdv_sk("", codigo_pdv)
                
                if pdv_sk:
                    return {
                        'codigo_pdv': codigo_pdv,
                        'nombre_pdv': nombre_pdv,
                        'pdv_sk': pdv_sk
                    }
                else:
                    if self.logger:
                        self.logger.error(f"❌ No se pudo encontrar pdv_sk para: {codigo_pdv}")
            
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al extraer info de PDV de tabla {nombre_tabla}: {e}")
            return None
    
    def _buscar_pdv_sk(self, nombre_pdv, codigo_pdv):
        """
        Busca el ID del PDV en la dimensión dim_pdv.
        
        Args:
            nombre_pdv (str): Nombre del PDV
            codigo_pdv (str): Código del PDV
            
        Returns:
            int: ID del PDV o None si no se encontró
        """
        try:
            # Priorizar búsqueda por código_pdv (más preciso)
            query = text("""
                SELECT pdv_sk
                FROM dim_pdv
                WHERE codigo_pdv = :codigo_pdv
            """)
            
            with self.engine.connect() as connection:
                resultado = connection.execute(query, {"codigo_pdv": codigo_pdv}).fetchone()
                
            if resultado:
                return resultado[0]
                
            # Si no se encuentra por código, buscar por nombre (menos preciso)
            if nombre_pdv:
                query = text("""
                    SELECT pdv_sk
                    FROM dim_pdv
                    WHERE LOWER(nombre_pdv) LIKE LOWER(:nombre_pdv)
                """)
                
                with self.engine.connect() as connection:
                    resultado = connection.execute(query, {
                        "nombre_pdv": f"%{nombre_pdv.replace('_', ' ')}%"
                    }).fetchone()
                    
                return resultado[0] if resultado else None
                
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al buscar PDV {nombre_pdv} ({codigo_pdv}): {e}")
            return None
    
    def _obtener_datos_tabla(self, nombre_tabla, pdv_info):
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
                'unidades': ['unidades', 'cantidad', 'unidades_vendidas', 'venta_unidades'],
                'costo': ['costo', 'costo_total', 'costo_unidad', 'costo_unitario'],
                'margen': ['margen', 'margen_bruto', 'margen_porcentaje', 'porcentaje_margen'],
                'stock': ['stock', 'stock_actual', 'existencia', 'inventario_final']
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
                
                if self.logger:
                    self.logger.info(f"✅ Procesados {len(df)} registros del PDV {pdv_info.get('codigo_pdv')}")
                
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
            if self.logger:
                self.logger.error(f"💥 Error al obtener datos de tabla {nombre_tabla}: {e}")
            return pd.DataFrame()
