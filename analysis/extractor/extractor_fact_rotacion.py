# analysis/extractor/extractor_fact_rotacion.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

class ExtractorFactRotacion:
    """
    Extractor para obtener datos consolidados de ventas e inventario
    y calcular métricas de rotación para fact_rotacion.
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
        Extrae datos de ventas e inventario y los consolida para calcular rotación.
        Combina datos de múltiples fuentes:
        - Tablas staging de ventas
        - Tablas staging de inventario
        - Dimensiones de productos y puntos de venta
        
        Returns:
            pd.DataFrame: DataFrame con los datos consolidados para fact_rotacion
        """
        try:
            # Lista para almacenar los resultados de todas las consultas
            resultados_consolidados = []
            
            # 1. Obtener lista de productos activos
            productos = self._obtener_productos_activos()
            if not productos or productos.empty:
                if self.logger:
                    self.logger.warning("⚠️ No se encontraron productos activos")
                return pd.DataFrame()
            
            # 2. Obtener lista de puntos de venta activos
            pdvs = self._obtener_pdvs_activos()
            if not pdvs or pdvs.empty:
                if self.logger:
                    self.logger.warning("⚠️ No se encontraron PDVs activos")
                return pd.DataFrame()
            
            # 3. Para cada PDV, obtener datos de ventas e inventario
            for _, pdv_row in pdvs.iterrows():
                pdv_sk = pdv_row['pdv_sk']
                codigo_pdv = pdv_row['codigo_pdv']
                
                # Obtener ventas para este PDV
                ventas_pdv = self._obtener_ventas_pdv(codigo_pdv)
                
                # Obtener inventario para este PDV
                inventario_pdv = self._obtener_inventario_pdv(codigo_pdv)
                
                if ventas_pdv.empty or inventario_pdv.empty:
                    continue
                
                # 4. Cruzar ventas e inventario para obtener la rotación
                df_rotacion = self._calcular_rotacion(ventas_pdv, inventario_pdv, pdv_sk, codigo_pdv)
                
                if not df_rotacion.empty:
                    resultados_consolidados.append(df_rotacion)
            
            # 5. Consolidar todos los resultados
            if resultados_consolidados:
                df_final = pd.concat(resultados_consolidados, ignore_index=True)
                
                if self.logger:
                    self.logger.info(f"✅ Extracción completada. Total registros: {len(df_final)}")
                
                return df_final
            else:
                if self.logger:
                    self.logger.warning("⚠️ No se generaron datos de rotación")
                return pd.DataFrame()
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error en extracción de datos de rotación: {e}")
            return pd.DataFrame()
    
    def _obtener_productos_activos(self):
        """Obtiene lista de productos activos desde dim_producto"""
        try:
            query = text("""
                SELECT 
                    producto_sk, 
                    codigo, 
                    nombre
                FROM dim_producto
                WHERE activo = 1
            """)
            
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
                
            if self.logger:
                self.logger.info(f"📋 Productos activos encontrados: {len(df)}")
                
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al obtener productos activos: {e}")
            return pd.DataFrame()
    
    def _obtener_pdvs_activos(self):
        """Obtiene lista de PDVs activos desde dim_pdv"""
        try:
            query = text("""
                SELECT 
                    pdv_sk, 
                    codigo_pdv, 
                    nombre_pdv
                FROM dim_pdv
                WHERE estado_pdv = 'activo'
            """)
            
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
                
            if self.logger:
                self.logger.info(f"🏪 PDVs activos encontrados: {len(df)}")
                
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al obtener PDVs activos: {e}")
            return pd.DataFrame()
    
    def _obtener_ventas_pdv(self, codigo_pdv):
        """
        Obtiene datos de ventas para un PDV específico desde tablas staging.
        
        Args:
            codigo_pdv (str): Código del PDV
            
        Returns:
            pd.DataFrame: DataFrame con los datos de ventas
        """
        try:
            # Buscar la tabla de ventas correspondiente a este PDV
            query_tabla = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg_ventas_%'
                AND table_name LIKE :patron
            """)
            
            with self.engine.connect() as connection:
                resultado = connection.execute(query_tabla, {"patron": f"%{codigo_pdv}%"}).fetchone()
            
            if not resultado:
                if self.logger:
                    self.logger.warning(f"⚠️ No se encontró tabla de ventas para PDV: {codigo_pdv}")
                return pd.DataFrame()
            
            nombre_tabla = resultado[0]
            
            # Obtener datos de ventas
            query_ventas = text(f"""
                SELECT 
                    codigo as codigo_producto,
                    SUM(venta_unidades) as venta_unidades,
                    SUM(venta_cajas) as venta_cajas,
                    SUM(venta_blisters) as venta_blisters,
                    AVG(costo_unitario) as costo_unitario,
                    AVG(precio_venta) as precio_venta_unitario,
                    SUM(costo_total) as costo_total,
                    SUM(venta_total) as venta_total,
                    CURDATE() as fecha
                FROM `{nombre_tabla}`
                GROUP BY codigo_producto
            """)
            
            with self.engine.connect() as connection:
                df = pd.read_sql(query_ventas, connection)
            
            if self.logger:
                self.logger.info(f"💰 Datos de ventas obtenidos para PDV {codigo_pdv}: {len(df)} productos")
                
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al obtener ventas para PDV {codigo_pdv}: {e}")
            return pd.DataFrame()
    
    def _obtener_inventario_pdv(self, codigo_pdv):
        """
        Obtiene datos de inventario para un PDV específico desde tablas staging.
        
        Args:
            codigo_pdv (str): Código del PDV
            
        Returns:
            pd.DataFrame: DataFrame con los datos de inventario
        """
        try:
            # Buscar la tabla de inventario correspondiente a este PDV
            query_tabla = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gestion_compras' 
                AND table_name LIKE 'stg_inventario_%'
                AND table_name LIKE :patron
            """)
            
            with self.engine.connect() as connection:
                resultado = connection.execute(query_tabla, {"patron": f"%{codigo_pdv}%"}).fetchone()
            
            if not resultado:
                if self.logger:
                    self.logger.warning(f"⚠️ No se encontró tabla de inventario para PDV: {codigo_pdv}")
                return pd.DataFrame()
            
            nombre_tabla = resultado[0]
            
            # Obtener datos de inventario
            query_inventario = text(f"""
                SELECT 
                    codigo as codigo_producto,
                    inventario_unidad as inventario_unidades_final,
                    costo_unidad as costo_unitario
                FROM `{nombre_tabla}`
            """)
            
            with self.engine.connect() as connection:
                df = pd.read_sql(query_inventario, connection)
            
            if self.logger:
                self.logger.info(f"📦 Datos de inventario obtenidos para PDV {codigo_pdv}: {len(df)} productos")
                
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al obtener inventario para PDV {codigo_pdv}: {e}")
            return pd.DataFrame()
    
    def _calcular_rotacion(self, ventas_df, inventario_df, pdv_sk, codigo_pdv):
        """
        Calcula métricas de rotación combinando datos de ventas e inventario.
        
        Args:
            ventas_df (pd.DataFrame): DataFrame con datos de ventas
            inventario_df (pd.DataFrame): DataFrame con datos de inventario
            pdv_sk (int): ID del PDV
            codigo_pdv (str): Código del PDV
            
        Returns:
            pd.DataFrame: DataFrame con los cálculos de rotación
        """
        try:
            # Unir datos de ventas e inventario por código de producto
            df_merged = pd.merge(
                ventas_df, 
                inventario_df,
                on='codigo_producto', 
                how='outer', 
                suffixes=('_venta', '_inv')
            )
            
            # Llenar valores nulos
            df_merged['venta_unidades'] = df_merged['venta_unidades'].fillna(0)
            df_merged['venta_total'] = df_merged['venta_total'].fillna(0)
            df_merged['costo_total'] = df_merged['costo_total'].fillna(0)
            df_merged['inventario_unidades_final'] = df_merged['inventario_unidades_final'].fillna(0)
            
            # Usar el costo unitario del inventario si está disponible, sino usar el de ventas
            df_merged['costo_unitario'] = df_merged['costo_unitario_inv'].fillna(df_merged['costo_unitario_venta'])
            
            # Calcular margen bruto y porcentaje
            df_merged['margen_bruto'] = df_merged['venta_total'] - df_merged['costo_total']
            df_merged['margen_porcentaje'] = df_merged.apply(
                lambda row: (row['margen_bruto'] / row['venta_total'] * 100) if row['venta_total'] > 0 else 0, 
                axis=1
            )
            
            # Calcular días de inventario y rotación (asumiendo 30 días por mes)
            df_merged['dias_inventario'] = df_merged.apply(
                lambda row: (row['inventario_unidades_final'] * 30 / row['venta_unidades']) 
                            if row['venta_unidades'] > 0 else 0,
                axis=1
            )
            
            # Limitar días de inventario a un máximo razonable (90 días)
            df_merged['dias_inventario'] = df_merged['dias_inventario'].clip(upper=90)
            
            # Calcular rotación mensual (veces que rota el inventario en un mes)
            df_merged['rotacion_mes'] = df_merged.apply(
                lambda row: (30 / row['dias_inventario']) if row['dias_inventario'] > 0 else 0,
                axis=1
            )
            
            # Crear el DataFrame final con la estructura requerida
            df_final = pd.DataFrame({
                'producto_sk': None,  # Se completará en el transformer con lookup en dim_producto
                'pdv_sk': pdv_sk,
                'fecha_sk': None,  # Se completará en el transformer con lookup en dim_fecha
                'codigo_producto': df_merged['codigo_producto'],
                'codigo_pdv': codigo_pdv,
                'fecha': df_merged['fecha'],
                'venta_unidades': df_merged['venta_unidades'],
                'venta_cajas': df_merged['venta_cajas'],
                'venta_blisters': df_merged['venta_blisters'],
                'costo_unitario': df_merged['costo_unitario'],
                'precio_venta_unitario': df_merged['precio_venta_unitario'],
                'costo_total': df_merged['costo_total'],
                'venta_total': df_merged['venta_total'],
                'margen_bruto': df_merged['margen_bruto'],
                'margen_porcentaje': df_merged['margen_porcentaje'],
                'inventario_unidades_inicial': 0,  # No tenemos datos históricos iniciales
                'inventario_unidades_final': df_merged['inventario_unidades_final'],
                'dias_inventario': df_merged['dias_inventario'],
                'rotacion_mes': df_merged['rotacion_mes']
            })
            
            if self.logger:
                self.logger.info(f"🔄 Rotación calculada para PDV {codigo_pdv}: {len(df_final)} productos")
                
            return df_final
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al calcular rotación para PDV {codigo_pdv}: {e}")
            return pd.DataFrame()
