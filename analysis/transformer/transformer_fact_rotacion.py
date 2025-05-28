# analysis/transformer/transformer_fact_rotacion.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
from analysis.transformer.transformer_base import BaseTransformer

class TransformadorFactRotacion(BaseTransformer):
    """
    Transformador para datos de rotación de inventario.
    Aplica transformaciones y enriquece los datos para la tabla fact_rotacion.
    """
    
    def __init__(self, df, logger=None):
        """
        Inicializa el transformador con los datos ya extraídos.
        
        Args:
            df (pd.DataFrame): DataFrame con los datos extraídos
            logger: Instancia de logger para registrar eventos
        """
        self.path = None  # No usamos path en este caso
        self.df = df
        self.logger = logger
        self.db_url = get_mysql_url("gestion_compras")
        self.engine = create_engine(self.db_url)
    
    def transformar(self):
        """
        Transforma y enriquece los datos para fact_rotacion.
        
        Returns:
            pd.DataFrame: DataFrame transformado listo para cargar
        """
        if self.df is None or self.df.empty:
            if self.logger:
                self.logger.warning("⚠️ No hay datos para transformar")
            return pd.DataFrame()
        
        try:
            # 1. Limpiar y validar datos
            self._limpiar_datos()
            
            # 2. Enriquecer con claves de dimensiones
            self._buscar_producto_sk()
            self._buscar_fecha_sk()
            
            # 3. Calcular campos adicionales
            self._calcular_campos_adicionales()
            
            # 4. Formato final
            self._ajustar_formato_final()
            
            if self.logger:
                self.logger.info(f"✅ Transformación completada. Registros finales: {len(self.df)}")
            
            return self.df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error en la transformación: {e}")
            return pd.DataFrame()
    
    def _limpiar_datos(self):
        """Limpia y normaliza los datos para asegurar calidad"""
        # Convertir campos numéricos
        columnas_numericas = [
            'venta_unidades', 'venta_cajas', 'venta_blisters',
            'costo_unitario', 'precio_venta_unitario',
            'costo_total', 'venta_total',
            'margen_bruto', 'margen_porcentaje',
            'inventario_unidades_inicial', 'inventario_unidades_final',
            'dias_inventario', 'rotacion_mes'
        ]
        
        for col in columnas_numericas:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                self.df[col] = self.df[col].fillna(0)
        
        # Convertir fechas
        if 'fecha' in self.df.columns:
            self.df['fecha'] = pd.to_datetime(self.df['fecha'], errors='coerce')
            # Usar fecha actual para valores nulos
            self.df['fecha'] = self.df['fecha'].fillna(pd.Timestamp.now().normalize())
        
        # Limpiar códigos de producto y PDV
        if 'codigo_producto' in self.df.columns:
            self.df['codigo_producto'] = self.df['codigo_producto'].astype(str).str.strip()
            
        if 'codigo_pdv' in self.df.columns:
            self.df['codigo_pdv'] = self.df['codigo_pdv'].astype(str).str.strip()
    
    def _buscar_producto_sk(self):
        """Busca las claves de producto en dim_producto"""
        try:
            # Obtener lista única de códigos de producto
            codigos_producto = self.df['codigo_producto'].unique().tolist()
            
            # Buscar los productos_sk en la dimensión
            query = text("""
                SELECT producto_sk, codigo
                FROM dim_producto
                WHERE codigo IN :codigos
            """)
            
            with self.engine.connect() as connection:
                resultado = pd.read_sql(
                    query, 
                    connection, 
                    params={"codigos": tuple(codigos_producto) if len(codigos_producto) > 1 else f"('{codigos_producto[0]}')" }
                )
            
            # Crear diccionario de mapeo
            mapeo_productos = dict(zip(resultado['codigo'], resultado['producto_sk']))
            
            # Aplicar el mapeo al DataFrame
            self.df['producto_sk'] = self.df['codigo_producto'].map(mapeo_productos)
            
            # Contar cuántos se mapearon
            productos_mapeados = self.df['producto_sk'].notnull().sum()
            
            if self.logger:
                self.logger.info(f"🔍 Productos mapeados: {productos_mapeados} de {len(self.df)}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al buscar producto_sk: {e}")
    
    def _buscar_fecha_sk(self):
        """Busca o crea las claves de fecha en dim_fecha"""
        try:
            # Obtener lista única de fechas
            fechas_unicas = self.df['fecha'].dt.date.unique()
            
            # Buscar fecha_sk para cada fecha en dim_fecha
            fechas_map = {}
            
            with self.engine.connect() as connection:
                for fecha in fechas_unicas:
                    query = text("""
                        SELECT fecha_sk
                        FROM dim_fecha
                        WHERE fecha = :fecha
                    """)
                    
                    resultado = connection.execute(query, {"fecha": fecha}).fetchone()
                    
                    if resultado:
                        fechas_map[fecha] = resultado[0]
                    else:
                        # La fecha no existe, intentar crearla
                        query_insertar = text("""
                            INSERT INTO dim_fecha (
                                fecha, dia, mes, anio, trimestre, semestre,
                                dia_semana, nombre_dia, nombre_mes, 
                                es_fin_semana, es_feriado
                            ) VALUES (
                                :fecha, 
                                DAY(:fecha), 
                                MONTH(:fecha), 
                                YEAR(:fecha),
                                QUARTER(:fecha),
                                IF(MONTH(:fecha) <= 6, 1, 2),
                                DAYOFWEEK(:fecha),
                                DATE_FORMAT(:fecha, '%W'),
                                DATE_FORMAT(:fecha, '%M'),
                                IF(DAYOFWEEK(:fecha) IN (1, 7), 1, 0),
                                0  # Por defecto no es feriado
                            )
                        """)
                        
                        connection.execute(query_insertar, {"fecha": fecha})
                        
                        # Obtener el ID generado
                        query_id = text("""
                            SELECT fecha_sk 
                            FROM dim_fecha 
                            WHERE fecha = :fecha
                        """)
                        
                        resultado = connection.execute(query_id, {"fecha": fecha}).fetchone()
                        
                        if resultado:
                            fechas_map[fecha] = resultado[0]
            
            # Aplicar el mapeo al DataFrame
            self.df['fecha_sk'] = self.df['fecha'].dt.date.map(fechas_map)
            
            if self.logger:
                self.logger.info(f"📅 Fechas mapeadas: {len(fechas_map)}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"💥 Error al buscar/crear fecha_sk: {e}")
    
    def _calcular_campos_adicionales(self):
        """Calcula campos adicionales y ajusta valores"""
        # Recalcular margen_porcentaje para evitar valores extremos
        self.df['margen_porcentaje'] = self.df.apply(
            lambda row: min(100, (row['margen_bruto'] / row['venta_total'] * 100)) 
                       if row['venta_total'] > 0 else 0,
            axis=1
        )
        
        # Limitar valores extremos en dias_inventario y rotacion_mes
        self.df['dias_inventario'] = self.df['dias_inventario'].clip(0, 90)
        self.df['rotacion_mes'] = self.df['rotacion_mes'].clip(0, 30)
    
    def _ajustar_formato_final(self):
        """Ajusta el formato final del DataFrame para la carga"""
        # Asegurar que todos los campos estén presentes
        columnas_requeridas = [
            'producto_sk', 'pdv_sk', 'fecha_sk',
            'codigo_producto', 'codigo_pdv', 'fecha',
            'venta_unidades', 'venta_cajas', 'venta_blisters',
            'costo_unitario', 'precio_venta_unitario',
            'costo_total', 'venta_total',
            'margen_bruto', 'margen_porcentaje',
            'inventario_unidades_inicial', 'inventario_unidades_final',
            'dias_inventario', 'rotacion_mes'
        ]
        
        for col in columnas_requeridas:
            if col not in self.df.columns:
                self.df[col] = None
