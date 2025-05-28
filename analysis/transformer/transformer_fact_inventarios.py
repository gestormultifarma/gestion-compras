# analysis/transformer/transformer_fact_inventarios.py

import pandas as pd
from datetime import datetime
from analysis.transformer.transformer_base import BaseTransformer

class TransformadorFactInventarios(BaseTransformer):
    """
    Transformador para convertir los datos extraídos de inventario 
    al formato final para la tabla fact_inventarios.
    """
    
    def __init__(self, data, logger=None):
        """
        Inicializa el transformador con los datos ya extraídos.
        
        Args:
            data (List[dict]): Lista de registros de inventario
            logger: Instancia de logger para registrar eventos
        """
        self.data = data
        self.logger = logger
        self.df = None
        
    def transformar(self):
        """
        Transforma los datos para fact_inventarios, incluyendo cálculos 
        y validaciones adicionales.
        
        Returns:
            pandas.DataFrame: DataFrame listo para cargar a fact_inventarios
        """
        if not self.data:
            if self.logger:
                self.logger.warning("⚠️ No hay datos para transformar")
            return pd.DataFrame()
        
        # Convertir la lista de diccionarios a DataFrame
        self.df = pd.DataFrame(self.data)
        
        # Validar datos y realizar transformaciones adicionales
        self._validar_y_limpiar_datos()
        self._calcular_metricas_adicionales()
        
        if self.logger:
            self.logger.info(f"✅ Transformación completada. Registros: {len(self.df)}")
            
        return self.df
    
    def _validar_y_limpiar_datos(self):
        """Valida y limpia los datos para asegurar integridad"""
        # Asegurar que las columnas numéricas sean de tipo numérico
        numeric_cols = ['cantidad_existencias', 'valor_existencias', 'costo_promedio']
        
        for col in numeric_cols:
            if col in self.df.columns:
                # Convertir a numérico, valores inválidos como NaN
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                
                # Reemplazar NaN por 0
                self.df[col] = self.df[col].fillna(0)
                
        # Asegurar que fecha es una fecha válida
        if 'fecha' in self.df.columns:
            self.df['fecha'] = pd.to_datetime(self.df['fecha'], errors='coerce')
            # Si hay fechas inválidas, usar la fecha actual
            self.df['fecha'] = self.df['fecha'].fillna(pd.Timestamp.now().normalize())
    
    def _calcular_metricas_adicionales(self):
        """Calcula métricas adicionales basadas en los datos de inventario"""
        # Calcular días de stock basado en ventas promedio (simulado)
        # En una implementación real, esto podría consultar datos de ventas reales
        if 'cantidad_existencias' in self.df.columns and 'nivel_stock' in self.df.columns:
            # Mapeo basado en nivel de stock (simplificado para demostración)
            nivel_a_dias = {
                'Sin stock': 0,
                'Bajo': 7,
                'Medio': 15,
                'Alto': 30
            }
            
            self.df['dias_stock'] = self.df['nivel_stock'].map(nivel_a_dias)
            
            # Calcular rotación en días y semanas
            # Asumimos un período de 30 días para este cálculo simplificado
            self.df['rotacion_dias'] = self.df['dias_stock'].apply(
                lambda x: 30 if x == 0 else x
            )
            
            self.df['rotacion_semanas'] = (self.df['rotacion_dias'] / 7).round(2)
            
        # Asegurar que todas las columnas de fecha tienen el formato correcto
        timestamp_cols = ['fecha_creacion', 'fecha_actualizacion']
        for col in timestamp_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col])
