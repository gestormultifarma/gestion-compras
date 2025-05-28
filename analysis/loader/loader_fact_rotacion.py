# analysis/loader/loader_fact_rotacion.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url
from analysis.loader.loader_base import BaseLoader

class LoaderFactRotacion(BaseLoader):
    """
    Cargador especializado para la tabla fact_rotacion.
    Implementa una estrategia de actualización incremental.
    """
    
    def __init__(self, logger=None):
        """
        Inicializa el cargador para la tabla fact_rotacion.
        
        Args:
            logger: Instancia de logger para registrar eventos
        """
        super().__init__(db_name="gestion_compras", logger=logger)
        
    def cargar_dataframe(self, df, nombre_tabla=None):
        """
        Carga los datos de rotación a la tabla fact_rotacion usando una estrategia
        de procesamiento por lotes.
        
        Args:
            df (pandas.DataFrame): DataFrame con los datos a cargar
            nombre_tabla (str, optional): Ignorado, siempre se usa 'fact_rotacion'
            
        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            if df.empty:
                if self.logger:
                    self.logger.warning("⚠️ DataFrame vacío, no se realizará carga")
                return False
            
            # Siempre usar fact_rotacion independientemente del nombre_tabla proporcionado
            nombre_tabla_destino = "fact_rotacion"
            
            # Procesar en lotes para evitar problemas de memoria y bloqueos
            tamaño_lote = 100  # Número de registros por lote
            total_registros = len(df)
            lotes = [df[i:i+tamaño_lote] for i in range(0, total_registros, tamaño_lote)]
            
            if self.logger:
                self.logger.info(f"📦 Procesando {total_registros} registros en {len(lotes)} lotes")
            
            # Contadores para estadísticas
            registros_actualizados = 0
            registros_insertados = 0
            
            # Procesar cada lote
            for i, lote in enumerate(lotes):
                if self.logger:
                    self.logger.info(f"🔄 Procesando lote {i+1}/{len(lotes)} ({len(lote)} registros)")
                
                # Usar una transacción por lote
                with self.engine.begin() as connection:
                    for _, row in lote.iterrows():
                        # Comprobar si ya existe este registro
                        existe = self._verificar_registro_existente(
                            connection, 
                            row['codigo_producto'], 
                            row['codigo_pdv'], 
                            row['fecha']
                        )
                        
                        if existe:
                            # Actualizar registro existente
                            self._actualizar_registro(connection, row)
                            registros_actualizados += 1
                        else:
                            # Insertar nuevo registro
                            self._insertar_registro(connection, row)
                            registros_insertados += 1
            
            # Registrar resultado
            mensaje_exito = (
                f"✅ Tabla '{nombre_tabla_destino}' actualizada con éxito. "
                f"Registros insertados: {registros_insertados}, "
                f"Registros actualizados: {registros_actualizados}"
            )
            print(mensaje_exito)
            
            if self.logger:
                self.logger.info(mensaje_exito)
                
            return True
            
        except Exception as e:
            mensaje_error = f"❌ Error al cargar fact_rotacion: {e}"
            print(mensaje_error)
            
            if self.logger:
                self.logger.error(mensaje_error)
                
            return False
    
    def _verificar_registro_existente(self, connection, codigo_producto, codigo_pdv, fecha):
        """
        Verifica si ya existe un registro para la combinación de producto, PDV y fecha.
        
        Args:
            connection: Conexión a la base de datos
            codigo_producto (str): Código del producto
            codigo_pdv (str): Código del PDV
            fecha (datetime): Fecha del registro
            
        Returns:
            bool: True si existe, False en caso contrario
        """
        query = text("""
            SELECT 1
            FROM fact_rotacion
            WHERE codigo_producto = :codigo_producto
              AND codigo_pdv = :codigo_pdv
              AND fecha = :fecha
        """)
        
        result = connection.execute(
            query, 
            {
                "codigo_producto": codigo_producto,
                "codigo_pdv": codigo_pdv,
                "fecha": fecha
            }
        ).fetchone()
        
        return result is not None
    
    def _actualizar_registro(self, connection, row):
        """
        Actualiza un registro existente en la tabla fact_rotacion.
        
        Args:
            connection: Conexión a la base de datos
            row (Series): Fila con los datos a actualizar
            
        Returns:
            None
        """
        query = text("""
            UPDATE fact_rotacion
            SET 
                producto_sk = :producto_sk,
                pdv_sk = :pdv_sk,
                fecha_sk = :fecha_sk,
                venta_unidades = :venta_unidades,
                venta_cajas = :venta_cajas,
                venta_blisters = :venta_blisters,
                costo_unitario = :costo_unitario,
                precio_venta_unitario = :precio_venta_unitario,
                costo_total = :costo_total,
                venta_total = :venta_total,
                margen_bruto = :margen_bruto,
                margen_porcentaje = :margen_porcentaje,
                inventario_unidades_inicial = :inventario_unidades_inicial,
                inventario_unidades_final = :inventario_unidades_final,
                dias_inventario = :dias_inventario,
                rotacion_mes = :rotacion_mes,
                fecha_actualizacion = NOW()
            WHERE 
                codigo_producto = :codigo_producto
                AND codigo_pdv = :codigo_pdv
                AND fecha = :fecha
        """)
        
        connection.execute(
            query,
            {
                "producto_sk": row.get('producto_sk'),
                "pdv_sk": row.get('pdv_sk'),
                "fecha_sk": row.get('fecha_sk'),
                "venta_unidades": row.get('venta_unidades', 0),
                "venta_cajas": row.get('venta_cajas', 0),
                "venta_blisters": row.get('venta_blisters', 0),
                "costo_unitario": row.get('costo_unitario', 0),
                "precio_venta_unitario": row.get('precio_venta_unitario', 0),
                "costo_total": row.get('costo_total', 0),
                "venta_total": row.get('venta_total', 0),
                "margen_bruto": row.get('margen_bruto', 0),
                "margen_porcentaje": row.get('margen_porcentaje', 0),
                "inventario_unidades_inicial": row.get('inventario_unidades_inicial', 0),
                "inventario_unidades_final": row.get('inventario_unidades_final', 0),
                "dias_inventario": row.get('dias_inventario', 0),
                "rotacion_mes": row.get('rotacion_mes', 0),
                "codigo_producto": row.get('codigo_producto'),
                "codigo_pdv": row.get('codigo_pdv'),
                "fecha": row.get('fecha')
            }
        )
    
    def _insertar_registro(self, connection, row):
        """
        Inserta un nuevo registro en la tabla fact_rotacion.
        
        Args:
            connection: Conexión a la base de datos
            row (Series): Fila con los datos a insertar
            
        Returns:
            None
        """
        query = text("""
            INSERT INTO fact_rotacion (
                producto_sk, pdv_sk, fecha_sk, codigo_producto, codigo_pdv, fecha,
                venta_unidades, venta_cajas, venta_blisters,
                costo_unitario, precio_venta_unitario,
                costo_total, venta_total,
                margen_bruto, margen_porcentaje,
                inventario_unidades_inicial, inventario_unidades_final,
                dias_inventario, rotacion_mes,
                fecha_carga, fecha_actualizacion
            ) VALUES (
                :producto_sk, :pdv_sk, :fecha_sk, :codigo_producto, :codigo_pdv, :fecha,
                :venta_unidades, :venta_cajas, :venta_blisters,
                :costo_unitario, :precio_venta_unitario,
                :costo_total, :venta_total,
                :margen_bruto, :margen_porcentaje,
                :inventario_unidades_inicial, :inventario_unidades_final,
                :dias_inventario, :rotacion_mes,
                NOW(), NOW()
            )
        """)
        
        connection.execute(
            query,
            {
                "producto_sk": row.get('producto_sk'),
                "pdv_sk": row.get('pdv_sk'),
                "fecha_sk": row.get('fecha_sk'),
                "codigo_producto": row.get('codigo_producto'),
                "codigo_pdv": row.get('codigo_pdv'),
                "fecha": row.get('fecha'),
                "venta_unidades": row.get('venta_unidades', 0),
                "venta_cajas": row.get('venta_cajas', 0),
                "venta_blisters": row.get('venta_blisters', 0),
                "costo_unitario": row.get('costo_unitario', 0),
                "precio_venta_unitario": row.get('precio_venta_unitario', 0),
                "costo_total": row.get('costo_total', 0),
                "venta_total": row.get('venta_total', 0),
                "margen_bruto": row.get('margen_bruto', 0),
                "margen_porcentaje": row.get('margen_porcentaje', 0),
                "inventario_unidades_inicial": row.get('inventario_unidades_inicial', 0),
                "inventario_unidades_final": row.get('inventario_unidades_final', 0),
                "dias_inventario": row.get('dias_inventario', 0),
                "rotacion_mes": row.get('rotacion_mes', 0)
            }
        )
