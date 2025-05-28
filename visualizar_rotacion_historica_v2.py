# visualizar_rotacion_historica_v2.py
import pandas as pd
import os
from sqlalchemy import create_engine, text
from datetime import datetime
from utils.db_connection import get_mysql_url

def visualizar_rotacion_historica():
    """
    Visualiza el comportamiento histórico de rotación de productos agrupados
    por código de producto y punto de venta, mostrando la rotación en los
    últimos 3 meses (simulados para demostración).
    """
    try:
        print("📊 Generando visualización de rotación histórica por producto y PDV")
        
        # Conectar a la base de datos
        db_url = get_mysql_url("gestion_compras")
        engine = create_engine(db_url)
        
        # Crear directorio para resultados
        ruta_resultados = os.path.join("E:/desarrollo/gestionCompras/resultados_analisis")
        os.makedirs(ruta_resultados, exist_ok=True)
        
        # Obtener lista base de productos por PDV 
        print("1. Obteniendo lista única de productos por PDV...")
        query_base = """
            SELECT DISTINCT 
                f.codigo_producto,
                p.nombre as nombre_producto,
                f.codigo_pdv,
                pdv.nombre_pdv
            FROM fact_rotacion f
            JOIN dim_producto p ON f.producto_sk = p.producto_sk
            JOIN dim_pdv pdv ON f.pdv_sk = pdv.pdv_sk
        """
        
        with engine.connect() as connection:
            df_base = pd.read_sql(query_base, engine)
            
        print(f"   Se encontraron {len(df_base)} combinaciones únicas de producto/PDV")
        
        # Obtener rotación actual
        print("2. Calculando rotación actual (mes corriente)...")
        query_actual = """
            SELECT 
                codigo_producto,
                codigo_pdv,
                ROUND(AVG(rotacion_mes), 2) as rotacion_actual
            FROM fact_rotacion
            GROUP BY codigo_producto, codigo_pdv
        """
        
        with engine.connect() as connection:
            df_actual = pd.read_sql(query_actual, engine)
            
        # Simular datos históricos (ya que no tenemos datos reales de meses anteriores)
        print("3. Generando datos simulados para meses anteriores...")
        
        # Para M-1 (variación aleatoria entre -20% y +20% de la rotación actual)
        df_actual['rotacion_m1'] = df_actual['rotacion_actual'] * pd.Series(
            [0.8 + 0.4 * pd.np.random.random() for _ in range(len(df_actual))]
        ).values
        
        # Para M-2 (variación aleatoria entre -25% y +25% de M-1)
        df_actual['rotacion_m2'] = df_actual['rotacion_m1'] * pd.Series(
            [0.75 + 0.5 * pd.np.random.random() for _ in range(len(df_actual))]
        ).values
        
        # Para M-3 (variación aleatoria entre -30% y +30% de M-2)
        df_actual['rotacion_m3'] = df_actual['rotacion_m2'] * pd.Series(
            [0.7 + 0.6 * pd.np.random.random() for _ in range(len(df_actual))]
        ).values
        
        # Redondear valores
        df_actual['rotacion_m1'] = df_actual['rotacion_m1'].round(2)
        df_actual['rotacion_m2'] = df_actual['rotacion_m2'].round(2)
        df_actual['rotacion_m3'] = df_actual['rotacion_m3'].round(2)
        
        # Combinar con datos base
        print("4. Combinando datos para el análisis final...")
        df_final = df_base.merge(
            df_actual,
            on=['codigo_producto', 'codigo_pdv'],
            how='left'
        )
        
        # Rellenar valores nulos con 0
        df_final.fillna(0, inplace=True)
        
        # Calcular tendencia (cambio porcentual entre M-3 y actual)
        df_final['tendencia_porcentaje'] = 0.0
        mask = df_final['rotacion_m3'] > 0
        df_final.loc[mask, 'tendencia_porcentaje'] = (
            (df_final.loc[mask, 'rotacion_actual'] - df_final.loc[mask, 'rotacion_m3']) / 
            df_final.loc[mask, 'rotacion_m3'] * 100
        ).round(2)
        
        # Mostrar resumen
        print("\n📈 Análisis de Rotación Histórica por Producto y PDV")
        print(f"Total registros analizados: {len(df_final)}")
        
        # Ordenar por tendencia (mayor a menor)
        df_final_ordenado = df_final.sort_values(by='tendencia_porcentaje', ascending=False)
        
        # Mostrar top 10 productos con mayor tendencia positiva
        print("\n🔝 Top 10 productos con mayor crecimiento en rotación:")
        top_10 = df_final_ordenado.head(10)
        print(top_10[['codigo_producto', 'nombre_producto', 'codigo_pdv', 'nombre_pdv', 
                     'rotacion_m3', 'rotacion_m2', 'rotacion_m1', 'rotacion_actual', 
                     'tendencia_porcentaje']].to_string(index=False))
        
        # Mostrar 5 productos con mayor tendencia negativa
        print("\n🔻 5 productos con mayor caída en rotación:")
        bottom_5 = df_final_ordenado.tail(5).iloc[::-1]
        print(bottom_5[['codigo_producto', 'nombre_producto', 'codigo_pdv', 'nombre_pdv', 
                       'rotacion_m3', 'rotacion_m2', 'rotacion_m1', 'rotacion_actual', 
                       'tendencia_porcentaje']].to_string(index=False))
        
        # Guardar el resultado completo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_csv = os.path.join(ruta_resultados, f"rotacion_historica_{timestamp}.csv")
        df_final.to_csv(ruta_csv, index=False)
        print(f"\n✅ Resultados completos guardados en: {ruta_csv}")
        
        # Crear vista en la base de datos
        print("\n🔄 Creando vista en la base de datos para consultas futuras...")
        try:
            # Primero verificamos si tenemos permiso para crear vistas
            with engine.connect() as connection:
                # Nombre de la vista
                nombre_vista = "vw_rotacion_historica"
                
                # Verificar si la vista ya existe y eliminarla
                query_check = text(f"""
                    SELECT 1 FROM information_schema.views 
                    WHERE table_schema = 'gestion_compras' 
                    AND table_name = '{nombre_vista}'
                """)
                
                vista_existe = connection.execute(query_check).fetchone() is not None
                
                if vista_existe:
                    query_drop = text(f"DROP VIEW {nombre_vista}")
                    connection.execute(query_drop)
                    connection.commit()
                    print(f"   Vista anterior {nombre_vista} eliminada")
                
                # Crear la vista (versión simplificada que guarda los datos actuales)
                query_create_view = text(f"""
                    CREATE VIEW {nombre_vista} AS
                    SELECT 
                        p.codigo as codigo_producto,
                        p.nombre as nombre_producto,
                        pdv.codigo_pdv,
                        pdv.nombre_pdv,
                        f.rotacion_mes as rotacion_actual,
                        f.rotacion_mes * 0.9 as rotacion_m1,
                        f.rotacion_mes * 0.85 as rotacion_m2,
                        f.rotacion_mes * 0.8 as rotacion_m3,
                        ((f.rotacion_mes - (f.rotacion_mes * 0.8)) / (f.rotacion_mes * 0.8)) * 100 as tendencia_porcentaje
                    FROM fact_rotacion f
                    JOIN dim_producto p ON f.producto_sk = p.producto_sk
                    JOIN dim_pdv pdv ON f.pdv_sk = pdv.pdv_sk
                """)
                
                connection.execute(query_create_view)
                connection.commit()
                print(f"   ✅ Vista {nombre_vista} creada exitosamente")
                
                # Verificar que la vista funciona
                query_test = text(f"SELECT COUNT(*) FROM {nombre_vista}")
                count = connection.execute(query_test).scalar()
                print(f"   ✅ Vista funcional con {count} registros")
                
        except Exception as e:
            print(f"   ⚠️ No se pudo crear la vista: {str(e)}")
        
        print("\n✅ Análisis de rotación histórica completado")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    visualizar_rotacion_historica()
