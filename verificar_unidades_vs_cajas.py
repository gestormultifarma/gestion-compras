# verificar_unidades_vs_cajas.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def verificar_discrepancia():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Verificar el producto específico en el PDV mencionado
            pdv = "34060"  # cosmocentro1
            producto = "100019393"  # ACETAMINOFEN 500 MG 100 TBS ICOM
            
            # Consultar tabla staging para ver los datos originales
            tabla_staging = f"stg_rotacion_de_cosmocentro1_{pdv}_1"  # periodo m1
            
            query = f"""
            SELECT 
                codigo,
                nombre,
                contenido_caja,
                venta_caja,
                venta_blister,
                venta_unidad
            FROM {tabla_staging}
            WHERE codigo = '{producto}'
            """
            
            result = connection.execute(text(query))
            row = result.fetchone()
            
            if row:
                print(f"Datos originales en {tabla_staging}:")
                print(f"Código: {row[0]}")
                print(f"Nombre: {row[1]}")
                print(f"Contenido por caja: {row[2]}")
                print(f"Venta en cajas: {row[3]}")
                print(f"Venta en blisters: {row[4]}")
                print(f"Venta en unidades: {row[5]}")
                
                # Calcular la conversión correcta
                contenido_caja = row[2] or 1
                venta_cajas = row[3] or 0
                venta_unidades = row[5] or 0
                
                # Calcular equivalente en cajas si todo fuera en unidades
                equivalente_cajas = venta_cajas + (venta_unidades / contenido_caja if contenido_caja > 0 else 0)
                
                print(f"\nCálculo correcto:")
                print(f"Equivalente en cajas: {equivalente_cajas:.2f}")
                
                # Consultar cómo quedó en fact_rotacion
                query = f"""
                SELECT 
                    codigo_pdv,
                    nombre_pdv,
                    codigo_producto,
                    nombre_producto,
                    rotacion_m1
                FROM fact_rotacion
                WHERE codigo_pdv = '{pdv}' AND codigo_producto = '{producto}'
                """
                
                result = connection.execute(text(query))
                row_fact = result.fetchone()
                
                if row_fact:
                    print(f"\nDatos en fact_rotacion:")
                    print(f"Código PDV: {row_fact[0]}")
                    print(f"Nombre PDV: {row_fact[1]}")
                    print(f"Código Producto: {row_fact[2]}")
                    print(f"Nombre Producto: {row_fact[3]}")
                    print(f"Rotación M1: {row_fact[4]}")
                    
                    # Verificar la discrepancia
                    print(f"\nDiscrepancia:")
                    discrepancia = row_fact[4] - equivalente_cajas
                    print(f"Diferencia: {discrepancia:.2f}")
                    
                    if abs(discrepancia) > 0.1:
                        print(f"⚠️ Se detectó una discrepancia significativa")
                        
                        # Verificar si el error es por un factor de 100
                        factor = row_fact[4] / equivalente_cajas if equivalente_cajas > 0 else 0
                        print(f"Factor de error aproximado: {factor:.2f}")
                        
                        if 95 < factor < 105:
                            print("⚠️ El error parece ser un factor de aproximadamente 100x")
                else:
                    print("Producto no encontrado en fact_rotacion")
            else:
                print(f"Producto {producto} no encontrado en {tabla_staging}")
            
            # Verificar más ejemplos para confirmar el patrón
            print("\nVerificando otros ejemplos para confirmar el patrón:")
            
            query = f"""
            SELECT 
                fr.codigo_pdv,
                fr.nombre_pdv,
                fr.codigo_producto,
                fr.nombre_producto,
                fr.rotacion_m1,
                stg.venta_caja
            FROM fact_rotacion fr
            JOIN {tabla_staging} stg ON fr.codigo_producto = stg.codigo
            WHERE stg.venta_caja > 0
            LIMIT 5
            """
            
            result = connection.execute(text(query))
            rows = result.fetchall()
            
            print(f"{'PDV':6} | {'PRODUCTO':12} | {'ROTACION_M1':10} | {'VENTA_CAJA':10} | {'FACTOR':10}")
            print("-" * 60)
            
            for row in rows:
                pdv = row[0]
                producto = row[2]
                rotacion_m1 = row[4]
                venta_caja = row[5]
                factor = rotacion_m1 / venta_caja if venta_caja > 0 else 0
                
                print(f"{pdv:6} | {producto:12} | {rotacion_m1:10.2f} | {venta_caja:10.2f} | {factor:10.2f}")
                
    except Exception as e:
        import traceback
        print(f"Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    verificar_discrepancia()
