# actualizar_rotacion_historica.py
from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

def actualizar_rotacion_historica():
    # Conectar a la base de datos
    db_url = get_mysql_url("gestion_compras")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as connection:
            # Actualizar los valores de rotación histórica
            update_sql = """
            UPDATE fact_rotacion 
            SET 
                rotacion_m1 = rotacion_mes * 0.95,
                rotacion_m2 = rotacion_mes * 0.90,
                rotacion_m3 = rotacion_mes * 0.85
            """
            connection.execute(text(update_sql))
            connection.commit()
            
            # Verificar la actualización con algunos ejemplos
            query = """
            SELECT 
                codigo_producto, 
                nombre_producto, 
                codigo_pdv, 
                nombre_pdv, 
                rotacion_mes, 
                rotacion_m1, 
                rotacion_m2, 
                rotacion_m3 
            FROM fact_rotacion 
            LIMIT 10
            """
            result = connection.execute(text(query))
            rows = result.fetchall()
            
            print("Datos históricos de rotación simulados correctamente.")
            print("\nEjemplos de datos actualizados:")
            print("=" * 100)
            print(f"{'CÓDIGO':12} | {'PRODUCTO':30} | {'PDV':8} | {'NOMBRE PDV':15} | {'ROT ACT':7} | {'ROT M-1':7} | {'ROT M-2':7} | {'ROT M-3':7}")
            print("-" * 100)
            
            for row in rows:
                codigo = row[0]
                nombre = row[1]
                pdv = row[2]
                nombre_pdv = row[3]
                rot_actual = row[4]
                rot_m1 = row[5]
                rot_m2 = row[6]
                rot_m3 = row[7]
                
                print(f"{str(codigo)[:12]:12} | {nombre[:30]:30} | {str(pdv)[:8]:8} | {nombre_pdv[:15]:15} | {rot_actual:7.2f} | {rot_m1:7.2f} | {rot_m2:7.2f} | {rot_m3:7.2f}")
            
    except Exception as e:
        print(f"Error al actualizar datos históricos: {str(e)}")

if __name__ == "__main__":
    actualizar_rotacion_historica()
