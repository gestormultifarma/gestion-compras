import mysql.connector
from mysql.connector import Error

def check_table_structure():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='gestion_compras',
            user='root',
            password='Multifarma123*'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Verificar estructura de fact_inventarios
            cursor.execute("DESCRIBE fact_inventarios")
            print("\nEstructura de fact_inventarios:")
            for row in cursor.fetchall():
                print(row)
            
            # Verificar estructura de fact_rotacion
            cursor.execute("DESCRIBE fact_rotacion")
            print("\nEstructura de fact_rotacion:")
            for row in cursor.fetchall():
                print(row)
                
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    check_table_structure()
