# utils\db_connection.py

def get_mysql_url(db_name):
    user = "root"
    password = "Multifarma123*"
    host = "localhost"
    port = 3306
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
