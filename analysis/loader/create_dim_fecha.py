# analysis/loader/create_dim_fecha.py

from sqlalchemy import create_engine, text, inspect as sqlalchemy_inspect
from utils.db_connection import get_mysql_url
from datetime import date, timedelta
import pandas as pd

def create_and_populate_dim_fecha(db_name: str, start_year: int = 2024, end_year: int = 2026):
    engine = create_engine(get_mysql_url(db_name))
    
    recrear_tabla = False
    with engine.connect() as conn_check:
        inspector = sqlalchemy_inspect(conn_check)
        table_exists = inspector.has_table("dim_fecha")
        if table_exists:
            columns = [col['name'] for col in inspector.get_columns('dim_fecha')]
            # Verificar todas las columnas, incluyendo las nuevas de temporada
            columnas_requeridas = {
                'merchandising', 'e_commerce', 
                'temporada_comercial', 'temporada_salud', 'evento_especial'
            }
            if not columnas_requeridas.issubset(set(columns)):
                print("Tabla dim_fecha existente no tiene todas las columnas nuevas (temporadas/eventos), se marcará para recrear.")
                recrear_tabla = True
        else:
            print("Tabla dim_fecha no existe, se marcará para crear.")
            recrear_tabla = True

    if recrear_tabla:
        with engine.connect() as conn_recreate: 
            with conn_recreate.begin(): 
                if table_exists: 
                    print("Eliminando tabla dim_fecha existente...")
                    conn_recreate.execute(text("DROP TABLE IF EXISTS dim_fecha;")) 
                
                print("Creando tabla dim_fecha...")
                conn_recreate.execute(text("""
                CREATE TABLE dim_fecha (
                  fecha_key INT NOT NULL PRIMARY KEY, fecha DATE NOT NULL UNIQUE, anio INT NOT NULL,
                  trimestre INT NOT NULL, mes INT NOT NULL, mes_nombre VARCHAR(20) NOT NULL,
                  dia INT NOT NULL, dia_nombre VARCHAR(10) NOT NULL, dia_semana INT NOT NULL,
                  semana_anyo INT NOT NULL, dia_del_anyo INT NOT NULL,
                  es_fin_de_semana TINYINT(1) NOT NULL, es_habil TINYINT(1) NOT NULL,
                  es_feriado TINYINT(1) NOT NULL DEFAULT 0,
                  temporada_climatica VARCHAR(50) NULL,  -- Aumentado un poco por si acaso
                  temporada_escolar VARCHAR(50) NULL,
                  temporada_comercial VARCHAR(100) NULL, -- Nueva columna
                  temporada_salud VARCHAR(100) NULL,     -- Nueva columna
                  evento_especial VARCHAR(100) NULL,   -- Nueva columna
                  merchandising VARCHAR(255) NULL,
                  e_commerce VARCHAR(255) NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                print("Tabla dim_fecha creada/recreada.")
    else:
        print("Tabla dim_fecha ya existe con las columnas necesarias. No se recreará.")

    # Cargar campañas
    campanas_merchandising_df = pd.DataFrame()
    try:
        with engine.connect() as conn:
            campanas_merchandising_df = pd.read_sql_table('dim_campana_merchandising', conn)
            campanas_merchandising_df['fecha_inicio'] = pd.to_datetime(campanas_merchandising_df['fecha_inicio']).dt.date
            campanas_merchandising_df['fecha_fin'] = pd.to_datetime(campanas_merchandising_df['fecha_fin']).dt.date
            print(f"Cargadas {len(campanas_merchandising_df)} campañas de merchandising.")
    except Exception as e:
        print(f"Advertencia: No se pudieron cargar campañas de merchandising: {e}. Se continuará sin ellas.")

    campanas_ecommerce_df = pd.DataFrame()
    try:
        with engine.connect() as conn:
            campanas_ecommerce_df = pd.read_sql_table('dim_campana_ecommerce', conn)
            campanas_ecommerce_df['fecha_inicio'] = pd.to_datetime(campanas_ecommerce_df['fecha_inicio']).dt.date
            campanas_ecommerce_df['fecha_fin'] = pd.to_datetime(campanas_ecommerce_df['fecha_fin']).dt.date
            print(f"Cargadas {len(campanas_ecommerce_df)} campañas de e-commerce.")
    except Exception as e:
        print(f"Advertencia: No se pudieron cargar campañas de e-commerce: {e}. Se continuará sin ellas.")

    feriados_por_ano = {
        2024: set(),
        2025: {
            "2025-01-01", "2025-01-06", "2025-03-24", "2025-04-17", "2025-04-18",
            "2025-05-01", "2025-06-02", "2025-06-23", "2025-06-30", "2025-07-20",
            "2025-08-07", "2025-08-18", "2025-10-13", "2025-11-03", "2025-11-17",
            "2025-12-08", "2025-12-25"
        },
        2026: set()
    }

    records = []
    start_date_obj = date(start_year, 1, 1)
    end_date_obj = date(end_year, 12, 31)
    delta = timedelta(days=1)
    current_date = start_date_obj

    print(f"Generando registros de fecha desde {start_date_obj} hasta {end_date_obj}...")
    while current_date <= end_date_obj:
        fk = int(current_date.strftime('%Y%m%d'))
        wd = current_date.weekday()
        es_fin = 1 if wd >= 5 else 0
        es_hab = 1 if wd < 5 else 0
        anio = current_date.year
        mes = current_date.month
        dia = current_date.day
        trimestre = (mes - 1) // 3 + 1
        semana_iso_tuple = current_date.isocalendar()
        semana_iso = semana_iso_tuple[1]
        dia_anyo = current_date.timetuple().tm_yday
        meses_es = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        dias_es = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        mes_nombre = meses_es[mes]
        dia_nombre = dias_es[wd]
        str_fecha = current_date.strftime('%Y-%m-%d')
        feriados_ano_actual = feriados_por_ano.get(anio, set())
        es_feriado = 1 if str_fecha in feriados_ano_actual else 0
        
        climatica = None
        escolar = None
        temporada_comercial_valor = None
        temporada_salud_valor = None
        evento_especial_valor = None
        merchandising_valor = None
        ecommerce_valor = None

        if anio == 2025:
            # Temporada climática 2025
            if mes in (1, 2): 
                climatica = 'Seca (Inicio Año)'
            elif mes in (3, 4, 5): 
                climatica = 'Lluvias 1'
            elif mes in (6, 7, 8, 9): 
                climatica = 'Seca (Mitad Año)'
            elif mes in (10, 11): 
                climatica = 'Lluvias 2'
            elif mes == 12: 
                climatica = 'Seca (Fin Año)'

            # Temporada escolar 2025
            if date(2025,1,1) <= current_date <= date(2025,1,19): 
                escolar = 'Vacaciones Inicio Año'
            elif date(2025,1,20) <= current_date <= date(2025,4,13): 
                escolar = 'Periodo Escolar 1'
            elif date(2025,4,14) <= current_date <= date(2025,4,20): 
                escolar = 'Semana Santa'
            elif date(2025,4,21) <= current_date <= date(2025,6,15): 
                escolar = 'Periodo Escolar 2'
            elif date(2025,6,16) <= current_date <= date(2025,7,6): 
                escolar = 'Vacaciones Mitad Año'
            elif date(2025,7,7) <= current_date <= date(2025,10,5): 
                escolar = 'Periodo Escolar 3'
            elif date(2025,10,6) <= current_date <= date(2025,10,12): 
                escolar = 'Receso Escolar Octubre'
            elif date(2025,10,13) <= current_date <= date(2025,11,30): 
                escolar = 'Periodo Escolar 4'
            elif date(2025,12,1) <= current_date <= date(2025,12,31): 
                escolar = 'Vacaciones Fin Año'
            
            # TEMPORADAS COMERCIALES Y EVENTOS ESPECIALES 2025
            if date(2025, 1, 1) <= current_date <= date(2025, 1, 6):
                temporada_comercial_valor = "Post-Navidad / Reyes"
                if current_date == date(2025, 1, 6): 
                    evento_especial_valor = "Día de Reyes"
            if date(2025, 2, 10) <= current_date <= date(2025, 2, 16):
                temporada_comercial_valor = "Semana San Valentín"
                if current_date == date(2025, 2, 14): 
                    evento_especial_valor = "San Valentín"
            if date(2025, 4, 13) <= current_date <= date(2025, 4, 20):
                temporada_comercial_valor = "Semana Santa"
                if current_date == date(2025, 4, 17): 
                    evento_especial_valor = "Jueves Santo"
                elif current_date == date(2025, 4, 18): 
                    evento_especial_valor = "Viernes Santo"
                elif current_date == date(2025, 4, 20): 
                    evento_especial_valor = "Domingo Resurrección"
            if date(2025, 5, 5) <= current_date <= date(2025, 5, 11):
                temporada_comercial_valor = "Semana Día de la Madre"
                if current_date == date(2025, 5, 11): 
                    evento_especial_valor = "Día de la Madre"
            if date(2025, 6, 9) <= current_date <= date(2025, 6, 15):
                temporada_comercial_valor = "Semana Día del Padre"
                if current_date == date(2025, 6, 15): 
                    evento_especial_valor = "Día del Padre"
            if date(2025, 9, 15) <= current_date <= date(2025, 9, 21):
                temporada_comercial_valor = "Semana Amor y Amistad"
                if current_date == date(2025, 9, 20): 
                    evento_especial_valor = "Día Amor y Amistad"
            if date(2025, 10, 27) <= current_date <= date(2025, 10, 31):
                temporada_comercial_valor = "Semana Halloween"
                if current_date == date(2025, 10, 31): 
                    evento_especial_valor = "Halloween"
            
            current_event = evento_especial_valor # Guardar evento actual para no sobrescribirlo con temporada general
            current_commercial = temporada_comercial_valor

            if date(2025, 11, 24) <= current_date <= date(2025, 12, 2):
                 temporada_comercial_valor = "Black Friday / Cyber Week"
                 if current_date == date(2025, 11, 28): 
                     evento_especial_valor = "Black Friday"
                 elif current_date == date(2025, 12, 1): 
                     evento_especial_valor = "Cyber Monday"
            elif (mes == 11 and dia >= 15) or mes == 12:
                 if not current_commercial: 
                     temporada_comercial_valor = "Temporada Navideña"
            
            if current_date == date(2025, 12, 7):
                evento_especial_valor = "Día de Velitas"
                if not current_commercial: 
                    temporada_comercial_valor = "Temporada Navideña"
            elif date(2025, 12, 16) <= current_date <= date(2025, 12, 24):
                evento_especial_valor = "Novenas Navideñas" if not current_event else current_event + "; Novenas"
                if not current_commercial: 
                    temporada_comercial_valor = "Temporada Navideña"
            elif current_date == date(2025, 12, 24):
                evento_especial_valor = "Noche Buena" if not current_event else current_event + "; Noche Buena"
                if not current_commercial: 
                    temporada_comercial_valor = "Temporada Navideña"
            elif current_date == date(2025, 12, 25):
                evento_especial_valor = "Navidad" if not current_event else current_event + "; Navidad"
                if not current_commercial: 
                    temporada_comercial_valor = "Temporada Navideña"
            elif current_date == date(2025, 12, 31):
                evento_especial_valor = "Fin de Año" if not current_event else current_event + "; Fin de Año"
                if not current_commercial: 
                    temporada_comercial_valor = "Temporada Navideña"

            # TEMPORADAS DE SALUD 2025
            if (date(2025, 3, 15) <= current_date <= date(2025, 5, 31)) or \
               (date(2025, 9, 15) <= current_date <= date(2025, 11, 30)):
                temporada_salud_valor = "Pico Enfermedades Respiratorias"
            if (date(2025, 4, 1) <= current_date <= date(2025, 5, 15)):
                if temporada_salud_valor: 
                    temporada_salud_valor += " / Alergias"
                else: 
                    temporada_salud_valor = "Temporada Alergias"
            if mes in [1, 2, 6, 7, 12] or escolar in ['Semana Santa', 'Vacaciones Mitad Año', 'Receso Escolar Octubre', 'Vacaciones Inicio Año', 'Vacaciones Fin Año']:
                 if temporada_salud_valor and "Protección Solar" not in temporada_salud_valor : 
                    temporada_salud_valor += " / Alta Demanda Protección Solar"
                 elif not temporada_salud_valor: 
                    temporada_salud_valor = "Alta Demanda Protección Solar"
        
        # Lógica para campañas de merchandising y e-commerce (ya existente)
        if not campanas_merchandising_df.empty:
            activas_merch = campanas_merchandising_df[
                (campanas_merchandising_df['fecha_inicio'] <= current_date) &
                (campanas_merchandising_df['fecha_fin'] >= current_date)
            ]
            if not activas_merch.empty:
                merchandising_valor = '; '.join(activas_merch['nombre_campana'].tolist())

        if not campanas_ecommerce_df.empty:
            activas_ecom = campanas_ecommerce_df[
                (campanas_ecommerce_df['fecha_inicio'] <= current_date) &
                (campanas_ecommerce_df['fecha_fin'] >= current_date)
            ]
            if not activas_ecom.empty:
                ecommerce_valor = '; '.join(activas_ecom['nombre_campana'].tolist())
        
        records.append({
            'fecha_key': fk, 'fecha': current_date, 'anio': anio, 'trimestre': trimestre,
            'mes': mes, 'mes_nombre': mes_nombre, 'dia': dia, 'dia_nombre': dia_nombre,
            'dia_semana': wd + 1, 'semana_anyo': semana_iso, 'dia_del_anyo': dia_anyo,
            'es_fin_de_semana': es_fin, 'es_habil': es_hab, 'es_feriado': es_feriado,
            'temporada_climatica': climatica, 'temporada_escolar': escolar,
            'temporada_comercial': temporada_comercial_valor,
            'temporada_salud': temporada_salud_valor,
            'evento_especial': evento_especial_valor,
            'merchandising': merchandising_valor,
            'e_commerce': ecommerce_valor
        })
        current_date += delta
    
    if records:
        stmt_insert_base = """
        INSERT INTO dim_fecha (
          fecha_key, fecha, anio, trimestre, mes, mes_nombre,
          dia, dia_nombre, dia_semana, semana_anyo, dia_del_anyo,
          es_fin_de_semana, es_habil, es_feriado,
          temporada_climatica, temporada_escolar, 
          temporada_comercial, temporada_salud, evento_especial, 
          merchandising, e_commerce
        )
        VALUES (
          :fecha_key, :fecha, :anio, :trimestre, :mes, :mes_nombre,
          :dia, :dia_nombre, :dia_semana, :semana_anyo, :dia_del_anyo,
          :es_fin_de_semana, :es_habil, :es_feriado,
          :temporada_climatica, :temporada_escolar, 
          :temporada_comercial, :temporada_salud, :evento_especial,
          :merchandising, :e_commerce
        )
        ON DUPLICATE KEY UPDATE
          fecha = VALUES(fecha), anio = VALUES(anio), trimestre = VALUES(trimestre),
          mes = VALUES(mes), mes_nombre = VALUES(mes_nombre), dia = VALUES(dia),
          dia_nombre = VALUES(dia_nombre), dia_semana = VALUES(dia_semana),
          semana_anyo = VALUES(semana_anyo), dia_del_anyo = VALUES(dia_del_anyo),
          es_fin_de_semana = VALUES(es_fin_de_semana), es_habil = VALUES(es_habil),
          es_feriado = VALUES(es_feriado), temporada_climatica = VALUES(temporada_climatica),
          temporada_escolar = VALUES(temporada_escolar), 
          temporada_comercial = VALUES(temporada_comercial), 
          temporada_salud = VALUES(temporada_salud),
          evento_especial = VALUES(evento_especial),
          merchandising = VALUES(merchandising),
          e_commerce = VALUES(e_commerce);
        """
        with engine.connect() as conn_insert:
            with conn_insert.begin():
                conn_insert.execute(text(stmt_insert_base), records)
        print(f"✅ dim_fecha poblada/actualizada. {len(records)} registros procesados para el rango de fechas.")
    else:
        print("No se generaron registros para insertar.")

if __name__ == "__main__":

    año_inicio = 2024
    año_fin = 2026 
    print(f"Iniciando creación/población de dim_fecha para los años {año_inicio} a {año_fin}...")
    create_and_populate_dim_fecha("gestion_compras", start_year=año_inicio, end_year=año_fin)

    # python -m analysis.loader.create_dim_fecha