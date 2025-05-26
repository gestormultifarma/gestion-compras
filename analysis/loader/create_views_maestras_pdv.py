# analysis\loader\create_views_maestras_pdv.py

from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

# 1) Define la sentencia SQL para crear/reemplazar la vista
VIEW_SQL = """
CREATE OR REPLACE VIEW vw_maestra_pdv_consolidada AS
SELECT
  t.codigo,
  ANY_VALUE(t.nombre)             AS nombre,
  AVG(t.costo)                    AS costo,
  AVG(t.costo_promedio)           AS costo_promedio,
  AVG(t.costo_descuento)          AS costo_descuento,
  AVG(t.costo_cargue)             AS costo_cargue,
  MAX(t.valor_caja_contado)       AS valor_caja_contado,
  ANY_VALUE(t.grupo_i)            AS grupo_i,
  ANY_VALUE(t.grupo_ii)           AS grupo_ii,
  ANY_VALUE(t.grupo_iii)          AS grupo_iii,
  ANY_VALUE(t.grupo_iv)           AS grupo_iv,
  ANY_VALUE(t.grupo_v)            AS grupo_v,
  ANY_VALUE(t.grupo_vi)           AS grupo_vi,
  ANY_VALUE(t.unidad)             AS unidad,
  ANY_VALUE(t.tipo_producto)      AS tipo_producto,
  MAX(t.bonificacion)             AS bonificacion,
  ANY_VALUE(t.proveedor)          AS proveedor,
  ANY_VALUE(t.clasificacion)      AS clasificacion,
  ANY_VALUE(t.componente)         AS componente,
  ANY_VALUE(t.sustituto)          AS sustituto
FROM (
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
  FROM stg_maestra_pdv_bella_suiza
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_bochalema
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_calicanto
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_calle_quinta
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_carmesi
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_ciudadela
  UNION ALL
   SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_cosmocentro_1
  UNION ALL 
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_cosmocentro_2
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_cosmocentro_3
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_global
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_holguines
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_ingenio
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_metro
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_minimarket
  UNION ALL
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_podium
  UNION ALL  
  SELECT codigo, nombre, costo, costo_promedio, costo_descuento, costo_cargue,
         valor_caja_contado, grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
         unidad, tipo_producto, bonificacion, proveedor, clasificacion, componente, sustituto
    FROM stg_maestra_pdv_unicentro_1
) AS t
GROUP BY t.codigo;
"""

# TODO: completar el SQL para cada tabla stg_maestra_pdv_*

def create_pdv_consolidated_view(db_name: str):
    """
    Conecta a la base de datos y crea o actualiza la vista vw_maestra_pdv_consolidada.
    """
    engine = create_engine(get_mysql_url(db_name))
    with engine.begin() as conn:
        conn.execute(text(VIEW_SQL))
    print("✅ Vista vw_maestra_pdv_consolidada creada/actualizada correctamente.")

if __name__ == "__main__":
    # Ajusta el nombre de la base de datos si tu esquema se llama distinto
    create_pdv_consolidated_view("gestion_compras")

# Prueba funcional: python -m analysis.loader.create_views_maestras_pdv

