# analysis/loader/loader_dim_producto.py (versión actualizada)

from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

"""
Este loader implementa SCD-2 optimizado evitando largos CTE en una sola transacción.
Se generan tablas temporales para "integr" y "changed", luego se ejecutan
UPDATE e INSERT en bloques separados para reducir bloqueos.
"""

def load_dim_producto(db_name: str):
    """
    Ejecuta SCD-2 de dim_producto en MySQL con tablas temporales para minimizar bloqueos.
    """
    engine = create_engine(get_mysql_url(db_name))
    with engine.begin() as conn:
        # 1) Crear tabla temporal con valores normalizados de la vista integrada
        conn.execute(text("""
            CREATE TEMPORARY TABLE tmp_integr AS
            SELECT
              codigo,
              TRIM(LOWER(nuevo_codigo))        AS nuevo_codigo,
              TRIM(LOWER(codigo_barras))       AS codigo_barras,
              TRIM(LOWER(nombre))              AS nombre,
              TRIM(LOWER(laboratorio))         AS laboratorio,
              TRIM(LOWER(principio_activo))    AS principio_activo,
              TRIM(LOWER(marca))               AS marca,
              TRIM(LOWER(departamento))        AS departamento,
              TRIM(LOWER(categoria))           AS categoria,
              TRIM(LOWER(subcategoria))        AS subcategoria,
              ROUND(pvp, 2)                    AS pvp,
              ROUND(costo_promedio, 4)         AS costo_promedio,
              TRIM(LOWER(grupo_i))             AS grupo_i,
              TRIM(LOWER(grupo_ii))            AS grupo_ii,
              TRIM(LOWER(grupo_iii))           AS grupo_iii,
              TRIM(LOWER(grupo_iv))            AS grupo_iv,
              TRIM(LOWER(grupo_v))             AS grupo_v,
              TRIM(LOWER(grupo_vi))            AS grupo_vi,
              TRIM(LOWER(unidad_medida))       AS unidad_medida,
              TRIM(LOWER(tipo_producto))       AS tipo_producto,
              porcentaje_descuento,
              TRIM(LOWER(proveedor))           AS proveedor,
              TRIM(LOWER(clasificacion))       AS clasificacion,
              TRIM(LOWER(componente))          AS componente,
              TRIM(LOWER(sustituto))           AS sustituto,
              TRIM(LOWER(estado))              AS estado
            FROM vw_maestra_integrada;
        """))

        # 2) Crear tabla temporal con códigos que cambiaron
        conn.execute(text("""
            CREATE TEMPORARY TABLE tmp_changed AS
            SELECT i.codigo
            FROM tmp_integr i
            JOIN dim_producto d ON d.codigo = i.codigo AND d.flag_actual = TRUE
            WHERE
              NOT (d.nuevo_codigo    <=> i.nuevo_codigo)
            OR NOT (d.codigo_barras <=> i.codigo_barras)
            OR NOT (d.nombre        <=> i.nombre)
            OR NOT (d.laboratorio   <=> i.laboratorio)
            OR NOT (d.principio_activo <=> i.principio_activo)
            OR NOT (d.marca         <=> i.marca)
            OR NOT (d.departamento  <=> i.departamento)
            OR NOT (d.categoria     <=> i.categoria)
            OR NOT (d.subcategoria  <=> i.subcategoria)
            OR NOT (d.pvp           <=> i.pvp)
            OR NOT (d.costo_promedio <=> i.costo_promedio)
            OR NOT (d.grupo_i       <=> i.grupo_i)
            OR NOT (d.grupo_ii      <=> i.grupo_ii)
            OR NOT (d.grupo_iii     <=> i.grupo_iii)
            OR NOT (d.grupo_iv      <=> i.grupo_iv)
            OR NOT (d.grupo_v       <=> i.grupo_v)
            OR NOT (d.grupo_vi      <=> i.grupo_vi)
            OR NOT (d.unidad_medida <=> i.unidad_medida)
            OR NOT (d.tipo_producto <=> i.tipo_producto)
            OR NOT (d.porcentaje_descuento <=> i.porcentaje_descuento)
            OR NOT (d.proveedor    <=> i.proveedor)
            OR NOT (d.clasificacion <=> i.clasificacion)
            OR NOT (d.componente   <=> i.componente)
            OR NOT (d.sustituto    <=> i.sustituto)
            OR NOT (d.estado       <=> i.estado);
        """))

        # 3) Expirar versiones antiguas solo para códigos cambiados
        expire_result = conn.execute(text("""
            UPDATE dim_producto d
            JOIN tmp_changed c ON d.codigo = c.codigo
            SET d.flag_actual = FALSE,
                d.fecha_fin    = CURDATE();
        """))
        print(f"✅ Registros expirados: {expire_result.rowcount}")

        # 4) Insertar nuevas versiones solo para códigos cambiados
        insert_result = conn.execute(text("""
            INSERT INTO dim_producto (
              codigo, nuevo_codigo, codigo_barras, nombre,
              laboratorio, principio_activo, marca, departamento,
              categoria, subcategoria, pvp, costo_promedio,
              grupo_i, grupo_ii, grupo_iii, grupo_iv, grupo_v, grupo_vi,
              unidad_medida, tipo_producto, porcentaje_descuento,
              proveedor, clasificacion, componente, sustituto,
              estado, fecha_inicio, flag_actual, fecha_actualizacion
            )
            SELECT
              i.codigo,
              i.nuevo_codigo,
              i.codigo_barras,
              i.nombre,
              i.laboratorio,
              i.principio_activo,
              i.marca,
              i.departamento,
              i.categoria,
              i.subcategoria,
              i.pvp,
              i.costo_promedio,
              i.grupo_i, i.grupo_ii, i.grupo_iii, i.grupo_iv, i.grupo_v, i.grupo_vi,
              i.unidad_medida,
              i.tipo_producto,
              i.porcentaje_descuento,
              i.proveedor,
              i.clasificacion,
              i.componente,
              i.sustituto,
              i.estado,
              CURDATE(),
              TRUE,
              CURRENT_TIMESTAMP
            FROM tmp_integr i
            JOIN tmp_changed c ON i.codigo = c.codigo;
        """))
        print(f"✅ Registros insertados: {insert_result.rowcount}")

    print("✅ dim_producto actualizado correctamente.")

if __name__ == "__main__":
    load_dim_producto("gestion_compras")


# Prueba funcional: python -m analysis.loader.loader_dim_producto