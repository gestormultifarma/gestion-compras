# analysis/loader/create_view_maestra_integrada.py

from sqlalchemy import create_engine, text
from utils.db_connection import get_mysql_url

# Define la sentencia SQL para crear/reemplazar la vista integrada
INTEGRATED_VIEW_SQL = """
CREATE OR REPLACE VIEW vw_maestra_integrada AS
SELECT 
    e.codigo,
    MAX(e.nuevo_codigo) AS nuevo_codigo,
    MAX(e.codigo_barras) AS codigo_barras,
    MAX(e.nombre) AS nombre,
    MAX(e.laboratorio) AS laboratorio,
    MAX(e.principio_activo) AS principio_activo,
    MAX(e.marca) AS marca,
    MAX(e.departamento) AS departamento,
    MAX(e.categoria) AS categoria,
    MAX(e.subcategoria) AS subcategoria,
    MAX(e.pvp) AS pvp,
    MAX(p.costo_promedio) AS costo_promedio,
    MAX(p.grupo_i) AS grupo_i,
    MAX(p.grupo_ii) AS grupo_ii,
    MAX(p.grupo_iii) AS grupo_iii,
    MAX(p.grupo_iv) AS grupo_iv,
    MAX(p.grupo_v) AS grupo_v,
    MAX(p.grupo_vi) AS grupo_vi,
    MAX(p.unidad) AS unidad_medida,
    MAX(p.tipo_producto) AS tipo_producto,
    MAX(p.bonificacion) AS porcentaje_descuento,
    MAX(p.proveedor) AS proveedor,
    MAX(p.clasificacion) AS clasificacion,
    MAX(p.componente) AS componente,
    MAX(p.sustituto) AS sustituto,
    'activo' AS estado
FROM stg_maestra_ecommerce e
LEFT JOIN vw_maestra_pdv_consolidada p ON p.codigo = e.codigo
GROUP BY e.codigo;
"""

def create_integrated_view(db_name: str):
    """
    Conecta a la base de datos y crea o actualiza la vista vw_maestra_integrada.
    """
    engine = create_engine(get_mysql_url(db_name))
    try:
        with engine.begin() as conn:
            conn.execute(text(INTEGRATED_VIEW_SQL))
        print("✅ Vista vw_maestra_integrada creada/actualizada correctamente.")
    except Exception as e:
        print(f"❌ Error al crear la vista vw_maestra_integrada: {e}")
        raise

if __name__ == "__main__":
    create_integrated_view("gestion_compras")

# Prueba funcional: python -m analysis.loader.create_view_maestra_integrada