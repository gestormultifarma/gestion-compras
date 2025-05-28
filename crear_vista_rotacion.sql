-- crear_vista_rotacion.sql

-- Primero eliminamos la vista si ya existe
DROP VIEW IF EXISTS vw_rotacion_historica;

-- Creamos la vista para el análisis de rotación histórica
CREATE VIEW vw_rotacion_historica AS
WITH 
-- Extraemos los datos base (productos únicos por PDV)
productos_pdv AS (
    SELECT DISTINCT 
        f.codigo_producto,
        p.nombre as nombre_producto,
        f.codigo_pdv,
        pdv.nombre_pdv,
        f.producto_sk,
        f.pdv_sk
    FROM fact_rotacion f
    JOIN dim_producto p ON f.producto_sk = p.producto_sk
    JOIN dim_pdv pdv ON f.pdv_sk = pdv.pdv_sk
),
-- Obtenemos la rotación actual 
rotacion_actual AS (
    SELECT 
        codigo_producto,
        codigo_pdv,
        producto_sk,
        pdv_sk,
        ROUND(AVG(rotacion_mes), 2) as rotacion_actual
    FROM fact_rotacion
    GROUP BY codigo_producto, codigo_pdv, producto_sk, pdv_sk
),
-- Simulamos los datos de meses anteriores (para demostración)
-- En un sistema real, estos datos vendrían de registros históricos
rotacion_m1 AS (
    -- Mes anterior (con una variación aleatoria del -10% a +10%)
    SELECT 
        codigo_producto,
        codigo_pdv,
        producto_sk,
        pdv_sk,
        ROUND(rotacion_actual * (0.9 + (RAND() * 0.2)), 2) as rotacion_m1
    FROM rotacion_actual
),
rotacion_m2 AS (
    -- Dos meses atrás (con una variación adicional)
    SELECT 
        codigo_producto,
        codigo_pdv,
        producto_sk,
        pdv_sk,
        ROUND(rotacion_m1 * (0.85 + (RAND() * 0.3)), 2) as rotacion_m2
    FROM rotacion_m1
),
rotacion_m3 AS (
    -- Tres meses atrás
    SELECT 
        codigo_producto,
        codigo_pdv,
        producto_sk,
        pdv_sk,
        ROUND(rotacion_m2 * (0.8 + (RAND() * 0.4)), 2) as rotacion_m3
    FROM rotacion_m2
)
-- Combinamos todos los datos en la vista final
SELECT 
    pp.codigo_producto,
    pp.nombre_producto,
    pp.codigo_pdv,
    pp.nombre_pdv,
    COALESCE(ra.rotacion_actual, 0) as rotacion_actual,
    COALESCE(r1.rotacion_m1, 0) as rotacion_m1,
    COALESCE(r2.rotacion_m2, 0) as rotacion_m2,
    COALESCE(r3.rotacion_m3, 0) as rotacion_m3,
    -- Calculamos la tendencia (cambio porcentual entre M-3 y actual)
    CASE 
        WHEN COALESCE(r3.rotacion_m3, 0) = 0 THEN 0
        ELSE ROUND(((COALESCE(ra.rotacion_actual, 0) - COALESCE(r3.rotacion_m3, 0)) / COALESCE(r3.rotacion_m3, 1)) * 100, 2)
    END as tendencia_porcentaje
FROM productos_pdv pp
LEFT JOIN rotacion_actual ra ON pp.producto_sk = ra.producto_sk AND pp.pdv_sk = ra.pdv_sk
LEFT JOIN rotacion_m1 r1 ON pp.producto_sk = r1.producto_sk AND pp.pdv_sk = r1.pdv_sk
LEFT JOIN rotacion_m2 r2 ON pp.producto_sk = r2.producto_sk AND pp.pdv_sk = r2.pdv_sk
LEFT JOIN rotacion_m3 r3 ON pp.producto_sk = r3.producto_sk AND pp.pdv_sk = r3.pdv_sk;

-- Vista resumida por PDV para análisis gerencial
DROP VIEW IF EXISTS vw_rotacion_pdv;

CREATE VIEW vw_rotacion_pdv AS
SELECT 
    nombre_pdv,
    codigo_pdv,
    COUNT(*) as total_productos,
    ROUND(AVG(rotacion_actual), 2) as rotacion_actual_prom,
    ROUND(AVG(rotacion_m1), 2) as rotacion_m1_prom,
    ROUND(AVG(rotacion_m2), 2) as rotacion_m2_prom,
    ROUND(AVG(rotacion_m3), 2) as rotacion_m3_prom,
    ROUND(AVG(tendencia_porcentaje), 2) as tendencia_prom
FROM vw_rotacion_historica
GROUP BY nombre_pdv, codigo_pdv
ORDER BY tendencia_prom DESC;
