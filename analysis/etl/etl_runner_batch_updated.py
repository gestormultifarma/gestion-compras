# analysis/etl/etl_runner_batch_updated.py

import os
import sys
import argparse
import warnings
import concurrent.futures
from datetime import datetime

# Importaciones de los ETL runners existentes
from analysis.etl.etl_runner_ventas import run_ventas_etl
from analysis.etl.etl_runner_inventario import run_inventario_etl
from analysis.etl.etl_runner_bodega import run_bodega_etl
from analysis.etl.etl_runner_mostrador import run_mostrador_etl
from analysis.etl.etl_runner_oferta import run_oferta_etl
from analysis.etl.etl_runner_ecommerce import run_ecommerce_etl
from analysis.etl.etl_runner_convenios import run_convenios_etl
from analysis.etl.etl_runner_merchandising import run_merchandising_etl

# Importaciones de los nuevos ETL runners de tablas de hechos
from analysis.etl.etl_runner_fact_inventarios import FactInventariosETLRunner
from analysis.etl.etl_runner_fact_rotacion import FactRotacionETLRunner

from utils.logger_etl import LoggerETL

# Suprimir advertencias de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

# Diccionario de ETLs disponibles
ETLS = {
    # ETLs de fuentes primarias (stage tables)
    'ventas': run_ventas_etl,
    'inventario': run_inventario_etl, 
    'bodega': run_bodega_etl,
    'mostrador': run_mostrador_etl,
    'oferta': run_oferta_etl,
    'ecommerce': run_ecommerce_etl,
    'convenios': run_convenios_etl,
    'merchandising': run_merchandising_etl,
    
    # Nuevos ETLs para tablas de hechos
    'fact_inventarios': lambda: FactInventariosETLRunner().run(),
    'fact_rotacion': lambda: FactRotacionETLRunner().run()
}

# Grupos de ETLs para ejecución en conjunto
ETL_GROUPS = {
    'diarios': ['ventas', 'inventario', 'bodega', 'mostrador', 'oferta'],
    'semanales': ['ecommerce', 'convenios'],
    'mensuales': ['merchandising'],
    'fact_tables': ['fact_inventarios', 'fact_rotacion'],
    'all': list(ETLS.keys())
}

# ETLs que dependen de otros
ETL_DEPENDENCIES = {
    'fact_inventarios': ['inventario'],
    'fact_rotacion': ['ventas', 'inventario']
}

def run_etl_with_dependencies(etl_name, executed_etls, logger):
    """
    Ejecuta un ETL asegurándose de que sus dependencias se hayan ejecutado primero.
    
    Args:
        etl_name (str): Nombre del ETL a ejecutar
        executed_etls (set): Conjunto de ETLs ya ejecutados
        logger (LoggerETL): Logger para registrar eventos
        
    Returns:
        bool: True si la ejecución fue exitosa, False en caso contrario
    """
    # Verificar si el ETL ya fue ejecutado
    if etl_name in executed_etls:
        logger.info(f"ℹ️ ETL '{etl_name}' ya fue ejecutado en esta sesión")
        return True
    
    # Verificar dependencias
    if etl_name in ETL_DEPENDENCIES:
        for dep in ETL_DEPENDENCIES[etl_name]:
            if dep not in executed_etls:
                logger.info(f"🔄 Ejecutando dependencia '{dep}' para '{etl_name}'")
                success = run_etl_with_dependencies(dep, executed_etls, logger)
                if not success:
                    logger.error(f"❌ Falló la dependencia '{dep}' para '{etl_name}'")
                    return False
    
    # Ejecutar el ETL
    logger.info(f"🚀 Iniciando ETL '{etl_name}'")
    try:
        result = ETLS[etl_name]()
        if result:
            logger.info(f"✅ ETL '{etl_name}' completado con éxito")
            executed_etls.add(etl_name)
            return True
        else:
            logger.error(f"❌ ETL '{etl_name}' falló")
            return False
    except Exception as e:
        logger.error(f"💥 Error en ETL '{etl_name}': {e}")
        return False

def run_batch(etl_names, parallel=False, max_workers=3):
    """
    Ejecuta un lote de ETLs, opcionalmente en paralelo.
    
    Args:
        etl_names (list): Lista de nombres de ETLs a ejecutar
        parallel (bool): Si es True, ejecuta los ETLs en paralelo
        max_workers (int): Número máximo de workers para ejecución paralela
        
    Returns:
        tuple: (éxitos, fallos) conteo de ETLs exitosos y fallidos
    """
    logger = LoggerETL("ETL Batch Runner")
    logger.info(f"🏁 Iniciando ejecución por lotes de {len(etl_names)} ETLs")
    
    # Resolver grupos de ETLs
    etls_to_run = []
    for name in etl_names:
        if name in ETL_GROUPS:
            etls_to_run.extend(ETL_GROUPS[name])
        elif name in ETLS:
            etls_to_run.append(name)
        else:
            logger.warning(f"⚠️ ETL desconocido: '{name}'")
    
    # Eliminar duplicados manteniendo el orden
    etls_to_run = list(dict.fromkeys(etls_to_run))
    logger.info(f"📋 ETLs a ejecutar: {etls_to_run}")
    
    executed_etls = set()
    success_count = 0
    failure_count = 0
    
    if parallel:
        # Ejecución en paralelo (solo para ETLs sin dependencias entre sí)
        independent_etls = [etl for etl in etls_to_run if etl not in ETL_DEPENDENCIES]
        dependent_etls = [etl for etl in etls_to_run if etl in ETL_DEPENDENCIES]
        
        logger.info(f"⚡ Ejecutando {len(independent_etls)} ETLs en paralelo")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_etl = {
                executor.submit(run_etl_with_dependencies, etl, executed_etls, logger): etl 
                for etl in independent_etls
            }
            
            for future in concurrent.futures.as_completed(future_to_etl):
                etl = future_to_etl[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception as e:
                    logger.error(f"💥 Excepción en ETL '{etl}': {e}")
                    failure_count += 1
        
        # Ejecutar ETLs con dependencias en secuencia
        logger.info(f"🔄 Ejecutando {len(dependent_etls)} ETLs con dependencias en secuencia")
        for etl in dependent_etls:
            if run_etl_with_dependencies(etl, executed_etls, logger):
                success_count += 1
            else:
                failure_count += 1
    else:
        # Ejecución secuencial
        logger.info(f"⏱️ Ejecutando {len(etls_to_run)} ETLs en secuencia")
        for etl in etls_to_run:
            if run_etl_with_dependencies(etl, executed_etls, logger):
                success_count += 1
            else:
                failure_count += 1
    
    logger.info(f"🏁 Ejecución por lotes completada. Éxitos: {success_count}, Fallos: {failure_count}")
    return success_count, failure_count

def main():
    parser = argparse.ArgumentParser(description='Ejecuta ETLs en lote')
    parser.add_argument('etls', nargs='*', 
                        help='ETLs a ejecutar. Puede ser nombres individuales o grupos. Si no se especifica, se ejecutan todos.')
    parser.add_argument('--parallel', action='store_true',
                        help='Ejecutar ETLs en paralelo cuando sea posible')
    parser.add_argument('--workers', type=int, default=3,
                        help='Número máximo de workers para ejecución paralela')
    parser.add_argument('--list', action='store_true',
                        help='Listar todos los ETLs disponibles y salir')
    
    args = parser.parse_args()
    
    if args.list:
        print("📋 ETLs individuales disponibles:")
        for name in sorted(ETLS.keys()):
            print(f"  - {name}")
        
        print("\n📦 Grupos de ETLs disponibles:")
        for group, etls in sorted(ETL_GROUPS.items()):
            print(f"  - {group}: {', '.join(etls)}")
        
        print("\n🔄 Dependencias de ETLs:")
        for etl, deps in sorted(ETL_DEPENDENCIES.items()):
            print(f"  - {etl} depende de: {', '.join(deps)}")
            
        return
    
    etls_to_run = args.etls if args.etls else ['all']
    success, failures = run_batch(etls_to_run, args.parallel, args.workers)
    
    if failures > 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
