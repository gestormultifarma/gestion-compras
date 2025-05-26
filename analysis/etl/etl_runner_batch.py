# analysis\etl\etl_runner_batch.py

import subprocess
import sys
from tqdm import tqdm

# Definimos los grupos de ETLs
etl_groups = {
    "todos": [
        "analysis.etl.etl_runner_bodega",
        "analysis.etl.etl_runner_convenios",
        "analysis.etl.etl_runner_correos_laboratorios",
        "analysis.etl.etl_runner_ecommerce",
        "analysis.etl.etl_runner_excluidos",
        "analysis.etl.etl_runner_exhibiciones",
        "analysis.etl.etl_runner_gerencia",
        "analysis.etl.etl_runner_inactivos",
        "analysis.etl.etl_runner_inventario",
        "analysis.etl.etl_runner_maestra_inventario",
        "analysis.etl.etl_runner_merchandising",
        "analysis.etl.etl_runner_mostrador",
        "analysis.etl.etl_runner_oferta",
        "analysis.etl.etl_runner_quincenales",
        "analysis.etl.etl_runner_semanales",
        "analysis.etl.etl_runner_temporales",
        "analysis.etl.etl_runner_transferencias",
        "analysis.etl.etl_runner_ventas",
    ],
    "diario": [
        "analysis.etl.etl_runner_ventas",
        "analysis.etl.etl_runner_inventario",
        "analysis.etl.etl_runner_bodega",
        "analysis.etl.etl_runner_mostrador",
        "analysis.etl.etl_runner_oferta",
    ],
    "semanal": [
        "analysis.etl.etl_runner_quincenales",
        "analysis.etl.etl_runner_semanales",
        "analysis.etl.etl_runner_transferencias",
    ],
    "mensual": [
        "analysis.etl.etl_runner_ecommerce",
        "analysis.etl.etl_runner_convenios",
    ],
    "ocasional": [
        "analysis.etl.etl_runner_exhibiciones",
        "analysis.etl.etl_runner_excluidos",
        "analysis.etl.etl_runner_gerencia",
        "analysis.etl.etl_runner_inactivos",
        "analysis.etl.etl_runner_correos_laboratorios",
        "analysis.etl.etl_runner_maestra_inventario",
        "analysis.etl.etl_runner_merchandising",
    ],
}

def correr_etls(modulos, grupo_nombre):
    print(f"\n🚀 Ejecutando grupo de ETLs: {grupo_nombre.upper()}")
    with tqdm(
        total=len(modulos),
        desc=f"ETLs {grupo_nombre}",
        unit="etl",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    ) as pbar:
        for etl in modulos:
            # Aviso previo
            tqdm.write(f"⚙️  Iniciando {etl} (este paso puede tardar)...")
            # Actualizar descripción de la barra
            pbar.set_description(f"Procesando: {etl.split('.')[-1]}")
            
            cmd = [sys.executable, "-u", "-m", etl]
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            # Leer la salida en vivo
            for linea in proc.stdout:
                tqdm.write(linea.rstrip())
            proc.wait()
            
            if proc.returncode != 0:
                tqdm.write(f"❌ Error ejecutando {etl} (exit code {proc.returncode})")
                break
            pbar.update(1)
        
        pbar.set_description(f"ETLs {grupo_nombre} completados")

def mostrar_menu():
    print("\n📋 Menú de ejecución de ETLs")
    print("0. Salir")
    print("1. Ejecutar TODOS los ETLs")
    print("2. Ejecutar ETLs DIARIOS")
    print("3. Ejecutar ETLs SEMANALES")
    print("4. Ejecutar ETLs MENSUALES")
    print("5. Ejecutar ETLs OCASIONALES")

def main():
    while True:
        mostrar_menu()
        opcion = input("\nSeleccione una opción: ").strip()
        if opcion == "0":
            print("\n👋 Saliendo del programa...")
            break
        elif opcion in {"1", "2", "3", "4", "5"}:
            mapping = {"1": "todos", "2": "diario", "3": "semanal", "4": "mensual", "5": "ocasional"}
            key = mapping[opcion]
            correr_etls(etl_groups[key], key)
        else:
            print("\n❌ Opción no válida. Intente nuevamente.")

if __name__ == "__main__":
    main()


# prueba funcional: python -m analysis.etl.etl_runner_batch
