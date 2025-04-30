import subprocess
import sys

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
    for etl in modulos:
        print(f"\n▶️  Ejecutando: {etl}")
        resultado = subprocess.run([sys.executable, "-m", etl], capture_output=True, text=True)
        print(resultado.stdout)
        if resultado.returncode != 0:
            print(f"❌ Error ejecutando {etl}")
            print(resultado.stderr)
            break  # Detener en el primer error si ocurre

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
        elif opcion == "1":
            correr_etls(etl_groups["todos"], "todos")
        elif opcion == "2":
            correr_etls(etl_groups["diario"], "diario")
        elif opcion == "3":
            correr_etls(etl_groups["semanal"], "semanal")
        elif opcion == "4":
            correr_etls(etl_groups["mensual"], "mensual")
        elif opcion == "5":
            correr_etls(etl_groups["ocasional"], "ocasional")
        else:
            print("\n❌ Opción no válida. Intente nuevamente.")

if __name__ == "__main__":
    main()
