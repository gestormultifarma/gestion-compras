from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.http import JsonResponse
from analysis.actualizador import procesar_archivos_excel  # TODO: Cambiar a la función correcta
from analysis.etl import procesar_archivo_excel
import os

@login_required
def inicio(request):
    return render(request, 'dashboard/inicio.html')

def cerrar_sesion(request):
    logout(request)
    return render(request, 'dashboard/logout.html')

@login_required
def actualizar_archivos(request):
    directorio_raiz = 'E:\\desarrollo\\gestionCompras\\data\\input'
    archivos_procesados = 0

    for carpeta, _, archivos in os.walk(directorio_raiz):
        for archivo in archivos:
            if archivo.endswith('.xlsx') or archivo.endswith('.xls'):
                ruta_archivo = os.path.join(carpeta, archivo)
                if procesar_archivo_excel(ruta_archivo):
                    archivos_procesados += 1

    return JsonResponse({'mensaje': f'{archivos_procesados} archivos procesados exitosamente.'})