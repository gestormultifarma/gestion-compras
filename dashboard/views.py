from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.http import JsonResponse
import os


@login_required
def inicio(request):
    return render(request, 'dashboard/inicio.html')

def cerrar_sesion(request):
    logout(request)
    return render(request, 'dashboard/logout.html')

@login_required
def actualizar_archivos(request):
    directorio_raiz = 'E:\desarrollo\gestionCompras\data\input' # directorio fuente de los archivos
    archivos_procesados = []

    for carpeta, _, archivos in os.walk(directorio_raiz):
        for archivo in archivos:
            if archivo.endswith('.xlsx') or archivo.endswith('.xls'):
                ruta_archivo = os.path.join(carpeta, archivo)
                # Aquí se incluirá la lógica de transformación y carga a DB
                archivos_procesados.append(ruta_archivo)

    return JsonResponse({'mensaje': f'{len(archivos_procesados)} archivos procesados exitosamente.'})