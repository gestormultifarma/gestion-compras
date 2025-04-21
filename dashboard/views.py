from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

@login_required
def inicio(request):
    return render(request, 'dashboard/inicio.html')

def cerrar_sesion(request):
    logout(request)
    return render(request, 'dashboard/logout.html')
