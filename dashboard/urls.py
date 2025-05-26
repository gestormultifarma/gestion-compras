# dashboard/urls.py

from django.urls import path
from . import views # Importa las vistas desde el archivo views.py de esta app

app_name = 'dashboard' # MUY IMPORTANTE para el namespace

urlpatterns = [
    path('', views.inicio, name='inicio'), # Esta será la raíz de la app dashboard
    path('logout/', views.cerrar_sesion, name='logout'),
    path('actualizar-archivos/', views.actualizar_archivos, name='actualizar_archivos'),
]