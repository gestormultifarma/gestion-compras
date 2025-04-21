from django.urls import path
from . import views

urlpatterns = [
    path('', views.inicio, name='inicio'),
    path('logout/', views.cerrar_sesion, name='logout'),
    path('actualizar-archivos/', views.actualizar_archivos, name='actualizar_archivos'),
]
