# web/urls.py

from django.contrib import admin
from django.urls import path, include # Asegúrate de que include esté importado
from django.contrib.auth import views as auth_views # Para la vista de login de Django

urlpatterns = [
    path('admin/', admin.site.urls),

    # URL de Login:
    # Usamos la vista LoginView incorporada de Django, pero le decimos que use tu plantilla.
    path(
        'login/',
        auth_views.LoginView.as_view(template_name='dashboard/login.html'),
        name='login' # Este es el nombre que usa LOGIN_URL = 'login' en settings.py
    ),

    # Incluir las URLs de la aplicación 'dashboard':
    # Todas las URLs definidas en dashboard/urls.py ahora estarán disponibles.
    # Como la primera ruta en dashboard.urls es '', la URL '' (raíz del sitio)
    # apuntará a dashboard.views.inicio.
    path('', include('dashboard.urls')),

    # NO intentes definir aquí paths usando vistas de 'dashboard' directamente,
    # para eso está el include.
    # Las siguientes líneas que tenías antes aquí (si las tenías como en tu ejemplo)
    # deben estar en dashboard/urls.py, no aquí:
    # path('', views.inicio, name='inicio'), <-- Esto ya se maneja por el include
    # path('logout/', views.cerrar_sesion, name='logout'), <-- Esto ya se maneja por el include
    # path('actualizar-archivos/', views.actualizar_archivos, name='actualizar_archivos'), <-- Ya se maneja por el include
]