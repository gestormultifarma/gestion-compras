# dashboard/admin.py
from django.contrib import admin
from .models import DimPdv, DimCampanaMerchandising, DimCampanaEcommerce

@admin.register(DimPdv)
class DimPdvAdmin(admin.ModelAdmin):
    list_display = (
        'codigo_pdv', 'nombre_pdv', 'tipo_pdv', 'formato_pdv', 'ciudad', 
        'estado_pdv', 'metros_cuadrados', 'metros_lineales_exhibicion', 'tamano_puerta', # Añadidos a list_display
        'fecha_actualizacion'
    )
    list_filter = (
        'tipo_pdv', 'formato_pdv', 'ciudad', 'estado_pdv', 'region', 
        'esquinero', 'vitrina_exhibicion'
    )
    search_fields = ('codigo_pdv', 'nombre_pdv', 'direccion', 'barrio', 'ciudad')
    
    fieldsets = (
        (None, {
            'fields': ('codigo_pdv', 'nombre_pdv', 'estado_pdv')
        }),
        ('Clasificación y Dimensiones Físicas', { # Renombrado y agrupado
            'classes': ('collapse',),
            'fields': (
                'tipo_pdv', 'formato_pdv', 
                ('largo', 'ancho', 'altura'), 
                'metros_cuadrados', 
            ),
        }),
        ('Características de Exhibición y Acceso', { # Sección renombrada/ajustada
            'classes': ('collapse',),
            'fields': (
                'vitrina_exhibicion', 'esquinero', 
                'metros_lineales_exhibicion', # Nombre de campo actualizado
                'tamano_puerta' # Nombre de campo actualizado
            ),
        }),
        ('Ubicación Detallada', {
            'classes': ('collapse',),
            'fields': ('direccion', 'barrio', 'ciudad', 'departamento_geo', 'region'),
        }),
        ('Ciclo de Vida del PDV', {
            'classes': ('collapse',),
            'fields': ('fecha_apertura', 'fecha_cierre'),
        }),
        ('Auditoría', {
            'fields': ('pdv_sk', 'fecha_creacion', 'fecha_actualizacion'),
        }),
    )
    
    readonly_fields = ('pdv_sk', 'metros_cuadrados', 'fecha_creacion', 'fecha_actualizacion')

@admin.register(DimCampanaMerchandising)
class DimCampanaMerchandisingAdmin(admin.ModelAdmin):
    list_display = ('nombre_campana', 'fecha_inicio', 'fecha_fin', 'fecha_actualizacion')
    search_fields = ('nombre_campana', 'descripcion_campana')
    list_filter = ('fecha_inicio', 'fecha_fin')
    ordering = ('-fecha_inicio',)
    date_hierarchy = 'fecha_inicio' # Útil para navegar por fechas

@admin.register(DimCampanaEcommerce)
class DimCampanaEcommerceAdmin(admin.ModelAdmin):
    list_display = ('nombre_campana', 'fecha_inicio', 'fecha_fin', 'fecha_actualizacion')
    search_fields = ('nombre_campana', 'descripcion_campana')
    list_filter = ('fecha_inicio', 'fecha_fin')
    ordering = ('-fecha_inicio',)
    date_hierarchy = 'fecha_inicio'