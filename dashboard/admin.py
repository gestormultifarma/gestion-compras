# dashboard/admin.py
from django.contrib import admin
from django.utils.html import format_html
from .models import (
    DimPdv, DimCampanaMerchandising, DimCampanaEcommerce,
    DimFecha, DimProducto, FactInventarios, FactRotacion
)

@admin.register(DimPdv)
class DimPdvAdmin(admin.ModelAdmin):
    list_display = (
        'codigo_pdv', 'nombre_pdv', 'tipo_pdv', 'formato_pdv', 'ciudad', 
        'estado_pdv', 'metros_cuadrados', 'metros_lineales_exhibicion', 'tamano_puerta', 
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
        ('Clasificación y Dimensiones Físicas', {
            'classes': ('collapse',),
            'fields': (
                'tipo_pdv', 'formato_pdv', 
                ('largo', 'ancho', 'altura'), 
                'metros_cuadrados', 
            ),
        }),
        ('Características de Exhibición y Acceso', {
            'classes': ('collapse',),
            'fields': (
                'vitrina_exhibicion', 'esquinero', 
                'metros_lineales_exhibicion',
                'tamano_puerta'
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
    date_hierarchy = 'fecha_inicio'

@admin.register(DimCampanaEcommerce)
class DimCampanaEcommerceAdmin(admin.ModelAdmin):
    list_display = ('nombre_campana', 'fecha_inicio', 'fecha_fin', 'fecha_actualizacion')
    search_fields = ('nombre_campana', 'descripcion_campana')
    list_filter = ('fecha_inicio', 'fecha_fin')
    ordering = ('-fecha_inicio',)
    date_hierarchy = 'fecha_inicio'

@admin.register(DimFecha)
class DimFechaAdmin(admin.ModelAdmin):
    list_display = ('fecha', 'anio', 'mes', 'dia', 'nombre_dia', 'nombre_mes', 'es_fin_semana', 'es_feriado')
    list_filter = ('anio', 'mes', 'es_fin_semana', 'es_feriado', 'trimestre', 'semestre')
    search_fields = ('fecha', 'nombre_dia', 'nombre_mes')
    ordering = ('-fecha',)
    date_hierarchy = 'fecha'
    readonly_fields = ('fecha_sk', 'fecha_creacion', 'fecha_actualizacion')

@admin.register(DimProducto)
class DimProductoAdmin(admin.ModelAdmin):
    list_display = ('codigo', 'nombre', 'categoria', 'marca', 'activo', 'fecha_actualizacion')
    list_filter = ('categoria', 'marca', 'activo', 'unidad_medida')
    search_fields = ('codigo', 'nombre', 'descripcion')
    list_select_related = True
    ordering = ('codigo',)
    readonly_fields = ('producto_sk', 'fecha_creacion', 'fecha_actualizacion')
    
    fieldsets = (
        (None, {
            'fields': ('codigo', 'nombre', 'descripcion', 'activo')
        }),
        ('Clasificación', {
            'classes': ('collapse',),
            'fields': ('categoria', 'subcategoria', 'marca')
        }),
        ('Unidades', {
            'classes': ('collapse',),
            'fields': ('unidad_medida', 'unidades_por_caja', 'unidades_por_blister')
        }),
        ('Ciclo de Vida', {
            'classes': ('collapse',),
            'fields': ('fecha_alta', 'fecha_baja')
        }),
        ('Auditoría', {
            'classes': ('collapse',),
            'fields': ('producto_sk', 'fecha_creacion', 'fecha_actualizacion')
        }),
    )

@admin.register(FactInventarios)
class FactInventariosAdmin(admin.ModelAdmin):
    list_display = ('pdv', 'fecha', 'cantidad_existencias', 'valor_existencias', 'nivel_stock')
    list_filter = ('fecha', 'nivel_stock', 'pdv__ciudad', 'pdv__tipo_pdv')
    search_fields = ('pdv__codigo_pdv', 'pdv__nombre_pdv')
    list_select_related = ('pdv',)
    date_hierarchy = 'fecha'
    ordering = ('-fecha', '-fecha_actualizacion')
    readonly_fields = ('inventario_sk', 'fecha_creacion', 'fecha_actualizacion')
    
    fieldsets = (
        (None, {
            'fields': ('pdv', 'fecha')
        }),
        ('Existencias', {
            'fields': (
                'cantidad_existencias', 
                'valor_existencias', 
                'costo_promedio'
            )
        }),
        ('Análisis de Rotación', {
            'classes': ('collapse',),
            'fields': (
                ('rotacion_dias', 'rotacion_semanas'),
                ('dias_stock', 'nivel_stock')
            )
        }),
        ('Auditoría', {
            'classes': ('collapse',),
            'fields': ('inventario_sk', 'fecha_creacion', 'fecha_actualizacion')
        }),
    )

@admin.register(FactRotacion)
class FactRotacionAdmin(admin.ModelAdmin):
    list_display = ('codigo_producto', 'codigo_pdv', 'fecha', 'venta_total', 'margen_porcentaje')
    list_filter = ('fecha', 'codigo_pdv')
    search_fields = ('codigo_producto', 'codigo_pdv')
    date_hierarchy = 'fecha'
    ordering = ('-fecha', 'codigo_pdv', 'codigo_producto')
    readonly_fields = ('fecha_carga', 'fecha_actualizacion')
    
    fieldsets = (
        (None, {
            'fields': ('producto', 'pdv', 'fecha_dim', 'fecha')
        }),
        ('Códigos', {
            'classes': ('collapse',),
            'fields': ('codigo_producto', 'codigo_pdv')
        }),
        ('Ventas', {
            'fields': (
                ('venta_unidades', 'venta_cajas', 'venta_blisters'),
                'venta_total'
            )
        }),
        ('Costos y Precios', {
            'classes': ('collapse',),
            'fields': (
                ('costo_unitario', 'precio_venta_unitario'),
                'costo_total',
                ('margen_bruto', 'margen_porcentaje')
            )
        }),
        ('Inventario', {
            'classes': ('collapse',),
            'fields': (
                ('inventario_unidades_inicial', 'inventario_unidades_final'),
                ('dias_inventario', 'rotacion_mes')
            )
        }),
        ('Auditoría', {
            'classes': ('collapse',),
            'fields': ('fecha_carga', 'fecha_actualizacion')
        }),
    )
