# dashboard/models.py
from django.db import models
from django.utils import timezone
from django.core.validators import MinValueValidator, MaxValueValidator

# Tabla de dimensiones para Fechas
class DimFecha(models.Model):
    fecha_sk = models.AutoField(primary_key=True, verbose_name="ID Fecha")
    fecha = models.DateField(unique=True, verbose_name="Fecha")
    
    # Campos derivados
    dia = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(31)])
    mes = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(12)])
    anio = models.PositiveSmallIntegerField()
    trimestre = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(4)])
    semestre = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(2)])
    dia_semana = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(7)])
    nombre_dia = models.CharField(max_length=10)
    nombre_mes = models.CharField(max_length=10)
    es_fin_semana = models.BooleanField(default=False)
    es_feriado = models.BooleanField(default=False)
    
    # Auditoría
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.fecha.strftime('%Y-%m-%d')

    class Meta:
        db_table = 'dim_fecha'
        verbose_name = "Dimensión Fecha"
        verbose_name_plural = "Dimensiones de Fechas"
        ordering = ['fecha']
        indexes = [
            models.Index(fields=['fecha']),
            models.Index(fields=['anio', 'mes']),
            models.Index(fields=['es_fin_semana']),
            models.Index(fields=['es_feriado']),
        ]

# Tabla de dimensiones para Productos
class DimProducto(models.Model):
    producto_sk = models.AutoField(primary_key=True, verbose_name="ID Producto")
    
    # Información básica
    codigo = models.CharField(max_length=50, unique=True, verbose_name="Código de Producto")
    nombre = models.CharField(max_length=200, verbose_name="Nombre del Producto")
    descripcion = models.TextField(null=True, blank=True, verbose_name="Descripción")
    
    # Categorización
    categoria = models.CharField(max_length=100, null=True, blank=True, verbose_name="Categoría")
    subcategoria = models.CharField(max_length=100, null=True, blank=True, verbose_name="Subcategoría")
    marca = models.CharField(max_length=100, null=True, blank=True, verbose_name="Marca")
    
    # Unidades y empaque
    unidad_medida = models.CharField(max_length=20, verbose_name="Unidad de Medida")
    unidades_por_caja = models.PositiveIntegerField(default=1, verbose_name="Unidades por Caja")
    unidades_por_blister = models.PositiveIntegerField(null=True, blank=True, verbose_name="Unidades por Blister")
    
    # Estado
    activo = models.BooleanField(default=True, verbose_name="¿Activo?")
    fecha_alta = models.DateField(auto_now_add=True, verbose_name="Fecha de Alta")
    fecha_baja = models.DateField(null=True, blank=True, verbose_name="Fecha de Baja")
    
    # Auditoría
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.codigo} - {self.nombre}"

    class Meta:
        db_table = 'dim_producto'
        verbose_name = "Producto"
        verbose_name_plural = "Productos"
        ordering = ['codigo']
        indexes = [
            models.Index(fields=['codigo']),
            models.Index(fields=['nombre']),
            models.Index(fields=['categoria']),
            models.Index(fields=['marca']),
            models.Index(fields=['activo']),
        ]

class DimPdv(models.Model):
    pdv_sk = models.AutoField(primary_key=True)
    codigo_pdv = models.CharField(max_length=100, unique=True, verbose_name="Código PDV")
    nombre_pdv = models.CharField(max_length=255, verbose_name="Nombre PDV")

    # --- tipo_pdv con choices ---
    TIPO_PDV_BARRIO = 'barrio'
    TIPO_PDV_CCOMERCIAL = 'c_comercial'
    TIPO_PDV_TIENDA_OCULTA = 'tienda_oculta'
    TIPO_PDV_CHOICES = [
        (TIPO_PDV_BARRIO, 'Barrio'),
        (TIPO_PDV_CCOMERCIAL, 'Centro Comercial'),
        (TIPO_PDV_TIENDA_OCULTA, 'Tienda Oculta (Bodega/Ecommerce)'),
    ]
    tipo_pdv = models.CharField(
        max_length=20, # Ajustado para los códigos
        choices=TIPO_PDV_CHOICES,
        null=True, blank=True,
        verbose_name="Tipo de PDV"
    )

    # --- formato_pdv con choices ---
    FORMATO_PDV_TRADICIONAL = 'tradicional'
    FORMATO_PDV_MINIMARKET = 'minimarket'
    FORMATO_PDV_ECOMMERCE = 'e_commerce'
    FORMATO_PDV_CHOICES = [
        (FORMATO_PDV_TRADICIONAL, 'Tradicional'),
        (FORMATO_PDV_MINIMARKET, 'Minimarket'),
        (FORMATO_PDV_ECOMMERCE, 'E-commerce'),
    ]
    formato_pdv = models.CharField(
        max_length=20, # Ajustado para los códigos
        choices=FORMATO_PDV_CHOICES,
        null=True, blank=True,
        verbose_name="Formato PDV"
    )

    direccion = models.CharField(max_length=500, null=True, blank=True, verbose_name="Dirección")
    barrio = models.CharField(max_length=255, null=True, blank=True)
    ciudad = models.CharField(max_length=100, null=True, blank=True)
    departamento_geo = models.CharField(max_length=100, null=True, blank=True, verbose_name="Departamento Geográfico")
    region = models.CharField(max_length=100, null=True, blank=True)
    fecha_apertura = models.DateField(null=True, blank=True, verbose_name="Fecha Apertura")
    fecha_cierre = models.DateField(null=True, blank=True, verbose_name="Fecha Cierre")
    
    ESTADO_CHOICES = [
        ('activo', 'Activo'),
        ('inactivo', 'Inactivo'),
        ('remodelacion', 'En Remodelación'),
    ]
    estado_pdv = models.CharField(
        max_length=20,
        choices=ESTADO_CHOICES,
        default='activo',
        verbose_name="Estado del PDV"
    )

    # --- Nuevas columnas de características (Booleanos como NullBooleanField o IntegerField con choices 0/1) ---
    # Usaremos IntegerField con choices para más claridad en el admin y BD, o NullBooleanField si prefieres True/False/None
    # Opción con IntegerField (0=No, 1=Sí, NULL=No Aplica/Desconocido)
    CHOICES_SI_NO_NULL = [ (None, '---'), (0, 'No'), (1, 'Sí') ]

    vitrina_exhibicion = models.IntegerField(choices=CHOICES_SI_NO_NULL, null=True, blank=True, verbose_name="¿Tiene Vitrina de Exhibición?")
    esquinero = models.IntegerField(choices=CHOICES_SI_NO_NULL, null=True, blank=True, verbose_name="¿Es Esquinero?")
    metros_lineales_exhibicion = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Metros Lineales de Exhibición"
    )
    tamano_puerta = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True, verbose_name="Tamaño de Puerta (mts)"
    )

    # --- Nuevas columnas de dimensiones físicas ---
    largo = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Largo (mts)")
    ancho = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Ancho (mts)")
    altura = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Altura (mts)")

    # --- metros_cuadrados (calculado y almacenado) ---
    metros_cuadrados = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True, 
        verbose_name="Metros Cuadrados (calculado)",
        editable=False # Hacemos que no sea editable directamente en el admin, se calcula
    )

    fecha_creacion = models.DateTimeField(auto_now_add=True, verbose_name="Fecha Creación")
    fecha_actualizacion = models.DateTimeField(auto_now=True, verbose_name="Última Actualización")

    def save(self, *args, **kwargs):
        # Calcular metros_cuadrados antes de guardar
        if self.largo is not None and self.ancho is not None:
            self.metros_cuadrados = self.largo * self.ancho
        else:
            self.metros_cuadrados = None # O 0 si prefieres
        super().save(*args, **kwargs) # Llama al método save original

    def __str__(self):
        return f"{self.nombre_pdv} ({self.codigo_pdv})"

    class Meta:
        db_table = 'dim_pdv'
        verbose_name = "Punto de Venta (PDV)"
        verbose_name_plural = "Puntos de Venta (PDV)"
        ordering = ['nombre_pdv']

# dashboard/models.py
# ... (tus imports y DimPdv existentes) ...

class DimCampanaMerchandising(models.Model):
    campana_merchandising_sk = models.AutoField(primary_key=True)
    nombre_campana = models.CharField(max_length=150, verbose_name="Nombre Campaña Merchandising") # Un poco más de espacio
    descripcion_campana = models.TextField(null=True, blank=True, verbose_name="Descripción")
    fecha_inicio = models.DateField(verbose_name="Fecha de Inicio")
    fecha_fin = models.DateField(verbose_name="Fecha de Fin")
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.nombre_campana} ({self.fecha_inicio.strftime('%Y-%m-%d')} - {self.fecha_fin.strftime('%Y-%m-%d')})"

    class Meta:
        db_table = 'dim_campana_merchandising' # Especifica el nombre de la tabla
        verbose_name = "Campaña de Merchandising"
        verbose_name_plural = "Campañas de Merchandising"
        ordering = ['-fecha_inicio', 'nombre_campana']
        constraints = [
            models.CheckConstraint(check=models.Q(fecha_fin__gte=models.F('fecha_inicio')), name='chk_fecha_fin_merch_gte_inicio')
        ]

class DimCampanaEcommerce(models.Model):
    campana_ecommerce_sk = models.AutoField(primary_key=True)
    nombre_campana = models.CharField(max_length=150, verbose_name="Nombre Campaña E-commerce") # Un poco más de espacio
    descripcion_campana = models.TextField(null=True, blank=True, verbose_name="Descripción")
    fecha_inicio = models.DateField(verbose_name="Fecha de Inicio")
    fecha_fin = models.DateField(verbose_name="Fecha de Fin")
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.nombre_campana} ({self.fecha_inicio.strftime('%Y-%m-%d')} - {self.fecha_fin.strftime('%Y-%m-%d')})"

    class Meta:
        db_table = 'dim_campana_ecommerce' # Especifica el nombre de la tabla
        verbose_name = "Campaña de E-commerce"
        verbose_name_plural = "Campañas de E-commerce"
        ordering = ['-fecha_inicio', 'nombre_campana']
        constraints = [
            models.CheckConstraint(check=models.Q(fecha_fin__gte=models.F('fecha_inicio')), name='chk_fecha_fin_ecom_gte_inicio')
        ]

# Tabla de hechos para inventarios
class FactInventarios(models.Model):
    inventario_sk = models.AutoField(primary_key=True, verbose_name="ID Inventario")
    
    # Relación con PDV
    pdv = models.ForeignKey(DimPdv, on_delete=models.PROTECT, db_column='pdv_id', verbose_name="Punto de Venta")
    
    # Fecha del inventario
    fecha = models.DateField(verbose_name="Fecha del Inventario")
    
    # Datos de inventario
    cantidad_existencias = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        verbose_name="Cantidad en Existencia"
    )
    valor_existencias = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        verbose_name="Valor en Existencia"
    )
    costo_promedio = models.DecimalField(
        max_digits=15, 
        decimal_places=4, 
        verbose_name="Costo Promedio"
    )
    
    # Análisis de rotación
    rotacion_dias = models.IntegerField(
        null=True, 
        blank=True, 
        verbose_name="Rotación en Días"
    )
    rotacion_semanas = models.DecimalField(
        max_digits=5, 
        decimal_places=2, 
        null=True, 
        blank=True, 
        verbose_name="Rotación en Semanas"
    )
    
    # Análisis de inventario
    dias_stock = models.IntegerField(
        null=True, 
        blank=True, 
        verbose_name="Días de Stock"
    )
    nivel_stock = models.CharField(
        max_length=20, 
        null=True, 
        blank=True, 
        verbose_name="Nivel de Stock"
    )
    
    # Auditoría
    fecha_creacion = models.DateTimeField(
        auto_now_add=True, 
        verbose_name="Fecha de Creación"
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True, 
        verbose_name="Última Actualización"
    )

    def __str__(self):
        return f"Inventario {self.pdv.nombre_pdv if self.pdv else 'Sin PDV'} - {self.fecha}"

    class Meta:
        db_table = 'fact_inventarios'
        verbose_name = "Inventario"
        verbose_name_plural = "Inventarios"
        ordering = ['-fecha', 'pdv__nombre_pdv']
        constraints = [
            models.UniqueConstraint(
                fields=['pdv', 'fecha'], 
                name='unique_inventario_pdv_fecha'
            )
        ]
        indexes = [
            models.Index(fields=['fecha']),
            models.Index(fields=['pdv']),
            models.Index(fields=['nivel_stock']),
        ]

# Tabla de hechos para rotación de inventario
class FactRotacion(models.Model):
    # Relaciones con dimensiones
    producto = models.ForeignKey('DimProducto', on_delete=models.PROTECT, db_column='producto_sk', null=True, blank=True, verbose_name="Producto")
    pdv = models.ForeignKey(DimPdv, on_delete=models.PROTECT, db_column='pdv_sk', null=True, blank=True, verbose_name="Punto de Venta")
    fecha_dim = models.ForeignKey('DimFecha', on_delete=models.PROTECT, db_column='fecha_sk', null=True, blank=True, verbose_name="Fecha")
    
    # Códigos para búsqueda rápida
    codigo_producto = models.CharField(max_length=50, null=True, blank=True, verbose_name="Código Producto")
    codigo_pdv = models.CharField(max_length=20, null=True, blank=True, verbose_name="Código PDV")
    fecha = models.DateField(null=True, blank=True, verbose_name="Fecha")
    
    # Datos de venta
    venta_unidades = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, verbose_name="Venta Unidades")
    venta_cajas = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, verbose_name="Venta Cajas")
    venta_blisters = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, verbose_name="Venta Blisters")
    
    # Precios y costos
    costo_unitario = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True, verbose_name="Costo Unitario")
    precio_venta_unitario = models.DecimalField(max_digits=15, decimal_places=4, null=True, blank=True, verbose_name="Precio Venta Unitario")
    
    # Totales
    costo_total = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True, verbose_name="Costo Total")
    venta_total = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True, verbose_name="Venta Total")
    
    # Análisis
    margen_bruto = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True, verbose_name="Margen Bruto")
    margen_porcentaje = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True, verbose_name="Margen %")
    
    # Inventario
    inventario_unidades_inicial = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, verbose_name="Inventario Inicial (Unidades)")
    inventario_unidades_final = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, verbose_name="Inventario Final (Unidades)")
    
    # Rotación
    dias_inventario = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Días de Inventario")
    rotacion_mes = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True, verbose_name="Rotación del Mes")
    
    # Auditoría
    fecha_carga = models.DateTimeField(auto_now_add=True, verbose_name="Fecha de Carga")
    fecha_actualizacion = models.DateTimeField(auto_now=True, verbose_name="Última Actualización")

    def __str__(self):
        return f"Rotación {self.codigo_producto} - {self.codigo_pdv} - {self.fecha}"

    class Meta:
        db_table = 'fact_rotacion'
        verbose_name = "Rotación de Inventario"
        verbose_name_plural = "Rotaciones de Inventario"
        ordering = ['-fecha', 'codigo_pdv', 'codigo_producto']
        indexes = [
            models.Index(fields=['codigo_producto', 'codigo_pdv', 'fecha']),
            models.Index(fields=['fecha']),
            models.Index(fields=['codigo_pdv']),
            models.Index(fields=['codigo_producto']),
        ]