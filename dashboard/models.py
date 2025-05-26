# dashboard/models.py
from django.db import models

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