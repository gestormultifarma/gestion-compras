from django.db import migrations

class Migration(migrations.Migration):
    """
    Esta migración marca las tablas existentes como creadas sin intentar crearlas nuevamente.
    Esto es necesario porque las tablas ya existen en la base de datos.
    """

    dependencies = [
        ('dashboard', '0005_dimfecha_dimproducto_factinventarios_factrotacion'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- No hacemos nada en la aplicación de la migración
            -- ya que las tablas ya existen
            """,
            reverse_sql="""
            -- No hacemos nada en la reversión de la migración
            -- ya que no queremos eliminar las tablas existentes
            """,
        )
    ]
