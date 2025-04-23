import pandas as pd

class BaseTransformer:
    def __init__(self, path):
        self.path = path
        self.df = None

    def leer_excel(self, **kwargs):
        """Carga el archivo Excel sin hacer transformaciones aún."""
        self.df = pd.read_excel(self.path, **kwargs)

    def estandarizar_columnas(self, columnas_estandarizadas):
        """Renombra columnas según el diccionario provisto."""
        columnas_actuales = self.df.columns
        columnas_renombrar = {col: columnas_estandarizadas[col] for col in columnas_actuales if col in columnas_estandarizadas}
        self.df.rename(columns=columnas_renombrar, inplace=True)

    def agregar_columnas_faltantes(self, columnas_esperadas):
        """Agrega columnas faltantes con valores nulos."""
        for col in columnas_esperadas:
            if col not in self.df.columns:
                self.df[col] = pd.NA

    def validar_columnas(self, columnas_esperadas):
        """Valida que las columnas esperadas estén presentes."""
        actuales = self.df.columns.tolist()
        faltantes = [col for col in columnas_esperadas if col not in actuales]
        extras = [col for col in actuales if col not in columnas_esperadas]

        if faltantes:
            raise ValueError(f"❌ Faltan columnas: {faltantes}")
        if extras:
            print(f"ℹ️ Columnas adicionales no mapeadas: {extras}")

    def transformar(self):
        raise NotImplementedError("Debes implementar el método 'transformar' en la subclase.")
