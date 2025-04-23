# analysis/extractor/extractor_ventas.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorVentas(ExtractorBase):
    def extraer(self):
        archivos = []
        for subcarpeta in os.listdir(self.directorio_raiz):
            ruta_subcarpeta = os.path.join(self.directorio_raiz, subcarpeta)
            if os.path.isdir(ruta_subcarpeta):
                for archivo in os.listdir(ruta_subcarpeta):
                    if (archivo.endswith('.xlsx') or archivo.endswith('.xls')) and not archivo.startswith('~$'):
                        ruta_completa = os.path.join(ruta_subcarpeta, archivo)
                        archivos.append(ruta_completa)
        return archivos
