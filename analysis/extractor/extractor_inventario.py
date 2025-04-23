# analysis/extractor/extractor_inventario.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorInventario(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if archivo.lower().endswith('.xlsx') and 'costeados' in archivo.lower():
                ruta_completa = os.path.join(self.directorio_raiz, archivo)
                archivos.append(ruta_completa)
        return archivos
