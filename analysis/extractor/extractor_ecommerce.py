# analysis/extractor/extractor_ecommerce.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorEcommerce(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if self.es_archivo_valido(archivo) and "maestra-ecommerce" in archivo.lower():
                archivos.append(os.path.join(self.directorio_raiz, archivo))
        return archivos
