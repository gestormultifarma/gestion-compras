# analysis/extractor/extractor_merchandising.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorMerchandising(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if self.es_archivo_valido(archivo) and "merchandising" in archivo.lower():
                ruta_completa = os.path.join(self.directorio_raiz, archivo)
                archivos.append(ruta_completa)
                
        return archivos
