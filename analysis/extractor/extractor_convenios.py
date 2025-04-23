# analysis/extractor/extractor_convenios.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorConvenios(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if self.es_archivo_valido(archivo) and "convenio" in archivo.lower():
                archivos.append(os.path.join(self.directorio_raiz, archivo))
        return archivos
