# analysis/extractor/extractor_gerencia.py

import os
from analysis.extractor.extractor_base import ExtractorBase

class ExtractorGerencia(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if self.es_archivo_valido(archivo) and "gerencia" in archivo.lower():
                ruta_completa = os.path.join(self.directorio_raiz, archivo)
                archivos.append(ruta_completa)
                
        return archivos
