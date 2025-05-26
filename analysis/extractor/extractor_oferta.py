# analysis/extractor/extractor_oferta.py

import os

from analysis.extractor.extractor_base import ExtractorBase

class ExtractorOferta(ExtractorBase):
    def extraer(self):
        archivos = []
        for archivo in os.listdir(self.directorio_raiz):
            if self.es_archivo_valido(archivo) and archivo.lower().endswith(".xls") and "bonificaciones" in archivo.lower():
                ruta_completa = os.path.join(self.directorio_raiz, archivo)
                #print(f"🗂️ Archivo válido encontrado: {ruta_completa}") 
                archivos.append(ruta_completa)
        #if not archivos:
        #    print("⚠️ No se encontraron archivos válidos.")
        return archivos
