# analysis/extractor/extractor_base.py

from abc import ABC, abstractmethod

class ExtractorBase(ABC):
    def __init__(self, directorio_raiz):
        self.directorio_raiz = directorio_raiz

    def es_archivo_valido(self, nombre_archivo):
        return (
            not nombre_archivo.startswith("~$")
            and nombre_archivo.lower().endswith((".xlsx", ".xls"))
        )

    @abstractmethod
    def extraer(self):
        pass
