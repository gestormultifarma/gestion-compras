# analysis/extractor/extractor_base.py

from abc import ABC, abstractmethod

class ExtractorBase(ABC):
    def __init__(self, directorio_raiz):
        self.directorio_raiz = directorio_raiz

    @abstractmethod
    def extraer(self):
        pass
