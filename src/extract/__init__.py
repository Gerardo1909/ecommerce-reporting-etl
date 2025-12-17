"""
Módulo de extracción de datos.
"""

from extract.base_extractor import BaseExtractor
from extract.csv_extractor import CSVExtractor

__all__ = [
    "BaseExtractor",
    "CSVExtractor",
]
