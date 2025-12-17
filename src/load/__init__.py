"""
Módulo de carga de datos.
"""

from load.base_loader import BaseLoader
from load.csv_loader import CSVLoader
from load.parquet_loader import ParquetLoader

__all__ = [
    "BaseLoader",
    "CSVLoader",
    "ParquetLoader",
]
