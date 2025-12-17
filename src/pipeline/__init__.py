"""
Módulo pipeline - Etapas del proceso ETL.
"""

from pipeline.extract import run as run_extract
from pipeline.transform import run as run_transform
from pipeline.load import run as run_load

__all__ = [
    "run_extract",
    "run_transform",
    "run_load",
]
