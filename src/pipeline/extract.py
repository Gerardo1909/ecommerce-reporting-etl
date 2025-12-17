"""
Etapa de extracción del pipeline ETL.
"""

import sys
from pathlib import Path
from typing import Dict

import pandas as pd

# Agregar config al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import SOURCE_TABLES, get_raw_path
from extract import CSVExtractor
from utils.logger import extract_logger, run_context, log_stage


@log_stage("Extracción", extract_logger)
def run() -> Dict[str, pd.DataFrame]:
    """
    Extrae los datasets necesarios desde CSV.

    Returns:
        Dict[str, pd.DataFrame]: Diccionario con los DataFrames extraídos.
            Las claves son los nombres lógicos de las tablas.
    """
    csv_extractor = CSVExtractor(source_path=get_raw_path())

    extracted_tables = {}
    for logical_name, filename in SOURCE_TABLES.items():
        extracted_tables[logical_name] = csv_extractor.extract(name=filename)

    # Registrar métricas de extracción
    total_rows = sum(df.shape[0] for df in extracted_tables.values())
    run_context.record_stage_metric(
        "Extracción", "tables_extracted", len(extracted_tables)
    )
    run_context.record_stage_metric("Extracción", "total_rows_extracted", total_rows)

    return extracted_tables
