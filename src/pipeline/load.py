"""
Etapa de carga del pipeline ETL.
"""

import sys
from pathlib import Path
from typing import Dict

import pandas as pd

# Agregar config al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    get_output_csv_path,
    get_output_parquet_path,
    get_processed_csv_path,
    get_processed_parquet_path,
    OUTPUT_FORMATS,
)
from load import CSVLoader, ParquetLoader
from utils.logger import load_logger, run_context, log_stage


@log_stage("Carga", load_logger)
def run(enriched: Dict[str, pd.DataFrame], results: Dict[str, pd.DataFrame]) -> None:
    """
    Guarda datasets enriquecidos y agregaciones en disco.

    Args:
        enriched: Diccionario con los DataFrames enriquecidos.
        results: Diccionario con las métricas calculadas.
    """
    processed_csv_path = get_processed_csv_path()
    output_csv_path = get_output_csv_path()
    processed_parquet_path = get_processed_parquet_path()
    output_parquet_path = get_output_parquet_path()

    files_saved = 0

    # Guardar en formato Parquet si está habilitado
    if OUTPUT_FORMATS.get("parquet", False):
        parquet_processed = ParquetLoader(target_path=processed_parquet_path)
        parquet_outputs = ParquetLoader(target_path=output_parquet_path)

        # Datasets enriquecidos
        for name, df in enriched.items():
            parquet_processed.save(df, name=f"{name}_enriched")
            files_saved += 1

        # Métricas agregadas
        for name, df in results.items():
            parquet_outputs.save(df, name=name)
            files_saved += 1

    # Guardar en formato CSV si está habilitado
    if OUTPUT_FORMATS.get("csv", False):
        csv_processed = CSVLoader(target_path=processed_csv_path)
        csv_outputs = CSVLoader(target_path=output_csv_path)

        # Datasets enriquecidos
        for name, df in enriched.items():
            csv_processed.save(df, name=f"{name}_enriched")
            files_saved += 1

        # Métricas agregadas
        for name, df in results.items():
            csv_outputs.save(df, name=name)
            files_saved += 1

    # Registrar métricas de carga
    run_context.record_stage_metric("Carga", "files_generated", files_saved)
    run_context.record_stage_metric("Carga", "enriched_datasets", len(enriched))
    run_context.record_stage_metric("Carga", "aggregated_metrics", len(results))
