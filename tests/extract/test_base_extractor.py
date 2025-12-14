"""
Pruebas unitarias para extractor base.
"""

from typing import Any

import pandas as pd
import pytest_check as check

from extract.base_extractor import BaseExtractor


class DummyExtractor(BaseExtractor):
    """
    Implementación mínima para probar utilidades de la clase base.
    """

    def extract(self, name: str) -> pd.DataFrame:
        return pd.DataFrame()

    def _validate_source_exists(self, source_location: Any) -> None:
        return


class TestBaseExtractorProfiling:
    """
    Tests para el profiling del BaseExtractor.
    Valida que se actualice correctamente la metadata después de perfilar datos.
    """

    def test_profile_data_should_update_metadata_when_dataframe_valid(
        self, sample_valid_dataframe: pd.DataFrame
    ) -> None:
        """
        Debe actualizar la metadata con información de profiling cuando el DataFrame es válido.
        """
        extractor = DummyExtractor()

        extractor._profile_data(sample_valid_dataframe)

        metadata = extractor.metadata

        check.equal(metadata["rows"], sample_valid_dataframe.shape[0])
        check.equal(metadata["columns"], sample_valid_dataframe.shape[1])
        check.equal(metadata["column_names"], list(sample_valid_dataframe.columns))
        check.equal(metadata["missing_values"], 0)
        check.equal(metadata["missing_percentage"], 0.0)
        check.is_true("dtypes" in metadata)
        check.is_true(metadata["memory_usage_mb"] >= 0)

    def test_profile_data_should_calculate_missing_values_when_dataframe_has_nulls(
        self, dataframe_with_nulls: pd.DataFrame
    ) -> None:
        """
        Debe calcular correctamente los valores nulos cuando el DataFrame tiene valores faltantes.
        """
        extractor = DummyExtractor()

        extractor._profile_data(dataframe_with_nulls)

        metadata = extractor.metadata
        expected_missing = int(dataframe_with_nulls.isnull().sum().sum())

        check.equal(metadata["missing_values"], expected_missing)
        check.is_true(metadata["missing_percentage"] > 0)


class TestBaseExtractorTimestamp:
    """
    Tests para la actualización del timestamp de extracción.
    Valida que se registre correctamente el momento de la extracción.
    """

    def test_update_extraction_timestamp_should_set_timestamp_when_called(self) -> None:
        """
        Debe establecer el timestamp de extracción cuando se llama al método.
        """
        extractor = DummyExtractor()

        check.is_none(extractor.metadata["extraction_timestamp"])

        extractor._update_extraction_timestamp()

        check.is_not_none(extractor.metadata["extraction_timestamp"])
        check.is_true(isinstance(extractor.metadata["extraction_timestamp"], str))


class TestBaseExtractorSummary:
    """
    Tests para la generación del resumen de extracción.
    Valida que el resumen contenga la información esperada.
    """

    def test_get_summary_should_include_metadata_when_profiled_data_provided(
        self, sample_valid_dataframe: pd.DataFrame
    ) -> None:
        """
        Debe incluir metadata en el resumen cuando se proporcionan datos perfilados.
        """
        extractor = DummyExtractor()

        extractor._profile_data(sample_valid_dataframe)
        extractor._update_extraction_timestamp()

        summary = extractor.get_summary()

        check.is_true("rows" in summary)
        check.is_true("columns" in summary)
        check.is_true("extraction_timestamp" in summary)
        check.is_true("extractor_type" in summary)
