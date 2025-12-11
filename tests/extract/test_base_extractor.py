"""
Pruebas unitarias para extractor base.
"""

import pandas as pd
import pytest
import pytest_check as check

from extract.base_extractor import BaseExtractor


class MockExtractor(BaseExtractor):
    """
    Implementación mock de BaseExtractor para testing.
    Simula un extractor simple que retorna datos predefinidos.
    """

    def __init__(self, data: pd.DataFrame = None, should_fail: bool = False):
        super().__init__()
        self._mock_data = data
        self._should_fail = should_fail
        self.source_description = "Mock Data Source"

    def extract(self) -> pd.DataFrame:
        """Implementación mock de extract."""
        if self._should_fail:
            raise ValueError("Mock extraction failed")

        if self._mock_data is not None:
            self.data = self._mock_data
        else:
            # Datos por defecto
            self.data = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "value": [10, 20, 30],
                }
            )

        self._update_extraction_timestamp()
        return self.data

    def _validate_source_exists(self) -> None:
        """Implementación mock de validación."""
        if self._should_fail:
            raise FileNotFoundError("Mock source not found")


@pytest.mark.unit
@pytest.mark.smoke
@pytest.mark.extract
class TestBaseExtractorInitialization:
    """
    Tests para la inicialización de BaseExtractor.
    Valida que el constructor configure correctamente todos los atributos.
    """

    def test_init_should_initialize_all_attributes_when_created(self):
        """Debe inicializar todos los atributos al crear la instancia."""
        extractor = MockExtractor()

        check.is_not_none(extractor.source_description)
        check.is_none(extractor.data, "data debe ser None antes de extract()")
        check.is_instance(extractor.metadata, dict)
        check.is_not_none(extractor.logger)

    def test_init_should_set_extractor_type_in_metadata_when_created(self):
        """Debe incluir el tipo de extractor en metadata."""
        extractor = MockExtractor()

        check.is_in("extractor_type", extractor.metadata)
        check.equal(extractor.metadata["extractor_type"], "MockExtractor")

    def test_init_should_set_extraction_timestamp_to_none_when_no_extraction_yet(self):
        """Debe inicializar extraction_timestamp como None antes de extraer."""
        extractor = MockExtractor()

        check.is_in("extraction_timestamp", extractor.metadata)
        check.is_none(
            extractor.metadata["extraction_timestamp"],
            "Timestamp debe ser None antes de extract()",
        )

    def test_init_should_allow_source_description_customization_when_subclass_sets_it(
        self,
    ):
        """Debe permitir que subclases personalicen source_description."""
        extractor = MockExtractor()
        extractor.source_description = "Custom Source Description"

        check.equal(extractor.source_description, "Custom Source Description")


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorGetData:
    """
    Tests para el método get_data().
    Valida la obtención del DataFrame después de la extracción.
    """

    def test_get_data_should_return_dataframe_when_extract_executed(
        self, sample_valid_dataframe
    ):
        """Debe retornar el DataFrame después de ejecutar extract()."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        result = extractor.get_data()

        check.is_instance(result, pd.DataFrame)
        check.equal(len(result), len(sample_valid_dataframe))

    def test_get_data_should_fail_when_no_extraction_performed(self):
        """Debe lanzar ValueError si no se ha ejecutado extract()."""
        extractor = MockExtractor()

        with pytest.raises(ValueError) as exc_info:
            extractor.get_data()

        check.is_in("No hay datos cargados", str(exc_info.value))
        check.is_in("extract()", str(exc_info.value))

    def test_get_data_should_return_same_data_as_extract_when_called_multiple_times(
        self, sample_valid_dataframe
    ):
        """Debe retornar consistentemente los mismos datos."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extracted = extractor.extract()

        result1 = extractor.get_data()
        result2 = extractor.get_data()

        check.is_true(result1.equals(extracted))
        check.is_true(result1.equals(result2))


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorProfileData:
    """
    Tests para el método profile_data().
    Valida la generación de perfiles estadísticos del DataFrame.
    """

    def test_profile_data_should_return_complete_profile_when_data_loaded(
        self, sample_valid_dataframe
    ):
        """Debe retornar un perfil completo con todas las estadísticas."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        # Verificar todas las claves esperadas
        expected_keys = [
            "rows",
            "columns",
            "column_names",
            "missing_values",
            "missing_percentage",
            "dtypes",
            "memory_usage_mb",
        ]
        for key in expected_keys:
            check.is_in(key, profile, f"Profile debe contener '{key}'")

    def test_profile_data_should_calculate_correct_dimensions_when_called(
        self, sample_valid_dataframe
    ):
        """Debe calcular correctamente filas y columnas."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        check.equal(profile["rows"], len(sample_valid_dataframe))
        check.equal(profile["columns"], len(sample_valid_dataframe.columns))
        check.equal(profile["column_names"], list(sample_valid_dataframe.columns))

    def test_profile_data_should_calculate_missing_values_correctly_when_nulls_present(
        self, dataframe_with_nulls
    ):
        """Debe calcular correctamente valores nulos y porcentaje."""
        extractor = MockExtractor(data=dataframe_with_nulls)
        extractor.extract()

        profile = extractor.profile_data()

        # dataframe_with_nulls tiene 3 nulos en 15 celdas (3 cols × 5 rows)
        check.equal(profile["missing_values"], 3)
        check.is_instance(profile["missing_percentage"], float)
        check.greater(profile["missing_percentage"], 0)

    def test_profile_data_should_include_data_types_for_all_columns_when_profiled(
        self, sample_valid_dataframe
    ):
        """Debe incluir tipos de datos para todas las columnas."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        check.is_instance(profile["dtypes"], dict)
        check.equal(
            len(profile["dtypes"]),
            len(sample_valid_dataframe.columns),
            "Debe tener tipo para cada columna",
        )

    def test_profile_data_should_calculate_memory_usage_when_profiled(
        self, sample_valid_dataframe
    ):
        """Debe calcular el uso de memoria en MB."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        check.is_in("memory_usage_mb", profile)
        check.is_instance(profile["memory_usage_mb"], float)
        check.greater_equal(profile["memory_usage_mb"], 0.0)

    def test_profile_data_should_update_metadata_when_executed(
        self, sample_valid_dataframe
    ):
        """Debe actualizar metadata con información del perfil."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        # Verificar que metadata se actualiza con el perfil
        check.equal(extractor.metadata["rows"], profile["rows"])
        check.equal(extractor.metadata["columns"], profile["columns"])

    def test_profile_data_should_fail_when_no_data_extracted(self):
        """Debe fallar si no hay datos cargados."""
        extractor = MockExtractor()

        with pytest.raises(ValueError) as exc_info:
            extractor.profile_data()

        check.is_in("No hay datos cargados", str(exc_info.value))


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorMetadata:
    """
    Tests para get_metadata() y gestión de metadata.
    Valida el tracking de información sobre la extracción.
    """

    def test_get_metadata_should_return_dict_when_called(self):
        """Debe retornar un diccionario con metadata."""
        extractor = MockExtractor()

        metadata = extractor.get_metadata()

        check.is_instance(metadata, dict)

    def test_get_metadata_should_include_extractor_type_when_initialized(self):
        """Debe incluir el tipo de extractor en metadata."""
        extractor = MockExtractor()

        metadata = extractor.get_metadata()

        check.is_in("extractor_type", metadata)
        check.equal(metadata["extractor_type"], "MockExtractor")

    def test_get_metadata_should_update_timestamp_after_extraction_when_extract_called(
        self, sample_valid_dataframe
    ):
        """Debe actualizar el timestamp después de extraer."""
        extractor = MockExtractor(data=sample_valid_dataframe)

        # Antes de extract
        check.is_none(extractor.metadata["extraction_timestamp"])

        # Después de extract
        extractor.extract()

        check.is_not_none(
            extractor.metadata["extraction_timestamp"],
            "Timestamp debe actualizarse después de extract()",
        )

    def test_get_metadata_should_contain_profile_info_after_profiling_when_profiled(
        self, sample_valid_dataframe
    ):
        """Debe incluir información del perfil después de profile_data()."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        # Antes de profiling
        check.is_not_in("rows", extractor.metadata)

        # Después de profiling
        extractor.profile_data()
        metadata = extractor.get_metadata()

        check.is_in("rows", metadata)
        check.is_in("columns", metadata)
        check.is_in("missing_values", metadata)


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorGetSummary:
    """
    Tests para el método get_summary().
    Valida la generación de resúmenes legibles del proceso.
    """

    def test_get_summary_should_return_str_when_called(self, sample_valid_dataframe):
        """Debe retornar una cadena de texto con el resumen."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        summary = extractor.get_summary()

        check.is_instance(summary, str)

    def test_get_summary_should_include_source_description_when_set(
        self, sample_valid_dataframe
    ):
        """Debe incluir la descripción de la fuente."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.source_description = "Test Source Description"
        extractor.extract()

        summary = extractor.get_summary()

        check.is_in("Test Source Description", summary)

    def test_get_summary_should_include_extraction_timestamp_when_extracted(
        self, sample_valid_dataframe
    ):
        """Debe incluir el timestamp de extracción."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        # Tomo timestamp del metadata para comparar
        extraction_timestamp = extractor.metadata.get("extraction_timestamp")

        summary = extractor.get_summary()

        check.is_in(extraction_timestamp, summary)

    def test_get_summary_should_include_data_dimensions_when_profiled(
        self, sample_valid_dataframe
    ):
        """Debe incluir dimensiones de datos si se hizo profiling."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()
        extractor.profile_data()

        summary = extractor.get_summary()

        check.is_in(f"{len(sample_valid_dataframe)} filas", summary)
        check.is_in(f"{len(sample_valid_dataframe.columns)} columnas", summary)

    def test_get_summary_should_work_without_profiling_when_extract_only(
        self, sample_valid_dataframe
    ):
        """Debe funcionar incluso sin profile_data()."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        summary = extractor.get_summary()

        check.is_instance(summary, str)
        check.is_in("Fuente", summary)
        check.is_in("Timestamp", summary)


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorValidateSource:
    """
    Tests para _validate_source_exists().
    Como es abstracto, probamos que las subclases deben implementarlo.
    """

    def test_validate_source_exists_should_be_implemented_by_subclass_when_called(self):
        """Debe ser implementado por cada subclase."""
        # MockExtractor implementa este método
        extractor = MockExtractor(should_fail=False)

        # No debe lanzar NotImplementedError
        try:
            extractor._validate_source_exists()
        except NotImplementedError:
            pytest.fail("MockExtractor debe implementar _validate_source_exists()")

    def test_validate_source_exists_should_fail_when_source_invalid(self):
        """Debe fallar cuando la fuente no es válida (según implementación)."""
        extractor = MockExtractor(should_fail=True)

        with pytest.raises(FileNotFoundError):
            extractor._validate_source_exists()


@pytest.mark.unit
@pytest.mark.extract
class TestBaseExtractorEdgeCases:
    """
    Tests para casos extremos y situaciones especiales.
    """

    def test_extractor_should_handle_empty_dataframe_when_extracted(
        self, empty_csv_file
    ):
        """Debe manejar correctamente DataFrames vacíos."""
        df = pd.DataFrame(columns=["id", "name", "value"])
        extractor = MockExtractor(data=df)

        result = extractor.extract()

        check.equal(len(result), 0)
        check.equal(len(result.columns), 3)

    def test_profile_data_should_handle_zero_missing_values_when_complete_data(
        self, sample_valid_dataframe
    ):
        """Debe manejar correctamente cuando no hay valores nulos."""
        extractor = MockExtractor(data=sample_valid_dataframe)
        extractor.extract()

        profile = extractor.profile_data()

        check.equal(profile["missing_values"], 0)
        check.equal(profile["missing_percentage"], 0.0)

    def test_multiple_extractions_should_update_timestamp_when_called_repeatedly(
        self, sample_valid_dataframe
    ):
        """Cada extracción debe actualizar el timestamp."""
        extractor = MockExtractor(data=sample_valid_dataframe)

        extractor.extract()
        first_timestamp = extractor.metadata["extraction_timestamp"]

        # Pequeña pausa para asegurar diferente timestamp
        import time

        time.sleep(0.01)

        extractor.extract()
        second_timestamp = extractor.metadata["extraction_timestamp"]

        check.not_equal(
            first_timestamp,
            second_timestamp,
            "Timestamps deben ser diferentes en extracciones sucesivas",
        )
