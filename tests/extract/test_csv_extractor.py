"""
Pruebas unitarias para extractor de CSV.

Verifica la correcta extracción de archivos CSV incluyendo:
- Lectura exitosa y generación de metadata
- Manejo de valores nulos en profiling  
- Validaciones de entrada (nombre vacío, archivo inexistente, directorio inexistente)
- Configuración personalizada (encoding, separador)

Las pruebas utilizan excepciones personalizadas del módulo exceptions:
- SourceNameNotSpecifiedError: nombre de archivo vacío
- SourceNotFoundError: archivo o directorio no encontrado
"""

from pathlib import Path

import pandas as pd
import pytest
import pytest_check as check

from exceptions import SourceNameNotSpecifiedError, SourceNotFoundError
from extract.csv_extractor import CSVExtractor


class TestCSVExtractorExtract:
    """
    Tests para la extracción del CSVExtractor.
    Valida que se extraiga correctamente el archivo CSV y se actualice la metadata.
    """

    def test_extract_should_return_dataframe_and_metadata_when_csv_valid(
        self, tmp_path: Path
    ) -> None:
        """
        Debe retornar DataFrame y actualizar metadata cuando el CSV es válido.
        """
        # Crear CSV de prueba en el directorio temporal
        csv_content = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 102, 103],
                "total": [100.0, 200.0, 150.0],
            }
        )
        csv_content.to_csv(tmp_path / "orders.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)
        result = extractor.extract(name="orders")

        metadata = extractor.metadata

        check.equal(len(result), 3)
        check.equal(list(result.columns), ["order_id", "customer_id", "total"])
        check.equal(metadata["rows"], 3)
        check.equal(metadata["columns"], 3)
        check.equal(metadata["missing_values"], 0)
        check.is_not_none(metadata["extraction_timestamp"])
        check.is_true(metadata["memory_usage_mb"] >= 0)

    def test_extract_should_update_profiling_when_dataframe_has_nulls(
        self, tmp_path: Path
    ) -> None:
        """
        Debe calcular correctamente los valores nulos en profiling cuando el CSV tiene datos faltantes.
        """
        csv_content = pd.DataFrame(
            {
                "id": [1, 2, None],
                "name": ["A", None, "C"],
                "value": [10.0, 20.0, None],
            }
        )
        csv_content.to_csv(tmp_path / "with_nulls.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)
        result = extractor.extract(name="with_nulls")

        metadata = extractor.metadata

        check.equal(metadata["missing_values"], 3)
        check.is_true(metadata["missing_percentage"] > 0)


class TestCSVExtractorValidation:
    """
    Tests para validaciones del CSVExtractor.
    
    Verifica que se lancen las excepciones personalizadas apropiadas:
    - SourceNameNotSpecifiedError: cuando el nombre del archivo está vacío
    - SourceNotFoundError: cuando el directorio fuente o archivo no existe
    """

    def test_extract_should_raise_source_name_not_specified_when_name_empty(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar SourceNameNotSpecifiedError cuando el nombre está vacío.
        
        Valida que el extractor rechace nombres vacíos con la excepción específica,
        facilitando el diagnóstico de errores en pipelines ETL.
        """
        pd.DataFrame({"col": [1]}).to_csv(tmp_path / "dummy.csv", index=False)

        extractor = CSVExtractor(source_path=tmp_path)

        with pytest.raises(SourceNameNotSpecifiedError):
            extractor.extract(name="")

    def test_init_should_raise_source_not_found_when_directory_missing(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar SourceNotFoundError cuando el directorio fuente no existe.
        
        Verifica que el constructor valide la existencia del directorio
        antes de permitir cualquier operación de extracción.
        """
        missing_dir = tmp_path / "does_not_exist"

        with pytest.raises(SourceNotFoundError):
            CSVExtractor(source_path=missing_dir)

    def test_extract_should_raise_source_not_found_when_csv_file_missing(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar SourceNotFoundError cuando el archivo CSV especificado no existe.
        
        Verifica que el método extract valide la existencia del archivo
        específico antes de intentar leerlo.
        """
        extractor = CSVExtractor(source_path=tmp_path)

        with pytest.raises(SourceNotFoundError):
            extractor.extract(name="nonexistent_file")


class TestCSVExtractorConfiguration:
    """
    Tests para la configuración del CSVExtractor.
    
    Verifica que las opciones de configuración (encoding, separador)
    se almacenen correctamente en metadata y se apliquen al parsear.
    """

    def test_init_should_store_configuration_in_metadata_when_custom_options(
        self, tmp_path: Path
    ) -> None:
        """
        Debe almacenar la configuración personalizada en metadata.
        
        Verifica que encoding, separador y path fuente se registren
        en la metadata para auditoría y debugging del pipeline.
        """
        extractor = CSVExtractor(source_path=tmp_path, encoding="latin-1", sep=";")

        metadata = extractor.metadata

        check.equal(metadata["encoding"], "latin-1")
        check.equal(metadata["separator"], ";")
        check.equal(metadata["source_path"], str(tmp_path))

    def test_extract_should_parse_with_custom_separator_when_semicolon_csv(
        self, tmp_path: Path
    ) -> None:
        """
        Debe parsear correctamente CSV con separador personalizado.
        
        Verifica que el extractor respete la configuración de separador
        al leer archivos con formato no estándar (ej: punto y coma).
        """
        csv_path = tmp_path / "semicolon.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id;name;value\n")
            f.write("1;Product A;100\n")
            f.write("2;Product B;200\n")

        extractor = CSVExtractor(source_path=tmp_path, sep=";")
        result = extractor.extract(name="semicolon")

        check.equal(len(result), 2)
        check.equal(list(result.columns), ["id", "name", "value"])
