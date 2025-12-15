"""
Pruebas unitarias para loader de CSV.

Verifica la correcta carga de DataFrames a archivos CSV incluyendo:
- Guardado exitoso y generación de metadata (rows, columns, file_size, timestamp)
- Validaciones de entrada (nombre vacío, directorio destino inexistente)

Las pruebas utilizan excepciones personalizadas del módulo exceptions:
- TargetNameNotSpecifiedError: nombre de archivo vacío
- TargetNotFoundError: directorio destino no encontrado
"""

from pathlib import Path

import pandas as pd
import pytest
import pytest_check as check

from exceptions import TargetNameNotSpecifiedError, TargetNotFoundError
from load.csv_loader import CSVLoader


class TestCSVLoaderSave:
    """
    Tests para el guardado del CSVLoader.
    
    Verifica la creación correcta del archivo CSV y la actualización
    de metadata incluyendo dimensiones, valores nulos y timestamps.
    """

    def test_save_should_create_csv_and_metadata_when_dataframe_valid(
        self, tmp_path: Path, sample_valid_dataframe: pd.DataFrame
    ) -> None:
        """
        Debe crear archivo CSV y actualizar metadata cuando el DataFrame es válido.
        
        Verifica que el archivo se cree correctamente, contenga los datos esperados
        y que la metadata refleje las dimensiones y características del guardado.
        """
        loader = CSVLoader(target_path=tmp_path, encoding="utf-8", sep=",", index=False)

        loader.save(sample_valid_dataframe, name="orders_enriched")

        target_file = tmp_path / "orders_enriched.csv"
        loaded = pd.read_csv(target_file)
        metadata = loader.metadata

        check.is_true(target_file.exists())
        check.equal(len(loaded), len(sample_valid_dataframe))
        check.is_true(set(loaded.columns) == set(sample_valid_dataframe.columns))
        check.equal(metadata["rows"], sample_valid_dataframe.shape[0])
        check.equal(metadata["columns"], sample_valid_dataframe.shape[1])
        check.equal(metadata["missing_values"], 0)
        check.is_true("target" in metadata)
        check.is_true(str(target_file) == metadata["target"])
        check.is_true(metadata["file_size_mb"] >= 0)
        check.is_true(metadata["load_timestamp"] is not None)


class TestCSVLoaderValidation:
    """
    Tests para validaciones del CSVLoader.
    
    Verifica que se lancen las excepciones personalizadas apropiadas:
    - TargetNameNotSpecifiedError: cuando el nombre del archivo está vacío
    - TargetNotFoundError: cuando el directorio destino no existe
    """

    def test_save_should_raise_target_name_not_specified_when_name_empty(
        self, tmp_path: Path, sample_valid_dataframe: pd.DataFrame
    ) -> None:
        """
        Debe lanzar TargetNameNotSpecifiedError cuando el nombre está vacío.
        
        Valida que el loader rechace nombres vacíos con la excepción específica,
        facilitando el diagnóstico de errores en pipelines de carga.
        """
        loader = CSVLoader(target_path=tmp_path)

        with pytest.raises(TargetNameNotSpecifiedError):
            loader.save(sample_valid_dataframe, name="")

    def test_init_should_raise_target_not_found_when_directory_missing(
        self, tmp_path: Path
    ) -> None:
        """
        Debe lanzar TargetNotFoundError cuando el directorio destino no existe.
        
        Verifica que el constructor valide la existencia del directorio
        antes de permitir cualquier operación de carga.
        """
        missing_dir = tmp_path / "does_not_exist"

        with pytest.raises(TargetNotFoundError):
            CSVLoader(target_path=missing_dir)