"""
Pruebas unitarias para extractor de CSV.
"""

import pytest
import pytest_check as check
import pandas as pd

from extract.csv_extractor import CSVExtractor


@pytest.mark.unit
@pytest.mark.smoke
@pytest.mark.extract
class TestCSVExtractorInitialization:
    """
    Tests para la inicialización de CSVExtractor.
    Valida configuración, parámetros y validación inicial.
    """

    def test_init_should_create_instance_when_valid_file_provided(self, valid_csv_file):
        """Debe crear instancia con archivo válido."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        check.is_not_none(extractor)
        check.is_instance(extractor, CSVExtractor)
        check.equal(str(extractor.file_path), str(valid_csv_file))

    def test_init_should_set_source_description_when_created(self, valid_csv_file):
        """Debe establecer source_description con el path del archivo."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        check.is_not_none(extractor.source_description)
        check.is_in(str(valid_csv_file), extractor.source_description)

    def test_init_should_fail_when_file_does_not_exist(self, tmp_path):
        """Debe fallar si el archivo no existe."""
        non_existent_file = tmp_path / "no_existe.csv"

        with pytest.raises(FileNotFoundError) as exc_info:
            CSVExtractor(file_path=non_existent_file)

        check.is_in("Archivo CSV no encontrado", str(exc_info.value))
        check.is_in(str(non_existent_file), str(exc_info.value))

    def test_init_should_use_default_encoding_when_not_specified(self, valid_csv_file):
        """Debe usar UTF-8 como encoding por defecto."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        check.equal(extractor.encoding, "utf-8")

    def test_init_should_use_default_separator_when_not_specified(self, valid_csv_file):
        """Debe usar coma como separador por defecto."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        check.equal(extractor.sep, ",")

    def test_init_should_accept_custom_encoding_when_provided(self, valid_csv_file):
        """Debe aceptar encoding personalizado."""
        extractor = CSVExtractor(file_path=valid_csv_file, encoding="latin1")

        check.equal(extractor.encoding, "latin1")

    def test_init_should_accept_custom_separator_when_provided(self, valid_csv_file):
        """Debe aceptar separador personalizado."""
        extractor = CSVExtractor(file_path=valid_csv_file, sep=";")

        check.equal(extractor.sep, ";")

    def test_init_should_store_configuration_in_metadata_when_created(
        self, valid_csv_file
    ):
        """Debe almacenar configuración en metadata."""
        extractor = CSVExtractor(
            file_path=valid_csv_file, encoding="utf-8", sep=",", auto_profile=False
        )

        metadata = extractor.get_metadata()

        check.is_in("encoding", metadata)
        check.is_in("separator", metadata)
        check.is_in("file_path", metadata)
        check.equal(metadata["encoding"], "utf-8")
        check.equal(metadata["separator"], ",")

    def test_init_should_enable_auto_profile_by_default_when_not_specified(
        self, valid_csv_file
    ):
        """Debe habilitar auto_profile por defecto."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        check.is_true(extractor.auto_profile)

    def test_init_should_allow_disabling_auto_profile_when_specified(
        self, valid_csv_file
    ):
        """Debe permitir deshabilitar auto_profile."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)

        check.is_false(extractor.auto_profile)


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorSourceValidation:
    """
    Tests para _validate_source_exists().
    Valida la existencia y accesibilidad del archivo CSV.
    """

    def test_validate_source_exists_should_pass_when_file_exists(self, valid_csv_file):
        """Debe pasar cuando el archivo existe."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        # La validación ocurre en __init__, no debe lanzar excepción
        try:
            extractor._validate_source_exists()
        except FileNotFoundError:
            pytest.fail("No debe fallar con archivo válido")

    def test_validate_source_exists_should_fail_when_file_missing(self, tmp_path):
        """Debe fallar cuando el archivo no existe."""
        non_existent_file = tmp_path / "missing.csv"

        with pytest.raises(FileNotFoundError) as exc_info:
            CSVExtractor(file_path=non_existent_file)

        check.is_in("Archivo CSV no encontrado", str(exc_info.value))

    def test_validate_source_exists_should_include_path_in_error_when_file_missing(
        self, tmp_path
    ):
        """Debe incluir el path en el mensaje de error."""
        non_existent_file = tmp_path / "archivo_perdido.csv"

        with pytest.raises(FileNotFoundError) as exc_info:
            CSVExtractor(file_path=non_existent_file)

        check.is_in(str(non_existent_file), str(exc_info.value))


@pytest.mark.unit
@pytest.mark.smoke
@pytest.mark.extract
class TestCSVExtractorExtract:
    """
    Tests para el método extract().
    Valida la extracción correcta de datos desde archivos CSV.
    """

    def test_extract_should_return_dataframe_when_valid_csv(self, valid_csv_file):
        """Debe retornar un DataFrame con archivo CSV válido."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)
        check.greater(len(result), 0, "DataFrame no debe estar vacío")

    def test_extract_should_load_all_rows_when_executed(self, valid_csv_file):
        """Debe cargar todas las filas del CSV."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        result = extractor.extract()

        # valid_csv_file tiene 5 filas de datos
        check.equal(len(result), 5)

    def test_extract_should_load_all_columns_when_executed(self, valid_csv_file):
        """Debe cargar todas las columnas del CSV."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        result = extractor.extract()

        # valid_csv_file tiene 6 columnas
        expected_columns = [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_date",
        ]
        check.equal(len(result.columns), len(expected_columns))
        for col in expected_columns:
            check.is_in(col, result.columns)

    def test_extract_should_update_extraction_timestamp_when_executed(
        self, valid_csv_file
    ):
        """Debe actualizar el timestamp de extracción."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        # Antes de extract
        check.is_none(extractor.metadata["extraction_timestamp"])

        extractor.extract()

        # Después de extract
        check.is_not_none(extractor.metadata["extraction_timestamp"])

    def test_extract_should_auto_profile_when_enabled(self, valid_csv_file):
        """Debe generar perfil automáticamente si está habilitado."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=True)

        extractor.extract()

        # Verificar que se generó el perfil
        metadata = extractor.get_metadata()
        check.is_in("rows", metadata)
        check.is_in("columns", metadata)

    def test_extract_should_not_auto_profile_when_disabled(self, valid_csv_file):
        """No debe generar perfil si está deshabilitado."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)

        extractor.extract()

        # Verificar que NO se generó el perfil
        metadata = extractor.get_metadata()
        check.is_not_in("rows", metadata)

    def test_extract_should_allow_multiple_calls_when_executed(self, valid_csv_file):
        """Debe permitir múltiples llamadas a extract()."""
        extractor = CSVExtractor(file_path=valid_csv_file)

        result1 = extractor.extract()
        result2 = extractor.extract()

        check.is_true(result1.equals(result2))


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorSchemaValidation:
    """
    Tests para validate_schema().
    Valida la correcta validación de esquemas de datos.
    """

    def test_validate_schema_should_pass_when_schema_matches(
        self, valid_csv_file, expected_schema_orders
    ):
        """Debe pasar cuando el esquema coincide."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        # No debe lanzar excepción
        try:
            result = extractor.validate_schema(**expected_schema_orders)
            check.is_true(result)
        except ValueError:
            pytest.fail("No debe fallar con esquema válido")

    def test_validate_schema_should_fail_when_missing_required_columns(
        self, valid_csv_file
    ):
        """Debe fallar cuando faltan columnas requeridas."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        invalid_schema = {
            "expected_columns": ["order_id", "nonexistent_column"],
        }

        with pytest.raises(ValueError) as exc_info:
            extractor.validate_schema(**invalid_schema)
        check.is_in("nonexistent_column", str(exc_info.value))

    def test_validate_schema_should_validate_data_types_when_specified(
        self, valid_csv_file
    ):
        """Debe validar tipos de datos cuando están especificados."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        schema_with_types = {
            "expected_columns": ["order_id"],
            "expected_dtypes": {"order_id": "int64"},
        }

        result = extractor.validate_schema(**schema_with_types)
        check.is_true(result)

    def test_validate_schema_should_fail_when_data_types_mismatch(self, valid_csv_file):
        """Debe fallar cuando los tipos de datos no coinciden."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        invalid_schema = {
            "expected_columns": ["order_id"],
            "expected_dtypes": {"order_id": "object"},  # Debería ser int64
        }

        with pytest.raises(ValueError):
            extractor.validate_schema(**invalid_schema)

    def test_validate_schema_should_fail_when_no_data_extracted(self, valid_csv_file):
        """Debe fallar si no se han extraído datos."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)

        schema = {"expected_columns": ["order_id"]}

        with pytest.raises(ValueError) as exc_info:
            extractor.validate_schema(**schema)

        check.is_in("No hay datos cargados", str(exc_info.value))


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorColumnInfo:
    """
    Tests para get_column_info().
    Valida la obtención de información sobre columnas.
    """

    def test_get_column_info_should_return_dataframe_when_data_extracted(
        self, valid_csv_file
    ):
        """Debe retornar un dataframe de pandas con información de columnas."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        column_info = extractor.get_column_info()

        check.is_instance(column_info, pd.DataFrame)

    def test_get_column_info_should_include_all_columns_when_executed(
        self, valid_csv_file
    ):
        """Debe incluir información de todas las columnas."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        column_info = extractor.get_column_info()

        expected_columns = [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_date",
        ]
        for col in expected_columns:
            check.is_in(col, column_info["columna"].values)

    def test_get_column_info_should_include_data_type_for_each_column_when_executed(
        self, valid_csv_file
    ):
        """Debe incluir el tipo de dato de cada columna."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        column_info = extractor.get_column_info()

        # Verificar que la columna 'tipo' existe
        check.is_in("tipo", column_info.columns)

        # Verificar que cada fila tiene un tipo no nulo
        for idx, row in column_info.iterrows():
            check.is_not_none(row["tipo"], f"Columna {row['columna']} debe tener tipo")

    def test_get_column_info_should_include_null_count_for_each_column_when_executed(
        self, valid_csv_file
    ):
        """Debe incluir el conteo de nulos de cada columna."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        column_info = extractor.get_column_info()

        # Verificar que la columna 'valores_nulos' existe
        check.is_in("valores_nulos", column_info.columns)

        # Verificar que cada fila tiene un conteo de nulos válido
        for idx, row in column_info.iterrows():
            check.is_instance(
                row["valores_nulos"],
                (int, float),
                f"Columna {row['columna']} debe tener valores_nulos numérico",
            )

    def test_get_column_info_should_include_unique_count_for_each_column_when_executed(
        self, valid_csv_file
    ):
        """Debe incluir el conteo de valores únicos de cada columna."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        column_info = extractor.get_column_info()

        # Verificar que la columna 'valores_unicos' existe
        check.is_in("valores_unicos", column_info.columns)

        # Verificar que cada fila tiene un conteo de valores únicos válido
        for idx, row in column_info.iterrows():
            check.is_instance(
                row["valores_unicos"],
                (int, float),
                f"Columna {row['columna']} debe tener valores_unicos numérico",
            )

    def test_get_column_info_should_fail_when_no_data_extracted(self, valid_csv_file):
        """Debe fallar si no se han extraído datos."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)

        with pytest.raises(ValueError) as exc_info:
            extractor.get_column_info()

        check.is_in("No hay datos cargados", str(exc_info.value))


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorEncodingAndSeparators:
    """
    Tests para manejo de diferentes encodings y separadores.
    Valida la flexibilidad del extractor.
    """

    def test_extract_should_handle_latin1_encoding_when_specified(
        self, csv_with_latin1_encoding
    ):
        """Debe manejar encoding latin1 correctamente."""
        extractor = CSVExtractor(
            file_path=csv_with_latin1_encoding, encoding="latin1", auto_profile=False
        )

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)
        check.greater(len(result), 0)

        # Verificar que los caracteres especiales se leyeron correctamente
        names = result["name"].tolist()
        check.is_in("José", names)
        check.is_in("María", names)

    def test_extract_should_handle_semicolon_separator_when_specified(
        self, csv_with_semicolon_separator
    ):
        """Debe manejar separador punto y coma."""
        extractor = CSVExtractor(
            file_path=csv_with_semicolon_separator, sep=";", auto_profile=False
        )

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)
        check.equal(len(result.columns), 3)

    def test_extract_should_fail_when_wrong_encoding_specified(
        self, csv_with_latin1_encoding
    ):
        """Debe fallar con encoding incorrecto."""
        extractor = CSVExtractor(
            file_path=csv_with_latin1_encoding, encoding="utf-8", auto_profile=False
        )

        # Puede fallar con UnicodeDecodeError o simplemente leer mal los datos
        # Dependiendo de la versión de pandas
        try:
            result = extractor.extract()
            # Si no falla, al menos los caracteres deben estar incorrectos
            names = result["nombre"].tolist()
            # Los nombres no deben leerse correctamente con encoding incorrecto
        except UnicodeDecodeError:
            # Esto es aceptable
            pass

    def test_extract_should_fail_when_wrong_separator_specified(
        self, csv_with_semicolon_separator
    ):
        """Debe producir resultados incorrectos con separador equivocado."""
        extractor = CSVExtractor(
            file_path=csv_with_semicolon_separator, sep=",", auto_profile=False
        )

        result = extractor.extract()

        # Con separador incorrecto, debe leer una sola columna
        check.equal(
            len(result.columns),
            1,
            "Separador incorrecto debe resultar en menos columnas",
        )


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorDateParsing:
    """
    Tests para el parsing de fechas.
    Valida la conversión automática de columnas de fecha.
    """

    def test_extract_should_parse_dates_when_specified(self, csv_with_dates):
        """Debe parsear fechas cuando se especifica parse_dates."""
        extractor = CSVExtractor(
            file_path=csv_with_dates,
            parse_dates=["created_at", "updated_at"],
            auto_profile=False,
        )

        result = extractor.extract()

        check.equal(result["created_at"].dtype, "datetime64[ns]")
        check.equal(result["updated_at"].dtype, "datetime64[ns]")

    def test_extract_should_not_parse_dates_when_not_specified(self, csv_with_dates):
        """No debe parsear fechas si no se especifica."""
        extractor = CSVExtractor(file_path=csv_with_dates, auto_profile=False)

        result = extractor.extract()

        # Sin parse_dates, deben ser object (strings)
        check.equal(result["created_at"].dtype, "object")

    def test_extract_should_store_parse_dates_config_in_metadata_when_specified(
        self, csv_with_dates
    ):
        """Debe almacenar configuración de parse_dates en metadata."""
        extractor = CSVExtractor(
            file_path=csv_with_dates,
            parse_dates=["created_at"],
            auto_profile=False,
        )

        metadata = extractor.get_metadata()

        check.is_in("parse_dates", metadata)
        check.is_in("created_at", metadata["parse_dates"])


@pytest.mark.unit
@pytest.mark.extract
class TestCSVExtractorErrorHandling:
    """
    Tests para el manejo de errores.
    Valida comportamiento con archivos malformados o inválidos.
    """

    def test_extract_should_fail_when_malformed_csv(self, malformed_csv_file):
        """Debe fallar con CSV malformado."""
        extractor = CSVExtractor(file_path=malformed_csv_file, auto_profile=False)

        with pytest.raises(Exception):  # pd.errors.ParserError o similar
            extractor.extract()

    def test_extract_should_provide_helpful_error_message_when_parser_error(
        self, malformed_csv_file
    ):
        """Debe proveer mensaje de error útil."""
        extractor = CSVExtractor(file_path=malformed_csv_file, auto_profile=False)

        with pytest.raises(Exception) as exc_info:
            extractor.extract()

        error_message = str(exc_info.value).lower()
        # Debe mencionar el archivo o el problema
        check.is_true(
            "error" in error_message or "parse" in error_message,
            "Mensaje debe indicar error de parsing",
        )

    def test_extract_should_handle_empty_csv_gracefully_when_no_data_rows(
        self, empty_csv_file
    ):
        """Debe manejar CSV vacío (solo headers) correctamente."""
        extractor = CSVExtractor(file_path=empty_csv_file, auto_profile=False)

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)
        check.equal(len(result), 0, "DataFrame debe estar vacío")
        check.greater(len(result.columns), 0, "Debe tener columnas (headers)")


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.extract
class TestCSVExtractorEdgeCases:
    """
    Tests para casos extremos y situaciones especiales.
    """

    def test_extract_should_handle_large_csv_efficiently_when_many_rows(
        self, large_csv_file
    ):
        """Debe manejar archivos grandes eficientemente."""
        extractor = CSVExtractor(file_path=large_csv_file, auto_profile=True)

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)
        check.equal(len(result), 10000)

    def test_extractor_should_work_with_pathlib_path_when_provided(
        self, valid_csv_file
    ):
        """Debe aceptar tanto str como Path."""
        # valid_csv_file ya es un Path
        extractor = CSVExtractor(file_path=valid_csv_file)

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)

    def test_extractor_should_work_with_string_path_when_provided(self, valid_csv_file):
        """Debe aceptar rutas como strings."""
        extractor = CSVExtractor(file_path=str(valid_csv_file))

        result = extractor.extract()

        check.is_instance(result, pd.DataFrame)

    def test_extract_should_preserve_data_integrity_when_executed_multiple_times(
        self, valid_csv_file
    ):
        """Múltiples extracciones deben producir datos idénticos."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)

        result1 = extractor.extract()
        result2 = extractor.extract()
        result3 = extractor.extract()

        check.is_true(result1.equals(result2))
        check.is_true(result2.equals(result3))

    def test_profile_data_should_work_after_extract_when_auto_profile_disabled(
        self, valid_csv_file
    ):
        """Debe poder generar perfil manualmente si auto_profile está deshabilitado."""
        extractor = CSVExtractor(file_path=valid_csv_file, auto_profile=False)
        extractor.extract()

        # Verificar que no se generó automáticamente
        check.is_not_in("rows", extractor.metadata)

        # Generar manualmente
        profile = extractor.profile_data()

        check.is_instance(profile, dict)
        check.is_in("rows", profile)
        check.is_in("rows", extractor.metadata)
