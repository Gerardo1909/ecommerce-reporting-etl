"""
Pruebas unitarias para ReviewsCleaner.

Este módulo valida:
- Validación de nulos en columnas clave (review_id, customer_id, rating, created_at)
- Relleno de columnas opcionales (helpful_votes)
- Conversión de tipos y validación de rangos
- Pipeline completo de limpieza

Fixtures utilizados:
- raw_reviews_dirty: Datos con nulos en claves (para probar excepciones)
- raw_reviews_valid_keys: Datos con claves completas para probar relleno
"""

import pytest
import pytest_check as check
import pandas as pd

from exceptions import NullConstraintError
from transform.cleaners.reviews_cleaner import ReviewsCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerNulls:
    """
    Tests de `handle_nulls` para validación de claves y relleno de votos.

    Verifica que:
    - Se lanza NullConstraintError cuando hay nulos en columnas clave
    - Se rellenan columnas opcionales como helpful_votes
    """

    def test_handle_nulls_should_raise_null_constraint_error_when_key_columns_have_nulls(
        self, raw_reviews_dirty
    ):
        """
        Dado un DataFrame con nulos en review_id, customer_id, rating o created_at,
        Cuando se ejecuta handle_nulls,
        Entonces debe lanzar NullConstraintError indicando las columnas afectadas.
        """
        cleaner = ReviewsCleaner()

        with pytest.raises(NullConstraintError) as exc_info:
            cleaner.handle_nulls(raw_reviews_dirty.copy())

        check.is_in("review_id", str(exc_info.value))

    def test_handle_nulls_should_fill_helpful_votes_when_key_columns_are_valid(
        self, raw_reviews_valid_keys
    ):
        """
        Dado un DataFrame con claves completas pero nulos en helpful_votes,
        Cuando se ejecuta handle_nulls,
        Entonces debe rellenar helpful_votes sin lanzar excepción.
        """
        cleaner = ReviewsCleaner()

        cleaned = cleaner.handle_nulls(raw_reviews_valid_keys.copy())

        check.equal(len(cleaned), 3)
        check.equal(cleaned["helpful_votes"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerConvert:
    """
    Tests de `convert_types` para normalizar calificaciones y fechas.

    Verifica que rating se convierte a numérico y created_at a datetime.
    """

    def test_convert_types_should_normalize_rating_and_date_when_reviews_valid(
        self, raw_reviews_valid_keys
    ):
        """
        Dado un DataFrame con tipos mixtos,
        Cuando se ejecuta convert_types,
        Entonces rating debe ser numérico y created_at datetime.
        """
        cleaner = ReviewsCleaner()

        converted = cleaner.convert_types(raw_reviews_valid_keys.copy())

        check.is_true(pd.api.types.is_numeric_dtype(converted["rating"]))
        check.is_true(pd.api.types.is_datetime64_any_dtype(converted["created_at"]))


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerCleanPipeline:
    """
    Tests del método `clean` para validar el pipeline completo.

    Verifica que el pipeline ejecuta todas las etapas correctamente
    y produce un DataFrame con identificadores completos.
    """

    def test_clean_should_raise_null_constraint_error_when_keys_have_nulls(
        self, raw_reviews_dirty
    ):
        """
        Dado un DataFrame con nulos en columnas clave,
        Cuando se ejecuta el pipeline clean,
        Entonces debe lanzar NullConstraintError durante handle_nulls.
        """
        cleaner = ReviewsCleaner()

        with pytest.raises(NullConstraintError):
            cleaner.clean(raw_reviews_dirty)

    def test_clean_should_return_required_identifiers_when_pipeline_runs(
        self, raw_reviews_valid_keys
    ):
        """
        Dado un DataFrame con claves completas,
        Cuando se ejecuta el pipeline clean,
        Entonces debe devolver reseñas con identificadores completos.
        """
        cleaner = ReviewsCleaner()

        cleaned = cleaner.clean(raw_reviews_valid_keys)

        check.is_true(
            (cleaned[["review_id", "product_id", "customer_id"]].notna().all()).all()
        )
