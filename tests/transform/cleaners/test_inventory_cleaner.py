"""
Pruebas unitarias para InventoryCleaner.

Este módulo valida:
- Validación de nulos en columnas clave (inventory_id, product_id, warehouse_id)
- Relleno de columnas numéricas (quantity, min_stock_level, max_stock_level)
- Conversión de tipos numéricos y fechas
- Pipeline completo de limpieza

Fixtures utilizados:
- raw_inventory_dirty: Datos con nulos en claves (para probar excepciones)
- raw_inventory_valid_keys: Datos con claves completas para probar relleno
"""

import pytest
import pytest_check as check
import pandas as pd

from exceptions import NullConstraintError
from transform.cleaners.inventory_cleaner import InventoryCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerNulls:
    """
    Tests de `_handle_nulls` para validación de claves y relleno numérico.

    Verifica que:
    - Se lanza NullConstraintError cuando hay nulos en columnas clave
    - Se rellenan columnas numéricas con estrategias apropiadas
    """

    def test_handle_nulls_should_raise_null_constraint_error_when_key_columns_have_nulls(
        self, raw_inventory_dirty
    ):
        """
        Dado un DataFrame con nulos en inventory_id, product_id o warehouse_id,
        Cuando se ejecuta _handle_nulls,
        Entonces debe lanzar NullConstraintError indicando las columnas afectadas.
        """
        cleaner = InventoryCleaner()

        with pytest.raises(NullConstraintError) as exc_info:
            cleaner._handle_nulls(raw_inventory_dirty.copy())

        check.is_in("inventory_id", str(exc_info.value))

    def test_handle_nulls_should_fill_numeric_columns_when_key_columns_are_valid(
        self, raw_inventory_valid_keys
    ):
        """
        Dado un DataFrame con claves completas pero nulos en columnas numéricas,
        Cuando se ejecuta _handle_nulls,
        Entonces debe rellenar quantity, min_stock_level y max_stock_level.
        """
        cleaner = InventoryCleaner()

        cleaned = cleaner._handle_nulls(raw_inventory_valid_keys.copy())

        check.equal(len(cleaned), 3)
        check.equal(
            cleaned[["quantity", "min_stock_level", "max_stock_level"]]
            .isna()
            .sum()
            .sum(),
            0,
        )


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerConvert:
    """
    Tests de `_convert_types` para normalizar tipos numéricos y fechas.

    Verifica que las columnas numéricas se convierten correctamente
    y que las fechas se parsean a datetime.
    """

    def test_convert_types_should_normalize_numeric_and_dates_when_inventory_dirty(
        self, raw_inventory_valid_keys
    ):
        """
        Dado un DataFrame con tipos mixtos,
        Cuando se ejecuta _convert_types,
        Entonces quantity debe ser numérico y last_restock_date datetime.
        """
        cleaner = InventoryCleaner()

        converted = cleaner._convert_types(raw_inventory_valid_keys.copy())

        check.is_true(pd.api.types.is_numeric_dtype(converted["quantity"]))
        check.is_true(
            pd.api.types.is_datetime64_any_dtype(converted["last_restock_date"])
        )


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryCleanerCleanPipeline:
    """
    Tests del método `clean` para validar el pipeline completo.

    Verifica que el pipeline ejecuta todas las etapas correctamente
    y produce un DataFrame con claves completas.
    """

    def test_clean_should_raise_null_constraint_error_when_keys_have_nulls(
        self, raw_inventory_dirty
    ):
        """
        Dado un DataFrame con nulos en columnas clave,
        Cuando se ejecuta el pipeline clean,
        Entonces debe lanzar NullConstraintError durante _handle_nulls.
        """
        cleaner = InventoryCleaner()

        with pytest.raises(NullConstraintError):
            cleaner.clean(raw_inventory_dirty)

    def test_clean_should_return_inventory_with_required_keys_when_pipeline_executes(
        self, raw_inventory_valid_keys
    ):
        """
        Dado un DataFrame con claves completas,
        Cuando se ejecuta el pipeline clean,
        Entonces debe retornar inventario con identificadores presentes.
        """
        cleaner = InventoryCleaner()

        cleaned = cleaner.clean(raw_inventory_valid_keys)

        check.is_true((cleaned["inventory_id"].notna()).all())
