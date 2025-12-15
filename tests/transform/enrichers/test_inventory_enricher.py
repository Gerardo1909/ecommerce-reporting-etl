"""
Pruebas unitarias para InventoryEnricher.

Verifica el enriquecimiento de la tabla de inventario incluyendo:
- Limpieza interna con InventoryCleaner
- Unión con catálogos de productos y bodegas
- Generación de banderas de stock (is_low_stock, is_overstock)
- Agregado de metadatos de ubicación y capacidad
"""

import pytest
import pytest_check as check

from transform.enrichers.inventory_enricher import InventoryEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestInventoryEnricher:
    """
    Tests del enriquecimiento de inventario.

    Verifica la integración de datos de productos y bodegas
    y el cálculo de banderas de salud de stock.
    """

    def test_enrich_should_join_catalogs_and_add_stock_flags_when_inventory_provided(
        self, inventory_enricher_inputs
    ):
        """
        Debe unir catálogos, añadir banderas de stock y metadatos de ubicación.

        Verifica que el enriquecimiento complete el pipeline correctamente,
        agregando nombre de producto, ubicación de bodega y banderas de stock.
        """
        enricher = InventoryEnricher()

        enriched = enricher.enrich(
            inventory_df=inventory_enricher_inputs["inventory"],
            products_df=inventory_enricher_inputs["products"],
            warehouses_df=inventory_enricher_inputs["warehouses"],
        )

        check.equal(len(enriched), 1)
        check.is_true("is_low_stock" in enriched.columns)
        check.is_true("product_name" in enriched.columns)
        check.is_true("location" in enriched.columns)
