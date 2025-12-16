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

    def test_enrich_should_include_product_columns_when_products_joined(
        self, inventory_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de productos tras el join.

        Verifica que _join_products agrega product_name, category_id
        y brand_id desde la tabla de productos.
        """
        enricher = InventoryEnricher()

        enriched = enricher.enrich(
            inventory_df=inventory_enricher_inputs["inventory"],
            products_df=inventory_enricher_inputs["products"],
            warehouses_df=inventory_enricher_inputs["warehouses"],
        )

        row = enriched.iloc[0]
        check.is_in("product_name", enriched.columns)
        check.is_in("category_id", enriched.columns)
        check.is_in("brand_id", enriched.columns)
        check.equal(row["product_name"], "Gadget")
        check.equal(row["category_id"], 1000)
        check.equal(row["brand_id"], 2000)

    def test_enrich_should_include_warehouse_columns_when_warehouses_joined(
        self, inventory_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de bodegas tras el join.

        Verifica que _join_warehouses agrega location, capacity_units
        y current_occupancy desde la tabla de bodegas.
        """
        enricher = InventoryEnricher()

        enriched = enricher.enrich(
            inventory_df=inventory_enricher_inputs["inventory"],
            products_df=inventory_enricher_inputs["products"],
            warehouses_df=inventory_enricher_inputs["warehouses"],
        )

        row = enriched.iloc[0]
        check.is_in("location", enriched.columns)
        check.is_in("capacity_units", enriched.columns)
        check.is_in("current_occupancy", enriched.columns)
        check.equal(row["location"], "LocationA")
        check.equal(row["capacity_units"], 500)
        check.equal(row["current_occupancy"], 250)

    def test_enrich_should_generate_stock_flags_when_derived_columns_added(
        self, inventory_enricher_inputs
    ):
        """
        Debe generar banderas de salud de stock.

        Verifica que _add_derived_columns crea is_low_stock e is_overstock
        comparando quantity con min_stock_level y max_stock_level.
        Con quantity=50, min=10, max=100: is_low_stock=False, is_overstock=False.
        """
        enricher = InventoryEnricher()

        enriched = enricher.enrich(
            inventory_df=inventory_enricher_inputs["inventory"],
            products_df=inventory_enricher_inputs["products"],
            warehouses_df=inventory_enricher_inputs["warehouses"],
        )

        row = enriched.iloc[0]
        check.is_in("is_low_stock", enriched.columns)
        check.is_in("is_overstock", enriched.columns)
        # quantity=50, min_stock_level=10 -> 50 <= 10 es False
        check.is_false(row["is_low_stock"])
        # quantity=50, max_stock_level=100 -> 50 >= 100 es False
        check.is_false(row["is_overstock"])
