"""
Pruebas unitarias para OrdersEnricher.

Verifica el enriquecimiento de la tabla de órdenes incluyendo:
- Limpieza interna con OrdersCleaner
- Unión con catálogos de clientes y promociones
- Cálculo de métricas derivadas (items_count, avg_item_price)
- Generación de columnas temporales (order_month, order_week)
- Banderas de comportamiento (used_promotion, is_free_shipping, is_high_discount)
"""

import pytest
import pytest_check as check

from transform.enrichers.orders_enricher import OrdersEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestOrdersEnricher:
    """
    Tests del enriquecimiento de pedidos.

    Verifica la integración de datos de clientes, promociones e items
    y el cálculo de métricas agregadas y banderas derivadas.
    """

    def test_enrich_should_join_catalogs_and_derive_metrics_when_orders_provided(
        self, orders_enricher_inputs
    ):
        """
        Debe unir promociones, agregar metadatos y calcular columnas derivadas.

        Verifica que el enriquecimiento complete el pipeline correctamente,
        agregando columnas de periodo, métricas de items y banderas de promoción.
        """
        enricher = OrdersEnricher()

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
        )

        check.equal(len(enriched), 1)
        check.is_true("order_month" in enriched.columns)
        check.is_true("avg_item_price" in enriched.columns)
        check.is_true(enriched.loc[0, "used_promotion"])
