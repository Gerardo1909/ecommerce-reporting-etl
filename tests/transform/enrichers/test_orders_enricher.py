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

    def test_enrich_should_include_customer_columns_when_customers_joined(
        self, orders_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de clientes tras el join.

        Verifica que _join_customer_data agrega segment, registration_date,
        city, country y email desde la tabla de clientes.
        """
        enricher = OrdersEnricher()

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
        )

        row = enriched.iloc[0]
        check.is_in("segment", enriched.columns)
        check.is_in("registration_date", enriched.columns)
        check.is_in("city", enriched.columns)
        check.is_in("country", enriched.columns)
        check.is_in("email", enriched.columns)
        check.equal(row["segment"], "vip")
        check.equal(row["city"], "CityA")
        check.equal(row["country"], "CountryA")
        check.equal(row["email"], "vip@example.com")

    def test_enrich_should_include_promotion_columns_when_promotions_joined(
        self, orders_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de promociones tras el join.

        Verifica que _join_promotion_data agrega promotion_type, discount_value,
        start_date, end_date e is_active desde la tabla de promociones.
        """
        enricher = OrdersEnricher()

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
        )

        row = enriched.iloc[0]
        check.is_in("promotion_type", enriched.columns)
        check.is_in("discount_value", enriched.columns)
        check.is_in("start_date", enriched.columns)
        check.is_in("end_date", enriched.columns)
        check.is_in("is_active", enriched.columns)
        check.equal(row["promotion_type"], "coupon")
        check.equal(row["discount_value"], 10)
        check.is_true(row["is_active"])

    def test_enrich_should_calculate_items_count_and_avg_price_when_order_items_aggregated(
        self, orders_enricher_inputs
    ):
        """
        Debe calcular items_count y avg_item_price correctamente.

        Verifica que _calculate_order_products_count_and_average_price
        suma las cantidades de ítems y calcula el precio promedio
        dividiendo total_amount entre items_count.
        """
        enricher = OrdersEnricher()

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
        )

        row = enriched.iloc[0]
        # order_items tiene quantity 1 + 2 = 3
        check.is_in("items_count", enriched.columns)
        check.is_in("avg_item_price", enriched.columns)
        check.equal(row["items_count"], 3)
        # avg_item_price = total_amount (109.0) / items_count (3) ≈ 36.33
        check.almost_equal(row["avg_item_price"], 109.0 / 3, rel=0.01)

    def test_enrich_should_generate_temporal_and_flag_columns_when_derived_columns_added(
        self, orders_enricher_inputs
    ):
        """
        Debe generar columnas temporales y banderas de comportamiento.

        Verifica que _add_derived_columns crea order_month, order_week,
        used_promotion, is_free_shipping e is_high_discount basándose
        en los valores de la orden.
        """
        enricher = OrdersEnricher()

        enriched = enricher.enrich(
            orders_df=orders_enricher_inputs["orders"],
            customers_df=orders_enricher_inputs["customers"],
            promotions_df=orders_enricher_inputs["promotions"],
            order_items_df=orders_enricher_inputs["order_items"],
        )

        row = enriched.iloc[0]
        # Columnas temporales
        check.is_in("order_month", enriched.columns)
        check.is_in("order_week", enriched.columns)
        check.equal(str(row["order_month"]), "2024-01")
        # Banderas de comportamiento
        check.is_in("used_promotion", enriched.columns)
        check.is_in("is_free_shipping", enriched.columns)
        check.is_in("is_high_discount", enriched.columns)
        # promotion_id=100 -> used_promotion=True
        check.is_true(row["used_promotion"])
        # shipping_cost=0.0 -> is_free_shipping=True
        check.is_true(row["is_free_shipping"])
        # discount_percent=10.0 < 20 -> is_high_discount=False
        check.is_false(row["is_high_discount"])
