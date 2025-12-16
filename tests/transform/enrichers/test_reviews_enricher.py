"""
Pruebas unitarias para ReviewsEnricher.

Verifica el enriquecimiento de la tabla de reseñas incluyendo:
- Limpieza interna con ReviewsCleaner
- Unión con catálogos de productos y clientes
- Generación de métricas derivadas (review_month, comment_length)
- Banderas de sentimiento (is_positive, is_negative)
"""

import pytest
import pytest_check as check

from transform.enrichers.reviews_enricher import ReviewsEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsEnricher:
    """
    Tests del enriquecimiento de reseñas.

    Verifica la integración de datos de productos y clientes
    y el cálculo de banderas de sentimiento y métricas de contenido.
    """

    def test_enrich_should_include_product_columns_when_products_joined(
        self, reviews_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de productos tras el join.

        Verifica que _join_products agrega product_name, category_id
        y brand_id desde la tabla de productos.
        """
        enricher = ReviewsEnricher()

        enriched = enricher.enrich(
            reviews_df=reviews_enricher_inputs["reviews"],
            products_df=reviews_enricher_inputs["products"],
            customers_df=reviews_enricher_inputs["customers"],
        )

        row_gadget = enriched[enriched["product_id"] == 10].iloc[0]
        row_widget = enriched[enriched["product_id"] == 11].iloc[0]
        check.is_in("product_name", enriched.columns)
        check.is_in("category_id", enriched.columns)
        check.is_in("brand_id", enriched.columns)
        check.equal(row_gadget["product_name"], "Gadget")
        check.equal(row_gadget["category_id"], 1000)
        check.equal(row_gadget["brand_id"], 2000)
        check.equal(row_widget["product_name"], "Widget")

    def test_enrich_should_include_customer_columns_when_customers_joined(
        self, reviews_enricher_inputs
    ):
        """
        Debe incluir columnas del catálogo de clientes tras el join.

        Verifica que _join_customers agrega segment, city y country
        desde la tabla de clientes.
        """
        enricher = ReviewsEnricher()

        enriched = enricher.enrich(
            reviews_df=reviews_enricher_inputs["reviews"],
            products_df=reviews_enricher_inputs["products"],
            customers_df=reviews_enricher_inputs["customers"],
        )

        row_vip = enriched[enriched["customer_id"] == 101].iloc[0]
        row_standard = enriched[enriched["customer_id"] == 102].iloc[0]
        check.is_in("segment", enriched.columns)
        check.is_in("city", enriched.columns)
        check.is_in("country", enriched.columns)
        check.equal(row_vip["segment"], "vip")
        check.equal(row_vip["city"], "X")
        check.equal(row_vip["country"], "AR")
        check.equal(row_standard["segment"], "standard")

    def test_enrich_should_generate_temporal_and_sentiment_columns_when_derived_columns_added(
        self, reviews_enricher_inputs
    ):
        """
        Debe generar columnas temporales y banderas de sentimiento.

        Verifica que _add_derived_columns crea review_month, comment_length,
        is_positive e is_negative basándose en los valores de la reseña.
        """
        enricher = ReviewsEnricher()

        enriched = enricher.enrich(
            reviews_df=reviews_enricher_inputs["reviews"],
            products_df=reviews_enricher_inputs["products"],
            customers_df=reviews_enricher_inputs["customers"],
        )

        row_positive = enriched[enriched["rating"] == 5].iloc[0]
        row_negative = enriched[enriched["rating"] == 2].iloc[0]
        # Columnas temporales
        check.is_in("review_month", enriched.columns)
        check.equal(str(row_positive["review_month"]), "2024-01")
        # Longitud de comentario: "great" = 5, "bad" = 3
        check.is_in("comment_length", enriched.columns)
        check.equal(row_positive["comment_length"], 5)
        check.equal(row_negative["comment_length"], 3)
        # Banderas de sentimiento: rating >= 4 es positivo, rating <= 2 es negativo
        check.is_in("is_positive", enriched.columns)
        check.is_in("is_negative", enriched.columns)
        check.is_true(row_positive["is_positive"])
        check.is_false(row_positive["is_negative"])
        check.is_false(row_negative["is_positive"])
        check.is_true(row_negative["is_negative"])
