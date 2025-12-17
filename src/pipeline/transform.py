"""
Etapa de transformación del pipeline ETL.
"""

import sys
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

# Agregar config al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    TOP_SPENDERS_N,
    TOP_SPENDERS_PERCENTILE,
    RECURRING_CUSTOMERS_MIN_ORDERS,
    TOP_PRODUCTS_N,
    LOW_STOCK_ITEMS_N,
    MIN_REVIEWS_FOR_PRODUCT,
    TOP_REVIEWED_PRODUCTS_N,
)
from transform import (
    OrdersCleaner,
    InventoryCleaner,
    ReviewsCleaner,
    OrdersEnricher,
    InventoryEnricher,
    ReviewsEnricher,
    CustomerAnalyticsAggregator,
    ProductAnalyticsAggregator,
    SalesAnalyticsAggregator,
    InventoryAnalyticsAggregator,
    ReviewAnalyticsAggregator,
    OrderLifecycleAggregator,
)
from utils.logger import transform_logger, run_context, log_stage


def _clean_and_enrich(
    tables: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """Limpia y enriquece los datasets."""
    # --- Órdenes ---
    orders_cleaner = OrdersCleaner()
    cleaned_orders = orders_cleaner.clean(tables["orders"])

    orders_enricher = OrdersEnricher()
    enriched_orders = orders_enricher.enrich(
        orders_df=cleaned_orders,
        customers_df=tables["customers"],
        promotions_df=tables["promotions"],
        order_items_df=tables["order_items"],
    )

    # --- Inventario ---
    inventory_cleaner = InventoryCleaner()
    cleaned_inventory = inventory_cleaner.clean(tables["inventory"])

    inventory_enricher = InventoryEnricher()
    enriched_inventory = inventory_enricher.enrich(
        inventory_df=cleaned_inventory,
        products_df=tables["products"],
        warehouses_df=tables["warehouses"],
    )

    # --- Reviews ---
    reviews_cleaner = ReviewsCleaner()
    cleaned_reviews = reviews_cleaner.clean(tables["reviews"])

    reviews_enricher = ReviewsEnricher()
    enriched_reviews = reviews_enricher.enrich(
        reviews_df=cleaned_reviews,
        products_df=tables["products"],
        customers_df=tables["customers"],
    )

    return {
        "orders": enriched_orders,
        "inventory": enriched_inventory,
        "reviews": enriched_reviews,
    }


def _aggregate(
    enriched: Dict[str, pd.DataFrame], tables: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    """Genera métricas de negocio a partir de datos enriquecidos."""
    enriched_orders = enriched["orders"]
    enriched_inventory = enriched["inventory"]
    enriched_reviews = enriched["reviews"]

    # Inicializar agregadores
    customer_agg = CustomerAnalyticsAggregator()
    product_agg = ProductAnalyticsAggregator()
    sales_agg = SalesAnalyticsAggregator()
    inventory_agg = InventoryAnalyticsAggregator()
    review_agg = ReviewAnalyticsAggregator()
    lifecycle_agg = OrderLifecycleAggregator()

    return {
        # Customer Analytics
        "top_spenders": customer_agg.top_spenders(
            enriched_orders, top_n=TOP_SPENDERS_N, percentile=TOP_SPENDERS_PERCENTILE
        ),
        "recurring_customers": customer_agg.recurring_customers(
            enriched_orders, min_orders=RECURRING_CUSTOMERS_MIN_ORDERS
        ),
        "average_ticket": pd.DataFrame(
            {"average_ticket": [customer_agg.average_ticket_overall(enriched_orders)]}
        ),
        # Product Analytics
        "top_products": product_agg.top_products_by_quantity(
            order_items_df=tables["order_items"],
            products_df=tables["products"],
            top_n=TOP_PRODUCTS_N,
        ),
        # Sales Analytics
        "monthly_sales": sales_agg.monthly_sales(enriched_orders),
        "promotion_usage_rate": pd.DataFrame(
            {"promotion_usage_rate": [sales_agg.promotion_usage_rate(enriched_orders)]}
        ),
        # Order Lifecycle
        "status_funnel": lifecycle_agg.status_funnel(enriched_orders),
        "cancellation_rate": pd.DataFrame(
            {"cancellation_rate": [lifecycle_agg.cancellation_rate(enriched_orders)]}
        ),
        "delivery_rate": pd.DataFrame(
            {"delivery_rate": [lifecycle_agg.delivery_rate(enriched_orders)]}
        ),
        "backlog_in_progress": lifecycle_agg.in_progress_backlog(enriched_orders),
        # Inventory Analytics
        "inventory_health": inventory_agg.stock_health_summary(enriched_inventory),
        "low_stock_items": inventory_agg.low_stock_items(
            enriched_inventory, top_n=LOW_STOCK_ITEMS_N
        ),
        "warehouse_utilization": inventory_agg.warehouse_utilization(
            enriched_inventory
        ),
        # Review Analytics
        "reviews_overview": review_agg.rating_overview(enriched_reviews),
        "reviews_by_product": review_agg.rating_by_product(
            enriched_reviews,
            min_reviews=MIN_REVIEWS_FOR_PRODUCT,
            top_n=TOP_REVIEWED_PRODUCTS_N,
        ),
        "reviews_monthly": review_agg.monthly_review_volume(enriched_reviews),
    }


@log_stage("Transformación", transform_logger)
def run(
    tables: Dict[str, pd.DataFrame],
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Ejecuta la transformación completa: limpieza, enriquecimiento y agregación.

    Args:
        tables: Diccionario con los DataFrames extraídos.

    Returns:
        Tuple con:
            - enriched: DataFrames enriquecidos
            - aggregated: Métricas de negocio calculadas
    """
    enriched = _clean_and_enrich(tables)
    aggregated = _aggregate(enriched, tables)

    # Registrar métricas
    total_enriched_rows = sum(df.shape[0] for df in enriched.values())
    run_context.record_stage_metric("Transformación", "tables_enriched", len(enriched))
    run_context.record_stage_metric(
        "Transformación", "total_enriched_rows", total_enriched_rows
    )
    run_context.record_stage_metric(
        "Transformación", "metrics_generated", len(aggregated)
    )

    return enriched, aggregated
