"""
Módulo de transformación de datos.
"""

# Cleaners
from transform.cleaners.base_cleaner import DataCleaner, NullStrategy
from transform.cleaners.orders_cleaner import OrdersCleaner
from transform.cleaners.inventory_cleaner import InventoryCleaner
from transform.cleaners.reviews_cleaner import ReviewsCleaner

# Enrichers
from transform.enrichers.orders_enricher import OrdersEnricher
from transform.enrichers.inventory_enricher import InventoryEnricher
from transform.enrichers.reviews_enricher import ReviewsEnricher

# Aggregators
from transform.aggregators.customer_analytics import CustomerAnalyticsAggregator
from transform.aggregators.product_analytics import ProductAnalyticsAggregator
from transform.aggregators.sales_analytics import SalesAnalyticsAggregator
from transform.aggregators.inventory_analytics import InventoryAnalyticsAggregator
from transform.aggregators.review_analytics import ReviewAnalyticsAggregator
from transform.aggregators.order_lifecycle import OrderLifecycleAggregator

__all__ = [
    # Cleaners
    "DataCleaner",
    "NullStrategy",
    "OrdersCleaner",
    "InventoryCleaner",
    "ReviewsCleaner",
    # Enrichers
    "OrdersEnricher",
    "InventoryEnricher",
    "ReviewsEnricher",
    # Aggregators
    "CustomerAnalyticsAggregator",
    "ProductAnalyticsAggregator",
    "SalesAnalyticsAggregator",
    "InventoryAnalyticsAggregator",
    "ReviewAnalyticsAggregator",
    "OrderLifecycleAggregator",
]
