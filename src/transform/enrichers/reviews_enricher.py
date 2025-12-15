"""
Módulo que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
"""

import pandas as pd

from transform.cleaners.reviews_cleaner import ReviewsCleaner
from utils.logger import transform_logger
from utils.validators import SchemaValidator


class ReviewsEnricher:
    """
    Clase que se encarga del enriquecimiento de la tabla "reviews" para posterior análisis.
    """

    def __init__(self):
        self.cleaner = ReviewsCleaner()
        self.logger = transform_logger

    def enrich(
        self,
        reviews_df: pd.DataFrame,
        products_df: pd.DataFrame,
        customers_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Ejecuta el pipeline de enriquecimiento y devuelve tabla de reviews lista para análisis de agregación.
        """
        self.logger.info("Iniciando enriquecimiento de tabla 'reviews'")
        reviews_df = self._clean_reviews(reviews_df)
        enriched_df = self._join_products(reviews_df, products_df)
        enriched_df = self._join_customers(enriched_df, customers_df)
        enriched_df = self._add_derived_columns(enriched_df)
        self.logger.info(
            "Enriquecimiento de tabla 'reviews' completado: %s filas", len(enriched_df)
        )
        return enriched_df

    def _clean_reviews(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        return self.cleaner.clean(reviews_df)

    def _join_products(
        self, reviews_df: pd.DataFrame, products_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["product_id", "product_name", "category_id", "brand_id"]
        validator = SchemaValidator(products_df, self.logger)
        validator.validate_required_columns(cols)
        validator.validate_no_nulls()
        return reviews_df.merge(products_df[cols], on="product_id", how="left")

    def _join_customers(
        self, reviews_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        cols = ["customer_id", "segment", "city", "country"]
        validator = SchemaValidator(customers_df, self.logger)
        validator.validate_required_columns(["customer_id"])
        return reviews_df.merge(customers_df[cols], on="customer_id", how="left")

    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["review_month"] = df["created_at"].dt.to_period("M")
        df["comment_length"] = df["comment"].fillna("").str.len()
        df["is_positive"] = df["rating"] >= 4
        df["is_negative"] = df["rating"] <= 2
        return df
