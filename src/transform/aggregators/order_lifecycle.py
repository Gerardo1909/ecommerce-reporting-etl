"""
Agregador para el ciclo de vida de órdenes (funnel de estado).
"""

import pandas as pd

from utils.logger import transform_logger


class OrderLifecycleAggregator:
    """
    Calcula métricas de estado de órdenes: funnel por status, cancelación, entregas y backlog.
    """

    def __init__(self):
        self.logger = transform_logger

    def status_funnel(self, enriched_orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cuenta órdenes por status, calcula la participación de cada estado y devuelve el funnel
        ordenado con totales y proporciones.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            DataFrame con conteo y participación por estado de órdenes
        """
        counts = enriched_orders_df["status"].value_counts().reset_index()
        counts.columns = ["status", "orders"]
        total = counts["orders"].sum()
        counts["share"] = counts["orders"] / total if total else 0
        self.logger.info("Funnel por estado generado: %s estados", len(counts))
        return counts

    def cancellation_rate(self, enriched_orders_df: pd.DataFrame) -> float:
        """
        Calcula la tasa de cancelación sobre el total de órdenes disponibles.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            Tasa de cancelación como float
        """
        rate = (enriched_orders_df["status"].str.lower() == "cancelled").mean()
        self.logger.info("Tasa de cancelación: %.2f", rate)
        return float(rate)

    def in_progress_backlog(self, enriched_orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra órdenes en curso (pending, processing, shipped), agrega backlog por mes en conteo
        y valor total, y devuelve la serie temporal ordenada.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            DataFrame con backlog en progreso por mes
        """
        in_progress_status = {"pending", "processing", "shipped"}
        filtered = enriched_orders_df[
            enriched_orders_df["status"].str.lower().isin(in_progress_status)
        ].copy()
        grouped = (
            filtered.groupby("order_month")
            .agg(
                backlog_orders=("order_id", "count"),
                backlog_value=("total_amount", "sum"),
            )
            .reset_index()
            .sort_values("order_month")
        )
        self.logger.info("Backlog en progreso calculado: %s periodos", len(grouped))
        return grouped

    def delivery_rate(self, enriched_orders_df: pd.DataFrame) -> float:
        """
        Calcula la tasa de entrega sobre el total de órdenes.

        Args:
            enriched_orders_df: DataFrame de órdenes enriquecidas

        Returns:
            Tasa de entrega como float
        """
        rate = (enriched_orders_df["status"].str.lower() == "delivered").mean()
        self.logger.info("Tasa de entrega: %.2f", rate)
        return float(rate)
