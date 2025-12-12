"""
Clase base abstracta para extractores de datos
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime

import pandas as pd

from utils.logger import extract_logger


class BaseExtractor(ABC):
    """
    Clase base abstracta para todos los extractores de datos.
    Define la interfaz común para extracción, validación y profiling.

    NO asume ninguna fuente específica (archivos, DB, API, etc.).
    Cada subclase define qué parámetros necesita en su constructor.
    """

    def __init__(self):
        """
        Inicializa el extractor base.
        """
        self.source_description: str = ""
        self.data: Optional[pd.DataFrame] = None
        self.metadata: Dict[str, Any] = {
            "extraction_timestamp": None,
            "extractor_type": self.__class__.__name__,
        }
        self.logger = extract_logger

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extrae datos desde la fuente.

        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        raise NotImplementedError()

    @abstractmethod
    def _validate_source_exists(self) -> None:
        """
        Verifica que la fuente de datos especificada existe.

        Raises:
            FileNotFoundError: Si la fuente no existe
        """
        raise NotImplementedError()

    def get_data(self) -> pd.DataFrame:
        """
        Retorna el DataFrame con los datos extraídos.

        Returns:
            pd.DataFrame: Datos extraídos

        Raises:
            ValueError: Si no hay datos cargados
        """
        if self.data is None:
            raise ValueError("No hay datos cargados. Ejecuta extract() primero.")
        return self.data

    def profile_data(self) -> Dict[str, Any]:
        """
        Genera un perfil completo de los datos extraídos.

        Incluye información sobre dimensiones, valores nulos, tipos de datos
        y uso de memoria. Esta información es útil para auditoría y debugging.

        Returns:
            Dict[str, Any]: Diccionario con estadísticas del DataFrame

        Raises:
            ValueError: Si no hay datos cargados
        """
        if self.data is None:
            raise ValueError("No hay datos cargados. Ejecuta extract() primero.")

        profile = {
            "rows": self.data.shape[0],
            "columns": self.data.shape[1],
            "column_names": list(self.data.columns),
            "missing_values": int(self.data.isnull().sum().sum()),
            "missing_percentage": round(
                (
                    self.data.isnull().sum().sum()
                    / (self.data.shape[0] * self.data.shape[1])
                )
                * 100,
                2,
            ),
            "dtypes": self.data.dtypes.astype(str).to_dict(),
            "memory_usage_mb": round(
                self.data.memory_usage(deep=True).sum() / 1024**2, 2
            ),
        }

        self.metadata.update(profile)

        info_msg = (
            f"Perfil de datos: {profile['rows']} filas, "
            f"{profile['columns']} columnas, "
            f"{profile['missing_values']} valores nulos "
            f"({profile['missing_percentage']}%)"
        )
        self.logger.info(info_msg)
        print(info_msg)

        return profile

    def get_metadata(self) -> Dict[str, Any]:
        """
        Retorna el diccionario de metadata de la extracción.

        Incluye información sobre la fuente, timestamp, estadísticas
        del DataFrame y cualquier información adicional agregada
        durante el proceso.

        Returns:
            Dict[str, Any]: Metadata de la extracción
        """
        return self.metadata

    def _update_extraction_timestamp(self) -> None:
        """
        Actualiza el timestamp de extracción en la metadata.

        Este método debe ser llamado por las subclases al finalizar
        exitosamente la extracción.
        """
        self.metadata["extraction_timestamp"] = datetime.now().isoformat()

    def get_summary(self) -> str:
        """
        Genera un resumen legible de la extracción.

        Returns:
            str: Resumen formateado de la extracción
        """
        if self.data is None:
            return "No hay datos cargados."

        summary_lines = [
            f"\n{'=' * 60}",
            f"Resumen de Extracción - {self.metadata['extractor_type']}",
            f"{'=' * 60}",
            f"Fuente: {self.source_description}",
            f"Timestamp: {self.metadata.get('extraction_timestamp', 'N/A')}",
            f"Dimensiones: {self.data.shape[0]} filas x {self.data.shape[1]} columnas",
            f"Valores nulos: {self.metadata.get('missing_values', 'N/A')} "
            f"({self.metadata.get('missing_percentage', 'N/A')}%)",
            f"Memoria: {self.metadata.get('memory_usage_mb', 'N/A')} MB",
            f"{'=' * 60}\n",
        ]

        return "\n".join(summary_lines)
