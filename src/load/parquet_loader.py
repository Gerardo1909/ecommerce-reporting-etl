"""
Loader especializado para archivos Parquet.
"""

from pathlib import Path
from typing import Any

import pandas as pd

from load.base_loader import BaseLoader


class ParquetLoader(BaseLoader):
    """
    Loader especializado para archivos Parquet.

    Proporciona funcionalidad específica para cargar archivos Parquet con
    opciones configurables como compresión, inclusión de índice, motor de lectura, etc.
    """

    def __init__(
        self,
        target_path: Path,
        compression: str = "snappy",
        index: bool = False,
        engine: str = "pyarrow",
    ):
        """
        Inicializa el loader de Parquet.

        Args:
            target_path: Ruta al archivo Parquet de destino
            compression: Método de compresión (default: 'snappy')
            index: Incluir índice en el archivo (default: False)
            engine: Motor para leer/escribir Parquet (default: 'pyarrow')

        Raises:
            TargetNotFoundError: Si el directorio de destino no existe
        """
        super().__init__()
        self._validate_target_exists(target_path)
        self.target_path = Path(target_path)
        self.target_description = str(self.target_path)
        self.compression = compression
        self.save_index = index
        self.engine = engine

        # Actualizar metadata con opciones de configuración
        self.metadata.update(
            {
                "target_path": str(self.target_path),
                "compression": self.compression,
                "engine": self.engine,
            }
        )

    def save(self, df: pd.DataFrame, name: str) -> None:
        """
        Carga datos hacia un archivo Parquet.

        Raises:
            ValueError: Si el nombre del archivo está vacío
            Exception: Para otros errores durante la extracción
        """
        if not name:
            raise ValueError("El parámetro 'name' no puede estar vacío.")

        target_path = self.target_path / f"{name}.parquet"

        # Se hace profiling antes y después del guardado
        self._profile_data_before_load(df)
        try:
            df.to_parquet(
                target_path,
                index=self.save_index,
                compression=self.compression,
                engine=self.engine,
            )
        except Exception as exc:
            self.logger.error("Error al guardar Parquet en %s: %s", target_path, exc)
            raise
        self._profile_data_after_load(target_path)

        # Una vez el profiling completo se ha hecho, se puede loguear el resumen
        summary = self.get_summary()
        self.logger.info(f"Datos guardados en {target_path}:\n{summary}")

    def _validate_target_exists(self, target_location: Any) -> None:
        """
        Verifica que el destino de datos especificada existe.

        Args:
            target_location (Path): Ubicación o identificador del destino

        Raises:
            TargetNotFoundError: Si el destino no existe
        """
        if not target_location.exists():
            error_msg = f"Directorio de destino no existe: {target_location}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        success_msg = f"Directorio de destino encontrado: {target_location}"
        self.logger.info(success_msg)
        print(success_msg)

    def _profile_data_after_load(self, after_load_info: Path) -> None:
        """
        Genera un perfil completo de los datos después de la carga y actualiza la metadata.

        Mide el tamaño del archivo parquet resultante en megabytes y actualiza el timestamp de carga.

        Args:
            after_load_info (Path): Ruta al archivo Parquet guardado
        """
        file_size_mb = round(after_load_info.stat().st_size / 1024**2, 2)
        self.metadata.update(
            {
                "target": str(after_load_info),
                "file_size_mb": file_size_mb,
            }
        )
        self._update_load_timestamp()
