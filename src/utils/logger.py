"""
Módulo para logging de la aplicación.
"""

import logging
import os
from logging.handlers import RotatingFileHandler


LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name: str, filename: str) -> logging.Logger:
    """
    Obtiene un logger configurado con handlers de consola y archivo.
    """
    logger = logging.getLogger(name)

    # Evitar duplicar handlers si el logger ya existe
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Formato detallado para mejor observabilidad
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler para archivo con rotación
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger

# Loggers separados para cada parte del proceso ETL
extract_logger = get_logger("extraction", "extract.log")
transform_logger = get_logger("transformation", "transform.log")
load_logger = get_logger("loading", "load.log")
