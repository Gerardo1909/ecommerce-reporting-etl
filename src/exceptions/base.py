"""
Módulo que define las excepciones base del proceso ETL.
"""

import logging


class ETLError(Exception):
    """
    Excepción base para todos los errores relacionados con el proceso ETL.

    Al instanciarse, automáticamente genera un log con el mensaje y el nivel especificado.

    Args:
        message: Mensaje descriptivo del error
        logger: Logger para registrar el error automáticamente
        log_level: Nivel de logging (default: ERROR). Puede ser WARNING, ERROR, CRITICAL
    """

    def __init__(
        self,
        message: str,
        logger: logging.Logger,
        log_level: int = logging.ERROR,
    ):
        self.message = message
        super().__init__(message)
        logger.log(log_level, message)
