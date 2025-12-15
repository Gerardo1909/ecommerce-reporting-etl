"""
Módulo de excepciones personalizadas para el proceso ETL.
"""

# Excepción base
from exceptions.base import ETLError

# Excepciones de extracción
from exceptions.extract_exceptions import (
    ExtractError,
    SourceNotFoundError,
    SourceParseError,
    SourceReadError,
    SourceNameNotSpecifiedError,
)

# Excepciones de transformación
from exceptions.transform_exceptions import (
    TransformError,
    SchemaValidationError,
    MissingRequiredColumnsError,
    UnexpectedColumnsError,
    DataTypeMismatchError,
    DataQualityError,
    RangeValidationError,
    NullConstraintError,
    DuplicateKeyError,
    CleaningInvariantError
)

# Excepciones de carga
from exceptions.load_exceptions import (
    LoadError,
    TargetNotFoundError,
    TargetNameNotSpecifiedError,
    LoadWriteError,
)

__all__ = [
    # Base
    "ETLError",
    # Extract
    "ExtractError",
    "SourceNotFoundError",
    "SourceParseError",
    "SourceReadError",
    "SourceNameNotSpecifiedError",
    # Transform
    "TransformError",
    "SchemaValidationError",
    "MissingRequiredColumnsError",
    "UnexpectedColumnsError",
    "DataTypeMismatchError",
    "DataQualityError",
    "RangeValidationError",
    "NullConstraintError",
    "DuplicateKeyError",
    "CleaningInvariantError",
    # Load
    "LoadError",
    "TargetNotFoundError",
    "TargetNameNotSpecifiedError",
    "LoadWriteError",
]
