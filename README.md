# E-commerce Reporting ETL

![CI
Status](https://github.com/Gerardo1909/ecommerce-reporting-etl/actions/workflows/ci.yml/badge.svg)

## Descripción del Proyecto

Pipeline automatizado de ETL (Extract, Transform, Load) diseñado para procesar y analizar datos transaccionales de e-commerce, generando métricas de negocio críticas para la toma de decisiones estratégicas.

### Contexto del Negocio

**NovaMart**, una empresa de e-commerce en crecimiento, enfrentaba desafíos significativos en la generación de reportes analíticos. El proceso manual de extracción y preparación de datos consumía 2 horas diarias del equipo de Business, resultando en información con 24 horas de antigüedad y vulnerable a errores humanos. Este sistema automatizado elimina la dependencia de procesos manuales, reduciendo el tiempo de procesamiento y mejorando la precisión de los datos.

### Problema Resuelto

La generación manual de reportes limitaba la capacidad de respuesta ante:
- Agotamiento de stock (stock-outs)
- Efectividad de promociones
- Picos de demanda inesperados
- Identificación de tendencias de venta

Este pipeline ETL automatiza el procesamiento de más de 10 tablas transaccionales, transformando datos crudos en insights accionables.

## Características Principales

- **Extracción**: Ingesta automática de datos desde múltiples fuentes CSV (órdenes, inventario, clientes, productos, etc.)
- **Limpieza de Datos**: 
  - Imputación inteligente de valores nulos usando promedios por categoría
  - Deduplicación automática 
- **Transformación**: Cálculo de métricas clave de negocio
- **Carga**: Exportación optimizada en formato Parquet (reducción de tamaño de 8x)

### Métricas Generadas

1. **Clientes Top**: Identificación del 20% de clientes que generan el 65% de las ventas
2. **Productos Más Vendidos**: Análisis de tendencias mensuales de productos
3. **Tasa de Uso de Promociones**: Evaluación de efectividad de campañas promocionales
4. **Análisis de Inventario**: Disponibilidad y rotación de productos

## Estructura del Proyecto (archivos y directorios más importantes)

```
ecommerce-reporting-etl/
├── config/                 # Archivos de configuración
├── data/                   # Ignorado en .gitignore (información sensible)
│   ├── raw/               # Datos fuente sin procesar (CSV)
│   ├── processed/         # Datos transformados intermedios
│   └── output/            # Resultados finales (Parquet y CSV)
├── logs/                  # Ignorado en .gitignore (archivos de log del proceso ETL)
├── reports/               # Ignorado en .gitignore (reportes de pruebas en HTML)
├── src/
│   ├── extract/           # Módulos de extracción de datos
│   ├── transform/         # Módulos de transformación y limpieza
│   ├── load/              # Módulos de carga y exportación
│   ├── utils/             # Utilidades (logger, helpers)
│   └── main.py            # Punto de entrada del pipeline
├── tests/                 # Suite de pruebas unitarias
├── pyproject.toml         # Configuración del proyecto y dependencias
├── requirements.txt       # Dependencias del proyecto
└── pytest.ini             # Configuración de testing

```

> **Nota de Seguridad**: Los directorios `data/`, `logs/` y `reports/` están excluidos del control de versiones mediante `.gitignore` ya que pueden contener información sensible del sistema, datos de clientes y resultados de análisis confidenciales.

## Requisitos Previos

- Python 3.13 o superior
- pip (gestor de paquetes de Python)

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/Gerardo1909/ecommerce-reporting-etl.git
cd ecommerce-reporting-etl
```

2. Crear un entorno virtual (recomendado):
```bash
python -m venv venv
```

3. Activar el entorno virtual:
```bash
# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

4. Instalar las dependencias:
```bash
pip install -r requirements.txt
```

5. Instalar el paquete en modo desarrollo:
```bash
pip install -e .
```

## Uso

### Ejecución del Pipeline ETL

```bash
python src/main.py
```

El pipeline procesará automáticamente los datos desde `data/raw/` y generará los resultados en `data/output/`.

### Estructura de Datos de Entrada

El sistema espera los siguientes archivos CSV en el directorio `data/raw/`:
- `ecommerce_orders.csv`
- `ecommerce_order_items.csv`
- `ecommerce_customers.csv`
- `ecommerce_products.csv`
- `ecommerce_categories.csv`
- `ecommerce_brands.csv`
- `ecommerce_inventory.csv`
- `ecommerce_promotions.csv`
- `ecommerce_reviews.csv`
- `ecommerce_suppliers.csv`
- `ecommerce_warehouses.csv`

### Resultados

El pipeline genera archivos de salida en dos formatos en el directorio `data/output/`:

**Formato Parquet** (optimizado para almacenamiento y procesamiento):
- Reducción de tamaño de 8x comparado con CSV
- Ideal para integración con herramientas de BI y análisis de big data
- Preserva tipos de datos y metadata

**Formato CSV** (compatibilidad universal):
- Formato ampliamente compatible que puede abrirse en Excel
- Facilita la comparación y validación manual de resultados
- Ideal para compartir con stakeholders no técnicos

**Archivos generados**:
- Métricas de clientes top
- Análisis de productos más vendidos
- Reportes de efectividad de promociones
- Indicadores de inventario

## Testing

El proyecto incluye una suite completa de pruebas unitarias con cobertura de código.

### Ejecutar todas las pruebas:
```bash
pytest
```

### Ejecutar pruebas con reporte de cobertura:
```bash
pytest --cov=src --cov-report=html
```

### Ejecutar pruebas específicas:
```bash
pytest tests/test_extract.py
pytest tests/test_transform.py
pytest tests/test_load.py
```

Los reportes de pruebas se generan en el directorio `reports/`.

## Tecnologías Utilizadas

- **Python 3.13**: Lenguaje de programación principal
- **Pandas 2.3.3**: Manipulación y análisis de datos
- **Pytest**: Framework de testing

## Ventajas del Sistema

1. **Eficiencia**: Reducción de 2 horas diarias a minutos de procesamiento automatizado
2. **Precisión**: Eliminación de errores manuales en el procesamiento de datos
3. **Escalabilidad**: Capacidad de procesar volúmenes crecientes de datos
4. **Optimización**: Reducción de 8x en el tamaño de archivos mediante formato Parquet
5. **Trazabilidad**: Sistema de logging completo para auditoría y debugging
6. **Calidad**: Suite de pruebas automatizadas garantizando la integridad del proceso

## Logs y Monitoreo

Los logs del proceso ETL se almacenan en el directorio `logs/` con información detallada sobre:
- Inicio y finalización de cada fase
- Registros procesados
- Errores y advertencias
- Tiempo de ejecución

## Contribución

Este proyecto sigue las mejores prácticas de desarrollo:
- Código modular y reutilizable
- Documentación inline
- Pruebas unitarias exhaustivas
- Configuración centralizada

## Autor

**Gerardo Toboso**
- Email: gerardotoboso1909@gmail.com

## Licencia

Este proyecto está bajo la Licencia MIT.