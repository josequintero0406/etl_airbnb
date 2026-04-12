# ETL Airbnb — Ciudad de México

### Taller 2 — Inteligencia de Negocios | ITM

Implementación de un proceso ETL completo sobre los datasets de Airbnb Ciudad de México, usando MongoDB como fuente, con transformaciones en Python y carga final en SQLite y XLSX.

---

## Integrantes del grupo

| Nombre | Responsabilidad |
|--------|----------------|
| José Manuel Quintero Rodríguez | Extracción, MongoDB e Informe |
| Roberto Echeverri Arroyabe | EDA y visualizaciones |
| Jalvi Humberto Villegas Taborda | Transformación Y Carga|

---

## Objetivo

Aplicar los conceptos de Extracción, Transformación y Carga (ETL) sobre los datasets de Airbnb CABA, almacenados en MongoDB local, mediante un proceso automatizado en Python con manejo de logs y análisis exploratorio de datos.

---

## Estructura del proyecto

```
etl_airbnb/
├── src/
│   ├── logger_config.py     # Módulo centralizado de logs
│   ├── extraccion.py        # Clase Extraccion (MongoDB → DataFrames)
│   ├── transformacion.py    # Clase Transformacion (limpieza y enriquecimiento)
│   └── carga.py             # Clase Carga (SQLite + XLSX)
├── notebooks/
│   └── exploracion_airbnb.ipynb   # EDA completo
├── logs/                    # Logs generados automáticamente
├── data/                    # SQLite, XLSX y gráficas generadas
├── main.py                  # Script principal del pipeline ETL
├── requirements.txt
└── README.md
```

---

## Instalación

### 1. Clonar el repositorio
```bash
git clone <URL_DEL_REPO>
cd etl_airbnb
```

### 2. Crear entorno virtual
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate
```

### 3. Instalar ipykernel
```bash
python -m ipykernel install --user --name etl_airbnb --display-name "Python 3.13 (ETL_Airbnb)"
```

### 4. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 5. Preparar MongoDB

Descargar los datasets de [Inside Airbnb — Ciudad de México](http://insideairbnb.com/get-the-data/):
- `listings.csv.gz`
- `calendar.csv.gz`
- `reviews.csv.gz`

Importar a MongoDB local (base de datos `airbnb_mexico`):
```bash
# Descomprimir
gunzip listings.csv.gz calendar.csv.gz reviews.csv.gz

# Importar a MongoDB
mongoimport --db airbnb_mexico --collection listings --type csv --headerline --file listings.csv
mongoimport --db airbnb_mexico --collection calendar --type csv --headerline --file calendar.csv
mongoimport --db airbnb_mexico --collection reviews  --type csv --headerline --file reviews.csv
```

---

## Ejecución

### Pipeline ETL completo
```bash
python main.py
```

### Con límite de registros (modo prueba)
```bash
python main.py --limite 50000
```

### Con MongoDB en host/puerto diferente
```bash
python main.py --uri mongodb://localhost:27017 --db airbnb_mexico
```

### EDA en Jupyter Notebook
```bash
jupyter notebook notebooks/exploracion_airbnb.ipynb
```

---

## Módulos individuales

```bash
# Solo extracción
python src/extraccion.py

# Solo transformación
python src/transformacion.py

# Solo carga
python src/carga.py
```

---

## Salidas generadas

| Archivo | Descripción |
|---------|-------------|
| `data/airbnb_mexico.sqlite` | Base de datos SQLite con las 3 tablas transformadas |
| `data/listings.xlsx` | Listings limpios en Excel (27,051 filas × 78 columnas) |
| `data/calendar.xlsx` | Calendar limpio en Excel (50,000 filas × 11 columnas) |
| `data/reviews.xlsx` | Reviews limpios en Excel (50,000 filas × 12 columnas) |
| `data/airbnb_mexico_etl.xlsx` | Archivo combinado con las 3 hojas |
| `logs/log_YYYYMMDD_HHMM.txt` | Log de cada ejecución |

---

## Ejemplo de log generado

```
2026-04-11 19:26:08 | INFO     | extraccion | Logger 'extraccion' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:08 | INFO     | extraccion | Extraccion inicializada — URI: mongodb://localhost:27017 | DB: airbnb_mexico
2026-04-11 19:26:08 | INFO     | extraccion | Intentando conectar a MongoDB...
2026-04-11 19:26:08 | INFO     | extraccion | Conexión exitosa a MongoDB — Base de datos: 'airbnb_mexico'
2026-04-11 19:26:08 | INFO     | extraccion | Colecciones disponibles: ['listings', 'calendar', 'reviews']
2026-04-11 19:26:08 | INFO     | extraccion | === Iniciando extracción completa de colecciones ===
2026-04-11 19:26:08 | INFO     | extraccion | Extrayendo colección: 'listings'...
2026-04-11 19:26:08 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:09 | INFO     | extraccion |   ✓ 'listings': 27,051 registros | 76 columnas
2026-04-11 19:26:09 | INFO     | extraccion | Extrayendo colección: 'reviews'...
2026-04-11 19:26:09 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:10 | INFO     | extraccion |   ✓ 'reviews': 50,000 registros | 6 columnas
2026-04-11 19:26:10 | INFO     | extraccion | Extrayendo colección: 'calendar'...
2026-04-11 19:26:10 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:10 | INFO     | extraccion |   ✓ 'calendar': 50,000 registros | 5 columnas
2026-04-11 19:26:10 | INFO     | extraccion | === Resumen de extracción ===
2026-04-11 19:26:10 | INFO     | extraccion |   listings    :   27,051 registros
2026-04-11 19:26:10 | INFO     | extraccion |   reviews     :   50,000 registros
2026-04-11 19:26:10 | INFO     | extraccion |   calendar    :   50,000 registros
2026-04-11 19:26:10 | INFO     | extraccion |   TOTAL       :  127,051 registros extraídos
2026-04-11 19:26:10 | INFO     | extraccion | Conexión a MongoDB cerrada correctamente.
2026-04-11 19:26:19 | INFO     | extraccion | Logger 'extraccion' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:19 | INFO     | extraccion | Extraccion inicializada — URI: mongodb://localhost:27017 | DB: airbnb_mexico
2026-04-11 19:26:19 | INFO     | extraccion | Intentando conectar a MongoDB...
2026-04-11 19:26:19 | INFO     | extraccion | Conexión exitosa a MongoDB — Base de datos: 'airbnb_mexico'
2026-04-11 19:26:19 | INFO     | extraccion | Colecciones disponibles: ['listings', 'calendar', 'reviews']
2026-04-11 19:26:19 | INFO     | extraccion | === Iniciando extracción completa de colecciones ===
2026-04-11 19:26:19 | INFO     | extraccion | Extrayendo colección: 'listings'...
2026-04-11 19:26:19 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:20 | INFO     | extraccion |   ✓ 'listings': 27,051 registros | 76 columnas
2026-04-11 19:26:20 | INFO     | extraccion | Extrayendo colección: 'reviews'...
2026-04-11 19:26:20 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:20 | INFO     | extraccion |   ✓ 'reviews': 50,000 registros | 6 columnas
2026-04-11 19:26:20 | INFO     | extraccion | Extrayendo colección: 'calendar'...
2026-04-11 19:26:20 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:20 | INFO     | extraccion |   ✓ 'calendar': 50,000 registros | 5 columnas
2026-04-11 19:26:20 | INFO     | extraccion | === Resumen de extracción ===
2026-04-11 19:26:20 | INFO     | extraccion |   listings    :   27,051 registros
2026-04-11 19:26:20 | INFO     | extraccion |   reviews     :   50,000 registros
2026-04-11 19:26:20 | INFO     | extraccion |   calendar    :   50,000 registros
2026-04-11 19:26:20 | INFO     | extraccion |   TOTAL       :  127,051 registros extraídos
2026-04-11 19:26:20 | INFO     | extraccion | Conexión a MongoDB cerrada correctamente.
2026-04-11 19:26:20 | INFO     | transformacion | Logger 'transformacion' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:20 | INFO     | transformacion | Transformacion inicializada.
2026-04-11 19:26:20 | INFO     | transformacion | ========================================
2026-04-11 19:26:20 | INFO     | transformacion |   INICIO DE TRANSFORMACIONES ETL
2026-04-11 19:26:20 | INFO     | transformacion | ========================================
2026-04-11 19:26:20 | INFO     | transformacion | === Transformando Listings ===
2026-04-11 19:26:20 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:20 | INFO     | transformacion |   Columnas eliminadas por > 70% nulos: 0
2026-04-11 19:26:20 | INFO     | transformacion | Iniciando limpieza de la columna 'price'
2026-04-11 19:26:20 | INFO     | transformacion |   Precio normalizado en columna 'price' → float | NaN: 3,484
2026-04-11 19:26:20 | INFO     | transformacion |   Precios limpiados: 23,567 → 23,567 valores numéricos válidos
2026-04-11 19:26:20 | INFO     | transformacion |   Categorías de precio asignadas:
categoria_precio
Lujo         21456
Premium       2087
Moderado        24
Económico        0
2026-04-11 19:26:20 | INFO     | transformacion |  Fechas convertidas en columna 'last_scraped'
2026-04-11 19:26:20 | INFO     | transformacion |   Categorías de precio asignadas:
categoria_precio
Lujo         21456
Premium       2087
Moderado        24
Económico        0
2026-04-11 19:26:22 | INFO     | transformacion |   Amenities expandidas → 'cantidad_amenities' (promedio: 31.7)
2026-04-11 19:26:22 | INFO     | transformacion |   'host_is_superhost' convertido a booleano
2026-04-11 19:26:22 | INFO     | transformacion |   'review_scores_rating': 3,401 nulos imputados con mediana (4.84)
2026-04-11 19:26:22 | INFO     | transformacion |   'reviews_per_month': 3,401 nulos imputados con mediana (1.26)
2026-04-11 19:26:22 | INFO     | transformacion |   'bedrooms': 980 nulos imputados con mediana (1.00)
2026-04-11 19:26:22 | INFO     | transformacion |   'beds': 3,506 nulos imputados con mediana (1.00)
2026-04-11 19:26:22 | INFO     | transformacion |   [listings] Registros: 27,051 → 27,051 (eliminados: 0 | 0.0%)
2026-04-11 19:26:22 | INFO     | transformacion |   Columnas finales: 78
2026-04-11 19:26:22 | INFO     | transformacion | === Transformando Calendar ===
2026-04-11 19:26:22 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:22 | INFO     | transformacion |  Fechas convertidas en columna 'date'
2026-04-11 19:26:22 | INFO     | transformacion |   Variables temporales derivadas: anio, mes, dia, trimestre, nombre_mes, dia_semana
2026-04-11 19:26:22 | INFO     | transformacion |   'available' convertido a booleano
2026-04-11 19:26:22 | INFO     | transformacion |   [calendar] Registros: 50,000 → 50,000 (eliminados: 0 | 0.0%)
2026-04-11 19:26:22 | INFO     | transformacion | === Transformando Reviews ===
2026-04-11 19:26:22 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:22 | INFO     | transformacion |  Fechas convertidas en columna 'date'
2026-04-11 19:26:22 | INFO     | transformacion |   Variables temporales derivadas: anio, mes, dia, trimestre, nombre_mes, dia_semana
2026-04-11 19:26:22 | INFO     | transformacion |   'comments': 0 nulos reemplazados por 'Sin comentario'
2026-04-11 19:26:22 | INFO     | transformacion |   [reviews] Registros: 50,000 → 50,000 (eliminados: 0 | 0.0%)
2026-04-11 19:26:22 | INFO     | transformacion | ========================================
2026-04-11 19:26:22 | INFO     | transformacion |   TRANSFORMACIONES COMPLETADAS
2026-04-11 19:26:22 | INFO     | transformacion | ========================================
2026-04-11 19:26:44 | INFO     | extraccion | Logger 'extraccion' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:44 | INFO     | extraccion | Extraccion inicializada — URI: mongodb://localhost:27017 | DB: airbnb_mexico
2026-04-11 19:26:44 | INFO     | extraccion | Intentando conectar a MongoDB...
2026-04-11 19:26:44 | INFO     | extraccion | Conexión exitosa a MongoDB — Base de datos: 'airbnb_mexico'
2026-04-11 19:26:44 | INFO     | extraccion | Colecciones disponibles: ['listings', 'calendar', 'reviews']
2026-04-11 19:26:44 | INFO     | extraccion | === Iniciando extracción completa de colecciones ===
2026-04-11 19:26:44 | INFO     | extraccion | Extrayendo colección: 'listings'...
2026-04-11 19:26:44 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:46 | INFO     | extraccion |   ✓ 'listings': 27,051 registros | 76 columnas
2026-04-11 19:26:46 | INFO     | extraccion | Extrayendo colección: 'reviews'...
2026-04-11 19:26:46 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:46 | INFO     | extraccion |   ✓ 'reviews': 50,000 registros | 6 columnas
2026-04-11 19:26:46 | INFO     | extraccion | Extrayendo colección: 'calendar'...
2026-04-11 19:26:46 | INFO     | extraccion |   Aplicando límite de 50000 registros
2026-04-11 19:26:46 | INFO     | extraccion |   ✓ 'calendar': 50,000 registros | 5 columnas
2026-04-11 19:26:46 | INFO     | extraccion | === Resumen de extracción ===
2026-04-11 19:26:46 | INFO     | extraccion |   listings    :   27,051 registros
2026-04-11 19:26:46 | INFO     | extraccion |   reviews     :   50,000 registros
2026-04-11 19:26:46 | INFO     | extraccion |   calendar    :   50,000 registros
2026-04-11 19:26:46 | INFO     | extraccion |   TOTAL       :  127,051 registros extraídos
2026-04-11 19:26:46 | INFO     | extraccion | Conexión a MongoDB cerrada correctamente.
2026-04-11 19:26:46 | INFO     | transformacion | Logger 'transformacion' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:46 | INFO     | transformacion | Transformacion inicializada.
2026-04-11 19:26:46 | INFO     | transformacion | ========================================
2026-04-11 19:26:46 | INFO     | transformacion |   INICIO DE TRANSFORMACIONES ETL
2026-04-11 19:26:46 | INFO     | transformacion | ========================================
2026-04-11 19:26:46 | INFO     | transformacion | === Transformando Listings ===
2026-04-11 19:26:46 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:46 | INFO     | transformacion |   Columnas eliminadas por > 70% nulos: 0
2026-04-11 19:26:46 | INFO     | transformacion | Iniciando limpieza de la columna 'price'
2026-04-11 19:26:46 | INFO     | transformacion |   Precio normalizado en columna 'price' → float | NaN: 3,484
2026-04-11 19:26:46 | INFO     | transformacion |   Precios limpiados: 23,567 → 23,567 valores numéricos válidos
2026-04-11 19:26:46 | INFO     | transformacion |   Categorías de precio asignadas:
categoria_precio
Lujo         21456
Premium       2087
Moderado        24
Económico        0
2026-04-11 19:26:46 | INFO     | transformacion |  Fechas convertidas en columna 'last_scraped'
2026-04-11 19:26:46 | INFO     | transformacion |   Categorías de precio asignadas:
categoria_precio
Lujo         21456
Premium       2087
Moderado        24
Económico        0
2026-04-11 19:26:48 | INFO     | transformacion |   Amenities expandidas → 'cantidad_amenities' (promedio: 31.7)
2026-04-11 19:26:48 | INFO     | transformacion |   'host_is_superhost' convertido a booleano
2026-04-11 19:26:48 | INFO     | transformacion |   'review_scores_rating': 3,401 nulos imputados con mediana (4.84)
2026-04-11 19:26:48 | INFO     | transformacion |   'reviews_per_month': 3,401 nulos imputados con mediana (1.26)
2026-04-11 19:26:48 | INFO     | transformacion |   'bedrooms': 980 nulos imputados con mediana (1.00)
2026-04-11 19:26:48 | INFO     | transformacion |   'beds': 3,506 nulos imputados con mediana (1.00)
2026-04-11 19:26:48 | INFO     | transformacion |   [listings] Registros: 27,051 → 27,051 (eliminados: 0 | 0.0%)
2026-04-11 19:26:48 | INFO     | transformacion |   Columnas finales: 78
2026-04-11 19:26:48 | INFO     | transformacion | === Transformando Calendar ===
2026-04-11 19:26:48 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:48 | INFO     | transformacion |  Fechas convertidas en columna 'date'
2026-04-11 19:26:48 | INFO     | transformacion |   Variables temporales derivadas: anio, mes, dia, trimestre, nombre_mes, dia_semana
2026-04-11 19:26:48 | INFO     | transformacion |   'available' convertido a booleano
2026-04-11 19:26:48 | INFO     | transformacion |   [calendar] Registros: 50,000 → 50,000 (eliminados: 0 | 0.0%)
2026-04-11 19:26:48 | INFO     | transformacion | === Transformando Reviews ===
2026-04-11 19:26:48 | INFO     | transformacion |   Duplicados eliminados: 0
2026-04-11 19:26:48 | INFO     | transformacion |  Fechas convertidas en columna 'date'
2026-04-11 19:26:48 | INFO     | transformacion |   Variables temporales derivadas: anio, mes, dia, trimestre, nombre_mes, dia_semana
2026-04-11 19:26:48 | INFO     | transformacion |   'comments': 0 nulos reemplazados por 'Sin comentario'
2026-04-11 19:26:48 | INFO     | transformacion |   [reviews] Registros: 50,000 → 50,000 (eliminados: 0 | 0.0%)
2026-04-11 19:26:48 | INFO     | transformacion | ========================================
2026-04-11 19:26:48 | INFO     | transformacion |   TRANSFORMACIONES COMPLETADAS
2026-04-11 19:26:48 | INFO     | transformacion | ========================================
2026-04-11 19:26:48 | INFO     | carga | Logger 'carga' iniciado — archivo: C:\Users\josem\Documents\ITM\Semestre10\Inteligencia Negocios\Taller2\etl_airbnb\logs\log_20260411_1926.txt
2026-04-11 19:26:49 | INFO     | carga | Carga inicializada — SQLite: ..\data\airbnb_mexico.sqlite | XLSX dir: ..\data
2026-04-11 19:26:49 | INFO     | carga | ========================================
2026-04-11 19:26:49 | INFO     | carga |   INICIO DE CARGA ETL
2026-04-11 19:26:49 | INFO     | carga | ========================================
2026-04-11 19:26:49 | INFO     | carga | === Cargando datos en SQLite: ..\data\airbnb_mexico.sqlite ===
2026-04-11 19:26:56 | INFO     | carga |   ✓ Tabla 'listings': 27,051 registros cargados
2026-04-11 19:26:56 | INFO     | carga |   ✓ Tabla 'calendar': 50,000 registros cargados
2026-04-11 19:26:57 | INFO     | carga |   ✓ Tabla 'reviews': 50,000 registros cargados
2026-04-11 19:26:57 | INFO     | carga | Carga SQLite completada: ..\data\airbnb_mexico.sqlite
2026-04-11 19:26:57 | INFO     | carga | === Exportando datos a XLSX en: ..\data ===
2026-04-11 19:27:34 | INFO     | carga |   ✓ XLSX exportado: ..\data\listings.xlsx (27,051 filas × 78 columnas)
2026-04-11 19:27:44 | INFO     | carga |   ✓ XLSX exportado: ..\data\calendar.xlsx (50,000 filas × 11 columnas)
2026-04-11 19:27:54 | INFO     | carga |   ✓ XLSX exportado: ..\data\reviews.xlsx (50,000 filas × 12 columnas)
2026-04-11 19:27:54 | INFO     | carga | === Exportando XLSX combinado: ..\data\airbnb_mexico_etl.xlsx ===
2026-04-11 19:28:06 | INFO     | carga |   Hoja 'listings': 27,051 registros
2026-04-11 19:28:08 | INFO     | carga |   Hoja 'calendar': 50,000 registros
2026-04-11 19:28:12 | INFO     | carga |   Hoja 'reviews': 50,000 registros
2026-04-11 19:28:47 | INFO     | carga | ✓ XLSX combinado exportado: ..\data\airbnb_mexico_etl.xlsx
2026-04-11 19:28:47 | INFO     | carga | === Verificando carga en SQLite ===
2026-04-11 19:28:47 | INFO     | carga |   ✓ Tabla 'listings': 27,051 registros
2026-04-11 19:28:47 | INFO     | carga |   ✓ Tabla 'calendar': 50,000 registros
2026-04-11 19:28:47 | INFO     | carga |   ✓ Tabla 'reviews': 50,000 registros
2026-04-11 19:28:47 | INFO     | carga | ========================================
2026-04-11 19:28:47 | INFO     | carga |   CARGA COMPLETADA
2026-04-11 19:28:47 | INFO     | carga |   Total tablas cargadas: 3
2026-04-11 19:28:47 | INFO     | carga |   Total registros: 127,051
2026-04-11 19:28:47 | INFO     | carga | ========================================

```
