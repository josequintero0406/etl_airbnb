## Integrantes del grupo y sus responsabilidades

José Manuel Quintero Rodriguez (Ecnargado de manejar la base de datos y logs de ETL)

Roberto Echeverry Arroyabe (Analista de datos)

Jalvi Humberto Villegas Taborda (Encargado de transformar y cargar los datos)

# ETL Airbnb — Ciudad Autónoma de Buenos Aires, Argentina

**Taller 2 — Inteligencia de Negocios | ITM**

Implementación de un proceso ETL completo sobre los datasets de Airbnb Buenos Aires, usando MongoDB como fuente, con transformaciones en Python y carga final en SQLite y XLSX.

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

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4. Preparar MongoDB

Descargar los datasets de [Inside Airbnb — Buenos Aires](http://insideairbnb.com/get-the-data/):
- `listings.csv.gz`
- `calendar.csv.gz`
- `reviews.csv.gz`

Importar a MongoDB local (base de datos `airbnb_argentina`):
```bash
# Descomprimir
gunzip listings.csv.gz calendar.csv.gz reviews.csv.gz

# Importar a MongoDB
mongoimport --db airbnb_argentina --collection listings --type csv --headerline --file listings.csv
mongoimport --db airbnb_argentina --collection calendar --type csv --headerline --file calendar.csv
mongoimport --db airbnb_argentina --collection reviews  --type csv --headerline --file reviews.csv
```

---

## Ejecución

### Pipeline ETL completo
```bash
python main.py
```

### Con límite de registros (modo prueba)
```bash
python main.py --limite 5000
```

### Con MongoDB en host/puerto diferente
```bash
python main.py --uri mongodb://localhost:27017 --db airbnb_argentina
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
| `data/airbnb_argentina.sqlite` | Base de datos SQLite con las 3 tablas transformadas |
| `data/listings.xlsx` | Listings limpios en Excel |
| `data/calendar.xlsx` | Calendar limpio en Excel |
| `data/reviews.xlsx` | Reviews limpios en Excel |
| `data/airbnb_argentina_etl.xlsx` | Archivo combinado con las 3 hojas |
| `logs/log_YYYYMMDD_HHMM.txt` | Log de cada ejecución |

---

## Integrantes del grupo

| Nombre | Responsabilidad |
|--------|----------------|
| [Nombre 1] | Extracción y MongoDB |
| [Nombre 2] | Transformación |
| [Nombre 3] | EDA y visualizaciones |
| [Nombre 4] | Carga e informe |

---

## Ejemplo de log generado

```
2025-04-01 10:32:15 | INFO     | extraccion | Logger 'extraccion' iniciado — archivo: logs/log_20250401_1032.txt
2025-04-01 10:32:15 | INFO     | extraccion | Extraccion inicializada — URI: mongodb://localhost:27017 | DB: airbnb_argetina
2025-04-01 10:32:15 | INFO     | extraccion | Intentando conectar a MongoDB...
2025-04-01 10:32:16 | INFO     | extraccion | Conexión exitosa a MongoDB — Base de datos: 'airbnb_argentina'
2025-04-01 10:32:16 | INFO     | extraccion | Colecciones disponibles: ['listings', 'reviews', 'calendar']
2025-04-01 10:32:16 | INFO     | extraccion | === Iniciando extracción completa de colecciones ===
2025-04-01 10:32:18 | INFO     | extraccion |   ✓ 'listings': 22,345 registros | 75 columnas
2025-04-01 10:32:45 | INFO     | extraccion |   ✓ 'calendar': 8,155,425 registros | 7 columnas
2025-04-01 10:33:02 | INFO     | extraccion |   ✓ 'reviews': 412,890 registros | 6 columnas
2025-04-01 10:33:02 | INFO     | transformacion | === Transformando Listings ===
2025-04-01 10:33:03 | INFO     | transformacion |   Precio normalizado en columna 'price' → float
2025-04-01 10:33:03 | INFO     | transformacion |   [listings] Registros: 22,345 → 22,298 (eliminados: 47 | 0.2%)
2025-04-01 10:33:15 | INFO     | carga | ✓ Tabla 'listings': 22,298 registros cargados
2025-04-01 10:33:16 | INFO     | carga | Carga SQLite completada: data/airbnb_argentina.sqlite
```
