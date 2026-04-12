"""
Módulo de carga de datos del proceso ETL Airbnb Ciudad de México.

Contiene la clase Carga que recibe los DataFrames transformados y los persiste en:
  - Una base de datos SQLite local (para análisis y consultas SQL)
  - Archivos XLSX (para reportes y entrega)

También verifica la integridad de la carga y registra todos los eventos en logs.

Uso básico:
    from carga import Carga

    carga = Carga(
        db_path="data/airbnb_mexico.sqlite",
        xlsx_dir="data/"
    )
    carga.cargar_todo(dataframes_limpios)
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from logger_config import get_logger


class Carga:
    """
    Clase encargada de la fase de Carga del proceso ETL.

    Persiste los DataFrames transformados en SQLite y XLSX,
    verifica la integridad de la carga y registra todos los eventos.

    Attributes:
        db_path (Path): Ruta al archivo SQLite de destino.
        xlsx_dir (Path): Directorio donde se exportarán los archivos XLSX.
        logger: Logger configurado para este módulo.
    """

    def __init__(
        self,
        db_path: str = "data/airbnb_mexico.sqlite",
        xlsx_dir: str = "data/",
    ):
        """
        Inicializa la clase con las rutas de destino.

        Args:
            db_path (str): Ruta al archivo SQLite. Se crea si no existe.
            xlsx_dir (str): Directorio para exportar archivos XLSX.
        """
        self.db_path = Path(db_path)
        self.xlsx_dir = Path(xlsx_dir)
        self.logger = get_logger("carga")

        # Crear directorios si no existen
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.xlsx_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Carga inicializada — SQLite: {self.db_path} | XLSX dir: {self.xlsx_dir}")

    # Carga a SQLite
    def cargar_sqlite(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """
        Inserta los DataFrames transformados en una base de datos SQLite.

        Cada DataFrame se carga como una tabla con el mismo nombre.
        Si la tabla ya existe, se reemplaza (útil para re-ejecuciones).

        Args:
            dataframes (dict): Diccionario {nombre_tabla: DataFrame}.

        Raises:
            sqlite3.Error: Si hay un error al escribir en SQLite.
        """
        self.logger.info(f"=== Cargando datos en SQLite: {self.db_path} ===")

        try:
            with sqlite3.connect(self.db_path) as conn:
                for nombre_tabla, df in dataframes.items():
                    if df.empty:
                        self.logger.warning(f"  DataFrame '{nombre_tabla}' vacío — se omite.")
                        continue

                    df_para_cargar = df.copy()

                    # Convertir columnas categóricas a string
                    for col in df_para_cargar.select_dtypes(include="category").columns:
                        df_para_cargar[col] = df_para_cargar[col].astype(str)

                    # Convertir datetime a string ISO
                    for col in df_para_cargar.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
                        df_para_cargar[col] = df_para_cargar[col].dt.strftime("%Y-%m-%d")

                    # Convertir listas y diccionarios a string
                    for col in df_para_cargar.columns:
                        if df_para_cargar[col].dtype == object:
                            df_para_cargar[col] = df_para_cargar[col].apply(
                                lambda x: str(x) if isinstance(x, (list, dict)) else x
                            )

                    df_para_cargar.to_sql(
                        nombre_tabla,
                        conn,
                        if_exists="replace",
                        index=False,
                    )
                    self.logger.info(
                        f"  ✓ Tabla '{nombre_tabla}': {len(df_para_cargar):,} registros cargados"
                    )

            self.logger.info(f"Carga SQLite completada: {self.db_path}")

        except sqlite3.Error as e:
            self.logger.error(f"Error al cargar en SQLite: {e}")
            raise

    # Exportación a XLSX
    def exportar_xlsx(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """
        Exporta cada DataFrame a un archivo XLSX independiente.

        Los archivos se guardan en xlsx_dir con el nombre de la colección.
        Ejemplo: data/listings.xlsx, data/calendar.xlsx, data/reviews.xlsx

        Args:
            dataframes (dict): Diccionario {nombre: DataFrame}.
        """
        self.logger.info(f"=== Exportando datos a XLSX en: {self.xlsx_dir} ===")

        for nombre, df in dataframes.items():
            if df.empty:
                self.logger.warning(f"  DataFrame '{nombre}' vacío — se omite exportación XLSX.")
                continue

            ruta_xlsx = self.xlsx_dir / f"{nombre}.xlsx"

            try:
                # Preparar copia para XLSX: convertir tipos incompatibles
                df_export = df.copy()
                for col in df_export.select_dtypes(include="category").columns:
                    df_export[col] = df_export[col].astype(str)

                # Exportar con formato básico
                with pd.ExcelWriter(ruta_xlsx, engine="openpyxl") as writer:
                    df_export.to_excel(writer, sheet_name=nombre[:31], index=False)  # Máx 31 chars en nombre de hoja

                self.logger.info(
                    f"  ✓ XLSX exportado: {ruta_xlsx} ({len(df_export):,} filas × {len(df_export.columns)} columnas)"
                )

            except Exception as e:
                self.logger.error(f"  Error exportando '{nombre}' a XLSX: {e}")

    # Exportación a XLSX combinado
    def exportar_xlsx_combinado(self, dataframes: dict[str, pd.DataFrame], nombre_archivo: str = "airbnb_mexico_etl.xlsx") -> None:
        """
        Exporta todos los DataFrames en un único archivo XLSX con múltiples hojas.

        Útil para entregar un solo archivo con todos los datos transformados.

        Args:
            dataframes (dict): Diccionario {nombre_hoja: DataFrame}.
            nombre_archivo (str): Nombre del archivo XLSX de salida.
        """
        ruta_xlsx = self.xlsx_dir / nombre_archivo
        self.logger.info(f"=== Exportando XLSX combinado: {ruta_xlsx} ===")

        try:
            with pd.ExcelWriter(ruta_xlsx, engine="openpyxl") as writer:
                for nombre, df in dataframes.items():
                    if df.empty:
                        continue
                    df_export = df.copy()
                    for col in df_export.select_dtypes(include="category").columns:
                        df_export[col] = df_export[col].astype(str)
                    df_export.to_excel(writer, sheet_name=nombre[:31], index=False)
                    self.logger.info(f"  Hoja '{nombre}': {len(df_export):,} registros")

            self.logger.info(f"✓ XLSX combinado exportado: {ruta_xlsx}")

        except Exception as e:
            self.logger.error(f"Error al exportar XLSX combinado: {e}")
            raise

    # Verificación de carga
    def verificar_carga(self, tablas_esperadas: list[str] = None) -> dict[str, int]:
        """
        Verifica que las tablas existan en SQLite y cuenta sus registros.

        Realiza un SELECT COUNT(*) por cada tabla y lo registra en el log.

        Args:
            tablas_esperadas (list): Nombres de tablas a verificar.
                                     Si es None, verifica todas las tablas en la DB.

        Returns:
            dict[str, int]: Diccionario {nombre_tabla: cantidad_registros}.
        """
        if not self.db_path.exists():
            self.logger.error(f"Base de datos no encontrada: {self.db_path}")
            return {}

        self.logger.info("=== Verificando carga en SQLite ===")
        conteos = {}

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Si no se especifican tablas, obtener todas
                if tablas_esperadas is None:
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tablas_esperadas = [row[0] for row in cursor.fetchall()]

                for tabla in tablas_esperadas:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tabla};")
                        conteo = cursor.fetchone()[0]
                        conteos[tabla] = conteo
                        estado = "✓" if conteo > 0 else "⚠ VACÍA"
                        self.logger.info(f"  {estado} Tabla '{tabla}': {conteo:,} registros")
                    except sqlite3.OperationalError:
                        self.logger.warning(f"  ✗ Tabla '{tabla}' no existe en la base de datos.")
                        conteos[tabla] = -1

        except sqlite3.Error as e:
            self.logger.error(f"Error al verificar carga: {e}")

        return conteos

    # Método principal para cargar todos los DataFrames
    def cargar_todo(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """
        Ejecuta el proceso completo de carga: SQLite + XLSX + verificación.

        Args:
            dataframes (dict): Diccionario con DataFrames transformados.
        """
        self.logger.info("========================================")
        self.logger.info("  INICIO DE CARGA ETL")
        self.logger.info("========================================")

        # 1. Cargar en SQLite
        self.cargar_sqlite(dataframes)

        # 2. Exportar XLSX individuales
        self.exportar_xlsx(dataframes)

        # 3. Exportar XLSX combinado
        self.exportar_xlsx_combinado(dataframes)

        # 4. Verificar carga
        conteos = self.verificar_carga(list(dataframes.keys()))

        self.logger.info("========================================")
        self.logger.info("  CARGA COMPLETADA")
        self.logger.info(f"  Total tablas cargadas: {len(conteos)}")
        self.logger.info(f"  Total registros: {sum(v for v in conteos.values() if v > 0):,}")
        self.logger.info("========================================")

# Ejecución directa para prueba
if __name__ == "__main__":
    from extraccion import Extraccion
    from transformacion import Transformacion

    # Extracción
    ext = Extraccion()
    ext.conectar()
    dfs_raw = ext.extraer_todo()
    ext.cerrar_conexion()

    # Transformación
    trans = Transformacion(dfs_raw)
    dfs_limpios = trans.transformar_todo()

    # Carga
    carga = Carga(
        db_path="../data/airbnb_mexico.sqlite",
        xlsx_dir="../data/"
    )
    carga.cargar_todo(dfs_limpios)
