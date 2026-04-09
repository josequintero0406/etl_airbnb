"""
Módulo de transformación de datos del proceso ETL Airbnb Buenos Aires.

Contiene la clase Transformacion que recibe los DataFrames extraídos de MongoDB
y aplica limpieza, normalización, derivación de variables y categorización,
generando DataFrames limpios y listos para la carga en SQLite/XLSX.

Uso básico:
    from extraccion import Extraccion
    from transformacion import Transformacion

    ext = Extraccion()
    ext.conectar()
    dfs_raw = ext.extraer_todo()

    trans = Transformacion(dfs_raw)
    dfs_limpios = trans.transformar_todo()
"""

import ast
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from logger_config import get_logger


class Transformacion:
    """
    Clase encargada de la fase de Transformación del proceso ETL.

    Aplica sobre cada colección:
      - Limpieza de valores nulos y duplicados
      - Normalización de precios (eliminar $, convertir a float)
      - Conversión de fechas al formato estándar YYYY-MM-DD
      - Derivación de variables temporales (año, mes, día, trimestre)
      - Categorización de precios por rangos
      - Expansión de campos anidados (amenities)
      - Registro detallado de cada paso en logs

    Attributes:
        dataframes (dict): DataFrames crudos indexados por nombre de colección.
        logger: Logger configurado para este módulo.
    """

    # Rangos de categorización de precio (en USD/ARS según el dataset)
    RANGOS_PRECIO = [0, 50, 150, 400, float("inf")]
    ETIQUETAS_PRECIO = ["Económico", "Moderado", "Premium", "Lujo"]

    def __init__(self, dataframes: dict[str, pd.DataFrame]):
        """
        Inicializa la clase con los DataFrames crudos.

        Args:
            dataframes (dict): Diccionario con claves 'listings', 'reviews',
                               'calendar' y valores pd.DataFrame.
        """
        self.dataframes = dataframes
        self.logger = get_logger("transformacion")
        self.logger.info("Transformacion inicializada.")

    # Utilidades internas
    def _log_cambio_registros(self, nombre: str, antes: int, despues: int) -> None:
        """Registra en log la diferencia de registros antes/después de una limpieza."""
        eliminados = antes - despues
        porcentaje = (eliminados / antes * 100) if antes > 0 else 0
        self.logger.info(
            f"  [{nombre}] Registros: {antes:,} → {despues:,} "
            f"(eliminados: {eliminados:,} | {porcentaje:.1f}%)"
        )

    def _limpiar_precio(self, df: pd.DataFrame, columna: str = "price") -> pd.DataFrame:
        """
        Normaliza una columna de precio: elimina '$', ',' y convierte a float.

        Los precios en Airbnb vienen como strings tipo '$1,200.00'.
        Se eliminan los caracteres no numéricos y se convierte a float.

        Args:
            df (pd.DataFrame): DataFrame con la columna de precio.
            columna (str): Nombre de la columna de precio.

        Returns:
            pd.DataFrame: DataFrame con la columna transformada a float.
        """
        if columna not in df.columns:
            self.logger.warning(f"Columna '{columna}' no encontrada para normalización.")
            return df

        df = df.copy()
        # Eliminar símbolo $, comas y espacios; convertir a float
        df[columna] = (
            df[columna]
            .astype(str)
            .str.replace(r"[\$,\s]", "", regex=True)
            .replace("nan", None)
            .replace("", None)
        )
        df[columna] = pd.to_numeric(df[columna], errors="coerce")
        self.logger.info(f"  Precio normalizado en columna '{columna}' → float")
        return df

    def _convertir_fecha(self, df: pd.DataFrame, columna: str = "date") -> pd.DataFrame:
        """
        Convierte una columna de fecha al formato estándar datetime y YYYY-MM-DD.

        Args:
            df (pd.DataFrame): DataFrame con columna de fecha.
            columna (str): Nombre de la columna de fecha.

        Returns:
            pd.DataFrame: DataFrame con la columna convertida a datetime.
        """
        if columna not in df.columns:
            self.logger.warning(f"Columna de fecha '{columna}' no encontrada.")
            return df

        df = df.copy()
        df[columna] = pd.to_datetime(df[columna], errors="coerce", format="mixed")
        invalidos = df[columna].isna().sum()
        if invalidos > 0:
            self.logger.warning(f"  {invalidos:,} fechas inválidas en '{columna}' → NaT")
        self.logger.info(f"  Fechas convertidas en columna '{columna}'")
        return df

    def _derivar_variables_temporales(self, df: pd.DataFrame, columna: str = "date") -> pd.DataFrame:
        """
        Deriva variables temporales a partir de una columna de fecha.

        Genera: año, mes, dia, trimestre, nombre_mes, dia_semana.

        Args:
            df (pd.DataFrame): DataFrame con columna de fecha ya convertida.
            columna (str): Nombre de la columna datetime base.

        Returns:
            pd.DataFrame: DataFrame con nuevas columnas temporales.
        """
        if columna not in df.columns:
            return df

        df = df.copy()
        df["anio"] = df[columna].dt.year
        df["mes"] = df[columna].dt.month
        df["dia"] = df[columna].dt.day
        df["trimestre"] = df[columna].dt.quarter
        df["nombre_mes"] = df[columna].dt.strftime("%B")
        df["dia_semana"] = df[columna].dt.day_name()
        self.logger.info(f"  Variables temporales derivadas desde '{columna}': anio, mes, dia, trimestre, nombre_mes, dia_semana")
        return df

    def _categorizar_precio(self, df: pd.DataFrame, columna: str = "price") -> pd.DataFrame:
        """
        Agrega una columna 'categoria_precio' basada en rangos predefinidos.

        Rangos (en la moneda del dataset):
          - Económico:  $0 – $50
          - Moderado:   $51 – $150
          - Premium:    $151 – $400
          - Lujo:       > $400

        Args:
            df (pd.DataFrame): DataFrame con columna de precio numérico.
            columna (str): Nombre de la columna de precio.

        Returns:
            pd.DataFrame: DataFrame con columna 'categoria_precio' añadida.
        """
        if columna not in df.columns:
            return df

        df = df.copy()
        df["categoria_precio"] = pd.cut(
            df[columna],
            bins=self.RANGOS_PRECIO,
            labels=self.ETIQUETAS_PRECIO,
            right=True,
        )
        conteo = df["categoria_precio"].value_counts()
        self.logger.info(f"  Categorías de precio asignadas:\n{conteo.to_string()}")
        return df

    def _expandir_amenities(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expande el campo 'amenities' de string/lista a columna con conteo.

        El campo amenities en Airbnb viene como string tipo:
        '["WiFi", "Kitchen", "Air conditioning"]'
        Se parsea y se agrega una columna 'cantidad_amenities' (int).

        Args:
            df (pd.DataFrame): DataFrame de listings con columna 'amenities'.

        Returns:
            pd.DataFrame: DataFrame con columna 'cantidad_amenities' añadida.
        """
        if "amenities" not in df.columns:
            self.logger.warning("Columna 'amenities' no encontrada.")
            return df

        df = df.copy()

        def contar_amenities(valor):
            if pd.isna(valor) or valor == "":
                return 0
            try:
                lista = ast.literal_eval(str(valor))
                return len(lista) if isinstance(lista, list) else 0
            except (ValueError, SyntaxError):
                # Intentar contar por comas si el parseo falla
                return len(str(valor).split(","))

        df["cantidad_amenities"] = df["amenities"].apply(contar_amenities)
        promedio = df["cantidad_amenities"].mean()
        self.logger.info(
            f"  Amenities expandidas → 'cantidad_amenities' (promedio: {promedio:.1f})"
        )
        return df

    # Transformaciones por colección   
    def transformar_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas las transformaciones al DataFrame de Listings.

        Pasos:
          1. Elimina duplicados por 'id'
          2. Elimina columnas con >70% de valores nulos
          3. Normaliza columna 'price'
          4. Convierte 'last_scraped' a datetime
          5. Categoriza precio en rangos
          6. Expande 'amenities' → 'cantidad_amenities'
          7. Convierte 'host_is_superhost' a booleano
          8. Rellena nulos en columnas clave

        Args:
            df (pd.DataFrame): DataFrame crudo de la colección Listings.

        Returns:
            pd.DataFrame: DataFrame limpio y enriquecido.
        """
        self.logger.info("=== Transformando Listings ===")
        antes = len(df)
        df = df.copy()

        # 1. Eliminar duplicados
        df = df.drop_duplicates(subset=["id"]) if "id" in df.columns else df.drop_duplicates()
        self.logger.info(f"  Duplicados eliminados: {antes - len(df):,}")

        # 2. Eliminar columnas con más del 70% de nulos
        umbral_nulos = 0.70
        cols_antes = len(df.columns)
        df = df.loc[:, df.isnull().mean() < umbral_nulos]
        self.logger.info(
            f"  Columnas eliminadas por >70% nulos: {cols_antes - len(df.columns)}"
        )

        # 3. Normalizar precio
        df = self._limpiar_precio(df, "price")

        # 4. Convertir fecha de scraping
        if "last_scraped" in df.columns:
            df = self._convertir_fecha(df, "last_scraped")

        # 5. Categorizar precio
        df = self._categorizar_precio(df, "price")

        # 6. Expandir amenities
        df = self._expandir_amenities(df)

        # 7. Convertir host_is_superhost a booleano
        if "host_is_superhost" in df.columns:
            df["host_is_superhost"] = df["host_is_superhost"].map(
                {"t": True, "f": False, True: True, False: False}
            )
            self.logger.info("  'host_is_superhost' convertido a booleano")

        # 8. Rellenar nulos en columnas numéricas clave con mediana
        cols_numericas = ["review_scores_rating", "reviews_per_month", "bedrooms", "beds"]
        for col in cols_numericas:
            if col in df.columns:
                mediana = df[col].median()
                nulos = df[col].isna().sum()
                df[col] = df[col].fillna(mediana)
                if nulos > 0:
                    self.logger.info(f"  '{col}': {nulos:,} nulos imputados con mediana ({mediana:.2f})")

        # 9. Rellenar nulos en columnas de texto
        cols_texto = ["neighbourhood_cleansed", "room_type", "property_type"]
        for col in cols_texto:
            if col in df.columns:
                df[col] = df[col].fillna("Desconocido")

        despues = len(df)
        self._log_cambio_registros("listings", antes, despues)
        self.logger.info(f"  Columnas finales: {len(df.columns)}")
        return df

    def transformar_calendar(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas las transformaciones al DataFrame de Calendar.

        Pasos:
          1. Elimina duplicados por (listing_id, date)
          2. Convierte 'date' a datetime y deriva variables temporales
          3. Normaliza columna 'price'
          4. Convierte 'available' a booleano
          5. Elimina filas con fecha nula

        Args:
            df (pd.DataFrame): DataFrame crudo de la colección Calendar.

        Returns:
            pd.DataFrame: DataFrame limpio con variables temporales.
        """
        self.logger.info("=== Transformando Calendar ===")
        antes = len(df)
        df = df.copy()

        # 1. Eliminar duplicados
        cols_dedup = ["listing_id", "date"] if all(c in df.columns for c in ["listing_id", "date"]) else None
        df = df.drop_duplicates(subset=cols_dedup)
        self.logger.info(f"  Duplicados eliminados: {antes - len(df):,}")

        # 2. Convertir fecha y derivar variables temporales
        df = self._convertir_fecha(df, "date")
        df = _derivar_variables_temporales_local(df, "date", self.logger)

        # 3. Normalizar precio
        df = self._limpiar_precio(df, "price")
        if "adjusted_price" in df.columns:
            df = self._limpiar_precio(df, "adjusted_price")

        # 4. Convertir 'available' a booleano
        if "available" in df.columns:
            df["available"] = df["available"].map({"t": True, "f": False, True: True, False: False})
            self.logger.info("  'available' convertido a booleano")

        # 5. Eliminar filas con fecha nula (no tienen valor analítico)
        if "date" in df.columns:
            nulos_fecha = df["date"].isna().sum()
            if nulos_fecha > 0:
                df = df.dropna(subset=["date"])
                self.logger.warning(f"  {nulos_fecha:,} filas eliminadas por fecha nula")

        despues = len(df)
        self._log_cambio_registros("calendar", antes, despues)
        return df

    def transformar_reviews(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas las transformaciones al DataFrame de Reviews.

        Pasos:
          1. Elimina duplicados por 'id'
          2. Convierte 'date' a datetime y deriva variables temporales
          3. Limpia columna 'comments' (nulos → 'Sin comentario')
          4. Elimina filas con listing_id nulo

        Args:
            df (pd.DataFrame): DataFrame crudo de la colección Reviews.

        Returns:
            pd.DataFrame: DataFrame limpio con fechas procesadas.
        """
        self.logger.info("=== Transformando Reviews ===")
        antes = len(df)
        df = df.copy()

        # 1. Eliminar duplicados
        df = df.drop_duplicates(subset=["id"]) if "id" in df.columns else df.drop_duplicates()
        self.logger.info(f"  Duplicados eliminados: {antes - len(df):,}")

        # 2. Convertir fecha
        df = self._convertir_fecha(df, "date")
        df = _derivar_variables_temporales_local(df, "date", self.logger)

        # 3. Limpiar columna comments
        if "comments" in df.columns:
            nulos = df["comments"].isna().sum()
            df["comments"] = df["comments"].fillna("Sin comentario")
            self.logger.info(f"  'comments': {nulos:,} nulos reemplazados por 'Sin comentario'")

        # 4. Eliminar filas sin listing_id
        if "listing_id" in df.columns:
            sin_id = df["listing_id"].isna().sum()
            if sin_id > 0:
                df = df.dropna(subset=["listing_id"])
                self.logger.warning(f"  {sin_id:,} reviews eliminadas por listing_id nulo")

        despues = len(df)
        self._log_cambio_registros("reviews", antes, despues)
        return df

    # Método principal
    def transformar_todo(self) -> dict[str, pd.DataFrame]:
        """
        Ejecuta todas las transformaciones sobre los tres DataFrames.

        Returns:
            dict[str, pd.DataFrame]: Diccionario con DataFrames limpios:
                - 'listings': listings transformado
                - 'calendar': calendar transformado
                - 'reviews':  reviews transformado
        """
        self.logger.info("========================================")
        self.logger.info("  INICIO DE TRANSFORMACIONES ETL")
        self.logger.info("========================================")

        resultados = {}

        for nombre in ["listings", "calendar", "reviews"]:
            df_crudo = self.dataframes.get(nombre, pd.DataFrame())

            if df_crudo.empty:
                self.logger.warning(f"DataFrame '{nombre}' vacío — se omite transformación.")
                resultados[nombre] = df_crudo
                continue

            try:
                metodo = getattr(self, f"transformar_{nombre}")
                resultados[nombre] = metodo(df_crudo)
            except Exception as e:
                self.logger.error(f"Error al transformar '{nombre}': {e}")
                resultados[nombre] = df_crudo  # Retornar crudo como fallback

        self.logger.info("========================================")
        self.logger.info("  TRANSFORMACIONES COMPLETADAS")
        self.logger.info("========================================")
        return resultados


# Función auxiliar (fuera de clase para reutilización)
def _derivar_variables_temporales_local(df, columna, logger):
    """Derivar variables temporales — función auxiliar compartida."""
    if columna not in df.columns:
        return df
    df = df.copy()
    df["anio"] = df[columna].dt.year
    df["mes"] = df[columna].dt.month
    df["dia"] = df[columna].dt.day
    df["trimestre"] = df[columna].dt.quarter
    df["nombre_mes"] = df[columna].dt.strftime("%B")
    df["dia_semana"] = df[columna].dt.day_name()
    logger.info(f"  Variables temporales derivadas: anio, mes, dia, trimestre, nombre_mes, dia_semana")
    return df

# Ejecución directa para prueba
if __name__ == "__main__":
    from extraccion import Extraccion

    ext = Extraccion()
    ext.conectar()
    dfs_raw = ext.extraer_todo()
    ext.cerrar_conexion()

    trans = Transformacion(dfs_raw)
    dfs_limpios = trans.transformar_todo()

    for nombre, df in dfs_limpios.items():
        print(f"\n{nombre.upper()} — shape: {df.shape}")
        print(df.dtypes)
