"""la clase Extraccion se conecta a una base de datos MongoDB local,
consulta las colecciones Listings, Reviews y Calendar, y retorna los datos
como DataFrames de pandas listos.
"""

import sys
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Asegurar que src/ esté en el path para importar logger_config
sys.path.insert(0, str(Path(__file__).resolve().parent))
from logger_config import get_logger

# Clase para la fase de Extracción del proceso ETL
class Extraccion:
    ## Nombres de las colecciones esperadas en MongoDB
    COLECCIONES = ["listings", "reviews", "calendar"]

    # Constructor de la clase Extraccion
    def __init__(self, uri: str = "mongodb://localhost:27017", db_name: str = "airbnb_mexico"):
    
        self.uri = uri  ##conexión a MongoDB. Por defecto localhost:27017
        self.db_name = db_name  ##Nombre de la base de datos.'airbnb_mexico'.
        self.client = None
        self.db = None
        self.logger = get_logger("extraccion")
        self.logger.info(f"Extraccion inicializada — URI: {uri} | DB: {db_name}")

    # Método principal para conectar a MongoDB y verificar la conexión.
    def conectar(self) -> None:
    
        self.logger.info("Intentando conectar a MongoDB...")
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # Ping para verificar conexión real
            self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            self.logger.info(f"Conexión exitosa a MongoDB — Base de datos: '{self.db_name}'")

            # Listar colecciones disponibles
            colecciones_disponibles = self.db.list_collection_names()
            self.logger.info(f"Colecciones disponibles: {colecciones_disponibles}")## Registra en el log el resultado de la conexión.

        except ServerSelectionTimeoutError as e: ##Si el servidor no responde a tiempo.
            self.logger.error(f"Timeout al conectar a MongoDB: {e}")
            raise
        except ConnectionFailure as e:  ##Si no puede conectarse al servidor MongoDB
            self.logger.error(f"Fallo de conexión a MongoDB: {e}")
            raise

    # Método para extraer una colección específica y convertirla a DataFrame.
    def extraer_coleccion(self, nombre_coleccion: str, limite = 50_000) -> pd.DataFrame:
    
        if self.db is None:
            raise RuntimeError("Debe llamar a conectar() antes de extraer datos.")

        self.logger.info(f"Extrayendo colección: '{nombre_coleccion}'...")

        try:
            coleccion = self.db[nombre_coleccion]

            # Aplicar límite si se especifica
            cursor = coleccion.find({}, {"_id": 0})  # Excluir campo _id de MongoDB
            if limite > 0:
                cursor = cursor.limit(limite)
                self.logger.info(f"  Aplicando límite de {limite} registros")

            df = pd.DataFrame(list(cursor))

            if df.empty:
                self.logger.warning(
                    f"La colección '{nombre_coleccion}' está vacía o no existe."
                )
                return df

            self.logger.info(
                f"  ✓ '{nombre_coleccion}': {len(df):,} registros | {len(df.columns)} columnas"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error al extraer '{nombre_coleccion}': {e}")
            raise

    # Método para extraer todas las colecciones y convertirlas a DataFrame.
    def extraer_todo(self, limite = 50_000) -> dict[str, pd.DataFrame]:
    
        if self.db is None:
            raise RuntimeError("Debe llamar a conectar() antes de extraer datos.")

        self.logger.info("=== Iniciando extracción completa de colecciones ===")
        dataframes = {}

        for nombre in self.COLECCIONES:
            try:
                df = self.extraer_coleccion(nombre, limite = 50_000)
                dataframes[nombre] = df
            except Exception as e:
                self.logger.error(f"No se pudo extraer '{nombre}': {e}")
                dataframes[nombre] = pd.DataFrame()  # DataFrame vacío como fallback

        # Resumen final de extracción
        self.logger.info("=== Resumen de extracción ===")
        total_registros = 0
        for nombre, df in dataframes.items():
            registros = len(df)
            total_registros += registros
            self.logger.info(f"  {nombre:12s}: {registros:>8,} registros")
        self.logger.info(f"  {'TOTAL':12s}: {total_registros:>8,} registros extraídos")

        return dataframes

    # Método para cerrar la conexión a MongoDB.
    def cerrar_conexion(self) -> None:

        if self.client:
            self.client.close()
            self.logger.info("Conexión a MongoDB cerrada correctamente.")

# Ejecución directa para prueba rápida
if __name__ == "__main__":
    ext = Extraccion(uri="mongodb://localhost:27017", db_name="airbnb_mexico")
    try:
        ext.conectar()
        dfs = ext.extraer_todo()

        print("\n--- Vista previa de Listings ---")
        print(dfs["listings"].head(5))

        print("\n--- Vista previa de Calendar ---")
        print(dfs["calendar"].head(5))

        print("\n--- Vista previa de Reviews ---")
        print(dfs["reviews"].head(5))

    finally:
        ext.cerrar_conexion()
