"""Utilidades de extraccion para archivos, APIs y MongoDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError


class Extraccion:
    """Centraliza la lectura de fuentes externas usadas por el proyecto."""

    def __init__(self, timeout: int = 10) -> None:
        """Configura el timeout por defecto para llamadas HTTP."""
        self.timeout = timeout

    def get_csv(self, ruta: str | Path, separador: str = ",") -> Optional[pd.DataFrame]:
        """Lee un archivo CSV y devuelve un DataFrame con datos validos."""
        try:
            df = pd.read_csv(Path(ruta), sep=separador)
        except FileNotFoundError:
            print(f"No se encontro el archivo CSV: {ruta}")
            return None
        except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as error:
            print(f"No fue posible leer el CSV {ruta}: {error}")
            return None

        if self.validar_df(df):
            return df

        print(f"El archivo CSV no contiene registros: {ruta}")
        return None

    def get_api(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """Consulta una API JSON y transforma la respuesta en un DataFrame."""
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as error:
            print(f"Error al realizar la solicitud a la API: {error}")
            return None
        except ValueError as error:
            print(f"La API no devolvio un JSON valido: {error}")
            return None

        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            print("La API devolvio un formato no soportado.")
            return None

        if self.validar_df(df):
            return df

        print(f"La API {url} no devolvio registros.")
        return None

    def conectar_mongodb(
        self,
        uri: str,
        database: str,
        server_selection_timeout_ms: int = 5000,
    ) -> Optional[Database]:
        """Intenta abrir una conexion a MongoDB y validar el acceso a la base."""
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
            client.admin.command("ping")
            db = client[database]
            print(f"Conexion exitosa a MongoDB: {database}")
            return db
        except PyMongoError as error:
            print(f"Error al conectar a MongoDB: {error}")
            return None

    def validar_df(self, df: Optional[pd.DataFrame] = None) -> bool:
        """Valida que un DataFrame exista y tenga al menos un registro."""
        return df is not None and not df.empty
