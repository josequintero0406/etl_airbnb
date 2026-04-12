"""
Script principal del proceso ETL Airbnb Ciudad de México.
Este es el archivo principal del ETL por lo que debe ejecutarse desde la línea de comandos.

Orquesta las tres fases del proceso ETL:
  1. Extracción → Lee datos de MongoDB
  2. Transformación → Limpia y enriquece los datos
  3. Carga → Persiste en SQLite y exporta a XLSX

Uso:
    python main.py --limite 50000   # Para prueba rápida con 50000 registros por colección
"""

import argparse
import sys
from pathlib import Path

# Añadir src al path
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga
from logger_config import get_logger

logger = get_logger("main")

# Función principal para ejecutar el pipeline ETL completo.
def ejecutar_etl(mongo_uri: str, db_name: str, limite: int = 0) -> None:
    """
    Ejecuta el pipeline ETL completo.

    Args:
        mongo_uri (str): URI de conexión a MongoDB.
        db_name (str): Nombre de la base de datos MongoDB.
        limite (int): Límite de registros por colección (0 = sin límite).
    """
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   ETL AIRBNB CIUDAD DE MEXICO — INICIO   ║")
    logger.info("╚══════════════════════════════════════════╝")
    logger.info("--------------------------------------------")
    logger.info("Equipo de trabajo:__________________________")
    logger.info("José Manuel Quintero Rodríguez")
    logger.info("Jalvi Humberto Villegas Taborda")
    logger.info("Roberto Echeverri Arroyave")
    logger.info("--------------------------------------------")

    # ── FASE 1: EXTRACCIÓN ──
    logger.info("FASE 1: EXTRACCIÓN")
    ext = Extraccion(uri=mongo_uri, db_name=db_name)
    ext.conectar()
    dfs_raw = ext.extraer_todo(limite=limite)
    ext.cerrar_conexion()

    # ── FASE 2: TRANSFORMACIÓN ──
    logger.info("FASE 2: TRANSFORMACIÓN")
    trans = Transformacion(dfs_raw)
    dfs_limpios = trans.transformar_todo()

    # ── FASE 3: CARGA ──
    logger.info("FASE 3: CARGA")
    carga = Carga(
        db_path="data/airbnb_mexico.sqlite",
        xlsx_dir="data/",
    )
    carga.cargar_todo(dfs_limpios)

    logger.info("╔═════════════════════════════╗")
    logger.info("║ ETL COMPLETADO EXITOSAMENTE ║")
    logger.info("╚═════════════════════════════╝")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Airbnb Ciudad de México")
    parser.add_argument("--uri", default="mongodb://localhost:27017", help="URI de MongoDB")
    parser.add_argument("--db", default="airbnb_mexico", help="Nombre de la base de datos")
    parser.add_argument("--limite", type=int, default=0, help="Límite de registros (0=sin límite)")
    args = parser.parse_args()

    ejecutar_etl(mongo_uri=args.uri, db_name=args.db, limite=args.limite)
