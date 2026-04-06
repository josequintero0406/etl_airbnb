"""
Módulo centralizado de configuración de logs para el proceso ETL Airbnb.
Genera un archivo de log por ejecución con el formato logs/log_YYYYMMDD_HHMM.txt
y también muestra los mensajes en consola.

Uso:
    from logger_config import get_logger
    logger = get_logger("extraccion")
    logger.info("Conexión establecida")
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def get_logger(nombre: str) -> logging.Logger:
    """
    Crea y retorna un logger configurado con salida a archivo y consola.

    Args:
        nombre (str): Nombre del módulo que usa el logger (ej: 'extraccion').

    Returns:
        logging.Logger: Logger configurado con handlers de archivo y consola.
    """
    # Directorio de logs relativo a este archivo
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Nombre del archivo de log con timestamp de ejecución
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_filename = logs_dir / f"log_{timestamp}.txt"

    # Crear logger con el nombre del módulo
    logger = logging.getLogger(nombre)
    logger.setLevel(logging.DEBUG)

    # Evitar agregar handlers duplicados si el logger ya existe
    if logger.handlers:
        return logger

    # Formato estándar con fecha, hora, nivel y mensaje
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler para archivo
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logger '{nombre}' iniciado — archivo: {log_filename}")
    return logger
